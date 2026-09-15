"""Real sibling gateway -> KNEMON -> core SQLite accounting contracts."""

import asyncio
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from mnemos.core import lifecycle
from mnemos.core.config import get_settings
from mnemos.domain.knemon import budget as B
from mnemos.domain.knemon.attempts import Attempt
from mnemos.persistence.sqlite import SqliteBackend


@pytest_asyncio.fixture
async def ledger(tmp_path, monkeypatch):
    settings = get_settings()
    monkeypatch.setattr(settings.knemon, "weekly_budget_cap_usd", 1.0)
    monkeypatch.setattr(
        settings.knemon, "provider_budget_caps_usd", "claude=0.2,anthropic=0.1"
    )
    backend = SqliteBackend(tmp_path / "attempts.sqlite", settings)
    monkeypatch.setattr(lifecycle, "_persistence_backend", backend)
    try:
        yield backend
    finally:
        await backend.close()


@pytest.mark.asyncio
async def test_attempt_retains_spend_after_ledger_failure(ledger, monkeypatch):
    async def fail(*args):
        raise RuntimeError("ledger temporarily unavailable")

    monkeypatch.setattr(ledger, "record_usage_ledger", fail)
    attempt = await Attempt("claude", "ModelCase", "graeae", 0.06).begin()
    await attempt.finish("err")
    assert await B.effective_spend_usd(
        ledger, caller_subsystem="graeae"
    ) == pytest.approx(0.06)
    with pytest.raises(B.CostReservationConflict):
        await Attempt("anthropic", "ModelCase", "graeae", 0.06).begin()


@pytest.mark.asyncio
async def test_pantheon_actual_attempts_charge_each_retry_and_provider(
    ledger, monkeypatch
):
    gateway = pytest.importorskip("mnemos.domain.pantheon.gateway")
    from mnemos.domain.pantheon.router import RouteDecision

    calls = []

    async def handler(request):
        calls.append(request.url.host)
        if request.url.host == "primary.test":
            return httpx.Response(503, json={"error": "failed"})
        return httpx.Response(
            200,
            json={
                "choices": [],
                "usage": {"prompt_tokens": 10, "completion_tokens": 20},
            },
        )

    primary = RouteDecision(
        alias="auto",
        provider="openai",
        model_id="primary",
        route_type="model",
        reason="test",
        model={"input_cost_per_mtok": 1, "output_cost_per_mtok": 1},
    )
    fallback = RouteDecision(
        alias="auto",
        provider="claude",
        model_id="FallbackCase",
        route_type="model",
        reason="test",
        model={"input_cost_per_mtok": 1, "output_cost_per_mtok": 1},
    )

    class Runtime:
        async def route(self, chain, operation, **kwargs):
            for decision in chain:
                try:
                    value = await operation(decision)
                    return SimpleNamespace(result=value, deployment=decision)
                except gateway.PantheonGatewayError as exc:
                    assert exc.status_code == 503
            raise AssertionError("no result")

    async def chain(_):
        return [primary, fallback]

    monkeypatch.setattr(gateway, "get_runtime", lambda: Runtime())
    monkeypatch.setattr(gateway, "_runtime_chain", chain)
    monkeypatch.setattr(
        gateway,
        "_provider_config",
        lambda d: {
            "api": "openai",
            "url": f"https://{'primary' if d is primary else 'fallback'}.test/chat",
            "key_name": "fake",
        },
    )
    monkeypatch.setattr(gateway, "_auth_headers", lambda *a: {})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        monkeypatch.setattr(gateway, "get_http_client", lambda: client)
        response = await gateway.forward_chat_completion(
            primary, {"messages": [], "max_tokens": 100}
        )
    assert calls == ["primary.test", "fallback.test"]
    assert response.provider == "claude"
    async with ledger.transactional() as tx:
        rows = await B._tx_rows(
            tx.conn,
            "SELECT provider, model, tokens_in, tokens_out FROM usage_ledger ORDER BY id",
        )
    assert [r["provider"] for r in rows] == ["openai", "anthropic"]
    assert rows[1]["model"] == "FallbackCase"
    assert (rows[1]["tokens_in"], rows[1]["tokens_out"]) == (10, 20)
    assert (
        await B.effective_spend_usd(ledger) > 0.00003
    )  # failed primary conservatively retained


@pytest.mark.asyncio
async def test_graeae_each_worker_call_checks_real_provider_cap_and_native_usage(
    ledger,
):
    module = pytest.importorskip("mnemos.domain.graeae.engine")
    from mnemos.domain.graeae.provider_worker import ProviderQueryResponse

    engine = module.GraeaeEngine.__new__(module.GraeaeEngine)
    engine.providers = {
        "claude": {
            "model": "NativeCase",
            "input_cost_per_mtok": 100,
            "output_cost_per_mtok": 100,
        }
    }
    calls = []

    async def worker(request):
        calls.append(request.provider)
        return ProviderQueryResponse(
            response_text="answer",
            latency_ms=1,
            status="success",
            cost=None,
            model_id_used="NativeCase",
            raw_provider_payload={
                "status": "success",
                "usage": {"input_tokens": 100, "output_tokens": 200},
            },
        )

    engine.provider_worker = worker
    await engine._call_provider_worker(
        "claude", "x", "chat", 10, generation_params={"max_tokens": 100}
    )
    async with ledger.transactional() as tx:
        rows = await B._tx_rows(tx.conn, "SELECT * FROM usage_ledger")
    assert rows[0]["provider"] == "anthropic"
    assert rows[0]["model"] == "NativeCase"
    assert (rows[0]["tokens_in"], rows[0]["tokens_out"]) == (100, 200)
    assert await B.effective_spend_usd(
        ledger, caller_subsystem="graeae"
    ) == pytest.approx(0.03)
    with pytest.raises(B.CostReservationConflict):
        await engine._call_provider_worker(
            "claude", "x", "chat", 10, generation_params={"max_tokens": 1000}
        )
    assert calls == ["claude"]


@pytest.mark.asyncio
async def test_stream_usage_and_cancellation_finalize_once(ledger):
    attempt = await Attempt(
        "claude", "StreamingCase", "pantheon", 0.08, input_price=100, output_price=100
    ).begin()
    attempt.feed(b'data: {"usage": {"input_tokens": 100, "output_')
    attempt.feed(b'tokens": 200}}\n\n')
    await attempt.finish("cancelled")
    await attempt.finish("cancelled")
    assert await B.effective_spend_usd(ledger) == pytest.approx(0.03)
    async with ledger.transactional() as tx:
        rows = await B._tx_rows(tx.conn, "SELECT COUNT(*) AS n FROM usage_ledger")
    assert rows[0]["n"] == 1


@pytest.mark.asyncio
async def test_concurrent_finalizers_append_one_ledger_row(ledger):
    attempt = await Attempt("claude", "m", "pantheon", 0.05).begin()
    await asyncio.gather(*(attempt.finish() for _ in range(10)))
    async with ledger.transactional() as tx:
        rows = await B._tx_rows(tx.conn, "SELECT COUNT(*) AS n FROM usage_ledger")
    assert rows[0]["n"] == 1
    assert await B.effective_spend_usd(ledger) == pytest.approx(0.05)


@pytest.mark.asyncio
async def test_recovery_estimate_corrected_by_late_authoritative_usage(ledger):
    from datetime import datetime, timedelta, timezone

    handle = await B.reserve_cost(
        ledger, provider="claude", model="m", estimated_cost_usd=0.08, ttl_seconds=1
    )
    await B.cleanup_cost_reservations(
        ledger, now=datetime.now(timezone.utc) + timedelta(seconds=2)
    )
    assert await B.effective_spend_usd(ledger) == pytest.approx(0.08)
    await B.settle_cost(ledger, attempt_id=handle.attempt_id, actual_cost_usd=0.03)
    assert await B.effective_spend_usd(ledger) == pytest.approx(0.03)
    with pytest.raises(B.CostReservationConflict):
        await B.settle_cost(ledger, attempt_id=handle.attempt_id, actual_cost_usd=0.04)


@pytest.mark.asyncio
async def test_idempotency_payload_cannot_change(ledger):
    await B.reserve_cost(
        ledger,
        provider="claude",
        model="m",
        estimated_cost_usd=0.01,
        attempt_id="fixed",
    )
    with pytest.raises(B.CostReservationConflict):
        await B.reserve_cost(
            ledger,
            provider="claude",
            model="other",
            estimated_cost_usd=0.01,
            attempt_id="fixed",
        )
    with pytest.raises(B.CostReservationConflict):
        await B.reserve_cost(
            ledger,
            provider="openai",
            model="m",
            estimated_cost_usd=0.01,
            attempt_id="fixed",
        )


@pytest.mark.asyncio
async def test_pantheon_stream_fallback_identity_survives_generator_cleanup(
    ledger, monkeypatch
):
    gateway = pytest.importorskip("mnemos.domain.pantheon.gateway")
    from mnemos.domain.pantheon.router import RouteDecision

    primary = RouteDecision(
        alias="auto",
        provider="openai",
        model_id="primary",
        route_type="model",
        reason="test",
        model={"input_cost_per_mtok": 1, "output_cost_per_mtok": 1},
    )
    fallback = RouteDecision(
        alias="auto",
        provider="claude",
        model_id="FallbackStream",
        route_type="model",
        reason="test",
        model={"input_cost_per_mtok": 1, "output_cost_per_mtok": 1},
    )

    async def handler(request):
        if request.url.host == "primary.test":
            return httpx.Response(503, text="failure")
        return httpx.Response(
            200,
            content=b'data: {"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":20}}\n\ndata: [DONE]\n\n',
        )

    class Runtime:
        async def route(self, chain, operation, **kwargs):
            for decision in chain:
                try:
                    return SimpleNamespace(
                        result=await operation(decision), deployment=decision
                    )
                except gateway.PantheonGatewayError as exc:
                    assert exc.status_code == 503
            raise AssertionError("no result")

    async def chain(_):
        return [primary, fallback]

    monkeypatch.setattr(gateway, "get_runtime", lambda: Runtime())
    monkeypatch.setattr(gateway, "_runtime_chain", chain)
    monkeypatch.setattr(
        gateway,
        "_provider_config",
        lambda d: {
            "api": "openai",
            "url": f"https://{'primary' if d is primary else 'fallback'}.test/chat",
            "key_name": "fake",
        },
    )
    monkeypatch.setattr(gateway, "_auth_headers", lambda *a: {})
    state = {}
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        monkeypatch.setattr(gateway, "get_http_client", lambda: client)
        chunks = [
            chunk
            async for chunk in gateway.stream_chat_completion(
                primary,
                {"messages": [], "max_tokens": 100, "_pantheon_stream_state": state},
            )
        ]
    assert chunks
    assert state["actual"] is fallback
    assert gateway.active_stream_actual_decision() is None
    async with ledger.transactional() as tx:
        rows = await B._tx_rows(
            tx.conn,
            "SELECT provider,tokens_in,tokens_out FROM usage_ledger ORDER BY id",
        )
    assert [row["provider"] for row in rows] == ["openai", "anthropic"]
    assert (rows[1]["tokens_in"], rows[1]["tokens_out"]) == (10, 20)


@pytest.mark.asyncio
@pytest.mark.parametrize("close_fails", [False, True])
async def test_failed_stream_error_body_still_closes_and_settles(
    ledger, monkeypatch, close_fails
):
    gateway = pytest.importorskip("mnemos.domain.pantheon.gateway")
    from mnemos.domain.pantheon.router import RouteDecision

    decision = RouteDecision(
        alias="x",
        provider="openai",
        model_id="m",
        route_type="model",
        reason="test",
        model={"input_cost_per_mtok": 1, "output_cost_per_mtok": 1},
    )
    closed = []

    class Response:
        status_code = 503

        async def aread(self):
            raise RuntimeError("broken error body")

    class Manager:
        async def __aenter__(self):
            return Response()

        async def __aexit__(self, *args):
            closed.append(True)
            if close_fails:
                raise RuntimeError("broken close")

    monkeypatch.setattr(
        gateway,
        "get_http_client",
        lambda: SimpleNamespace(stream=lambda *a, **kw: Manager()),
    )
    monkeypatch.setattr(
        gateway,
        "_provider_config",
        lambda d: {
            "api": "openai",
            "url": "https://fake.test/chat",
            "key_name": "fake",
        },
    )
    monkeypatch.setattr(gateway, "_auth_headers", lambda *a: {})
    with pytest.raises(RuntimeError):
        await gateway._open_chat_stream_once(
            decision, {"messages": [], "max_tokens": 100}
        )
    assert closed == [True]
    async with ledger.transactional() as tx:
        rows = await B._tx_rows(tx.conn, "SELECT status FROM knemon_cost_reservations")
    assert [row["status"] for row in rows] == ["settled"]
    assert await B.effective_spend_usd(ledger) > 0
