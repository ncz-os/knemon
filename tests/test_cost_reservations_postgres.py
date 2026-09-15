"""Optional real PostgreSQL race contract; each run uses an isolated schema."""

import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from mnemos.domain.knemon import budget as B
from mnemos.domain.knemon.migrations import ensure_knemon_schema


@pytest.mark.asyncio
async def test_postgres_reservation_race_and_settlement(monkeypatch):
    dsn = os.getenv("KNEMON_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set KNEMON_TEST_POSTGRES_DSN for disposable PostgreSQL")
    asyncpg = pytest.importorskip("asyncpg")
    schema = "knemon_review_" + uuid.uuid4().hex
    admin = await asyncpg.connect(dsn)
    await admin.execute(f'CREATE SCHEMA "{schema}"')
    pool = await asyncpg.create_pool(
        dsn, min_size=1, max_size=8, server_settings={"search_path": schema}
    )

    class Backend:
        @asynccontextmanager
        async def transactional(self):
            async with pool.acquire() as conn:
                async with conn.transaction():
                    yield SimpleNamespace(conn=conn)

    backend = Backend()
    monkeypatch.setattr(
        B,
        "get_settings",
        lambda: SimpleNamespace(
            knemon=SimpleNamespace(
                weekly_budget_cap_usd=1.0, parsed_provider_budget_caps_usd=lambda: {}
            )
        ),
    )
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "CREATE TABLE usage_ledger (id BIGSERIAL PRIMARY KEY, ts TIMESTAMPTZ NOT NULL, session_id TEXT, provider TEXT, caller_subsystem TEXT, est_cost_usd NUMERIC)"
            )
        await ensure_knemon_schema(backend)

        async def reserve():
            try:
                return await B.reserve_cost(
                    backend,
                    provider="claude",
                    model="CaseSensitive",
                    estimated_cost_usd=1.0,
                )
            except B.CostReservationConflict:
                return None

        handles = [
            h for h in await asyncio.gather(*(reserve() for _ in range(12))) if h
        ]
        assert len(handles) == 1
        attempt = handles[0].attempt_id
        await asyncio.gather(
            *(
                B.settle_cost(backend, attempt_id=attempt, actual_cost_usd=0.75)
                for _ in range(8)
            )
        )
        assert await B.effective_spend_usd(backend) == pytest.approx(0.75)
        assert await B.effective_spend_usd_aggregate(backend) == pytest.approx(0.75)
        async with pool.acquire() as conn:
            ledger_id = await conn.fetchval(
                "INSERT INTO usage_ledger(ts,provider,caller_subsystem,est_cost_usd) VALUES($1,'anthropic','pantheon',0.75) RETURNING id",
                datetime.now(timezone.utc),
            )
        await B.settle_cost(
            backend, attempt_id=attempt, actual_cost_usd=0.75, ledger_id=ledger_id
        )
        assert await B.effective_spend_usd(backend) == pytest.approx(0.75)
        with pytest.raises(B.CostReservationConflict):
            await B.reserve_cost(
                backend, provider="anthropic", model="m", estimated_cost_usd=0.5
            )
    finally:
        await pool.close()
        await admin.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await admin.close()
