"""F04 regression tests: durable atomic cost reservation.

Reproduces the F04 review finding (``mnemos/domain/knemon/budget.py``
read-then-decide, no reservation; concurrent requests all admitted
against the same remaining headroom) and pins the new
``reserve_cost`` / ``settle_cost`` / ``release_cost`` /
``canonical_budget_key`` contract.

The tests run against a hand-rolled in-memory backend that speaks the
``backend.transactional()`` protocol the budget module consumes. The
backend uses a per-caller ``_ReserveBackend`` instance whose
``transactional()`` returns a context-managed sqlite3 connection with
the F04 schema pre-applied. This keeps the regression run isolated
from any real persistence layer the host might have configured.
"""

from __future__ import annotations

import asyncio
import sqlite3
import threading
import uuid
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from mnemos.domain.knemon import budget as B
from mnemos.domain.knemon.budget import (
    CostReservationConflict,
    ReservationHandle,
    canonical_budget_key,
    evaluate_usage_budget,
    pending_reservations_usd,
    release_cost,
    reserve_cost,
    settle_cost,
)
from mnemos.domain.knemon.migrations import ensure_knemon_schema

# ---------------------------------------------------------------------------
# In-memory backend with the F04 schema pre-applied
# ---------------------------------------------------------------------------


class _TxConn:
    """Tiny adapter so ``_conn_from_tx`` can pull ``.conn`` off our object."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn


class _ReserveBackend:
    """sqlite3-backed in-memory persistence.

    One instance per test. ``transactional()`` yields a sqlite3
    connection inside a BEGIN/COMMIT block. Because sqlite3 connections
    are not safe to share across threads, the per-instance lock is held
    for the duration of the transaction so concurrent reserves from
    asyncio gather do not interleave on the same connection.

    Note: this is a TEST-ONLY backend. Production uses the multi-process
    durability of postgres / sqlite-on-disk / oracle / db2 / mysql.
    """

    def __init__(self, *, isolation_level: str | None = None) -> None:
        self._isolation_level = isolation_level
        self._conn = sqlite3.connect(":memory:", isolation_level=isolation_level)
        self._lock = threading.Lock()
        self._setup_schema()

    def _setup_schema(self) -> None:
        with self._acquire_cursor() as cur:
            cur.executescript(
                """
                CREATE TABLE knemon_cost_recovery (attempt_id TEXT PRIMARY KEY);
                CREATE TABLE knemon_cost_reservations (
                    attempt_id TEXT PRIMARY KEY,
                    provider TEXT NOT NULL,
                    model TEXT NOT NULL,
                    caller_subsystem TEXT NOT NULL,
                    estimated_cost_usd REAL NOT NULL CHECK (estimated_cost_usd >= 0),
                    actual_cost_usd REAL,
                    reserved_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    settled_at TEXT,
                    released_at TEXT,
                    ledger_id INTEGER,
                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'settled', 'released'))
                );
                CREATE TABLE knemon_cost_reservation_locks (
                    caller_subsystem TEXT PRIMARY KEY,
                    touched_at TEXT NOT NULL
                );
                CREATE TABLE knemon_spend_window_aggregates (
                    caller_subsystem TEXT NOT NULL,
                    provider TEXT NOT NULL DEFAULT '*',
                    window_start_ts TEXT NOT NULL,
                    committed_usd REAL NOT NULL DEFAULT 0 CHECK (committed_usd >= 0),
                    pending_usd REAL NOT NULL DEFAULT 0 CHECK (pending_usd >= 0),
                    last_reconciled_at TEXT,
                    last_updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (caller_subsystem, provider)
                );
                CREATE TABLE usage_ledger (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    session_id TEXT,
                    request_count INTEGER NOT NULL DEFAULT 1,
                    provider TEXT NOT NULL,
                    caller_subsystem TEXT NOT NULL,
                    est_cost_usd REAL NOT NULL DEFAULT 0,
                    tokens_in INTEGER NOT NULL DEFAULT 0,
                    tokens_out INTEGER NOT NULL DEFAULT 0,
                    tokens_reasoning INTEGER NOT NULL DEFAULT 0,
                    latency_ms INTEGER NOT NULL DEFAULT 0,
                    outcome TEXT NOT NULL DEFAULT 'ok',
                    tier TEXT NOT NULL DEFAULT 'standard',
                    plan_window_id TEXT,
                    path_kind TEXT NOT NULL DEFAULT 'api'
                );
                """
            )

    @contextmanager
    def _acquire_cursor(self):
        with self._lock:
            cur = self._conn.cursor()
            try:
                yield cur
            finally:
                cur.close()

    @asynccontextmanager
    async def transactional(self):
        # sqlite3 with autocommit OFF + a manual BEGIN/COMMIT gives us
        # the cross-async-call transactional semantics the budget code
        # expects. We serialise on a per-instance lock so concurrent
        # gather() calls each get a clean view of pending reservations.
        with self._lock:
            cur = self._conn.cursor()
            try:
                cur.execute("BEGIN IMMEDIATE")
                yield _TxConn(self._conn)
                cur.execute("COMMIT")
            except Exception:
                cur.execute("ROLLBACK")
                raise
            finally:
                cur.close()

    async def record_committed_spend(
        self, *, caller_subsystem: str, provider: str, amount_usd: float
    ) -> None:
        """Insert a synthetic committed usage_ledger row (test helper)."""
        with self._lock:
            self._conn.execute(
                "INSERT INTO usage_ledger (ts, session_id, request_count, provider, caller_subsystem, est_cost_usd) "
                "VALUES (?, NULL, 1, ?, ?, ?)",
                (
                    datetime.now(timezone.utc).isoformat(),
                    provider,
                    caller_subsystem,
                    amount_usd,
                ),
            )
            self._conn.commit()

    def reservation_rows(self, *, caller_subsystem: str) -> list[dict]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT attempt_id, provider, model, status, estimated_cost_usd, actual_cost_usd "
                "FROM knemon_cost_reservations WHERE caller_subsystem = ? ORDER BY reserved_at",
                (caller_subsystem,),
            )
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


@pytest.fixture
def backend():
    return _ReserveBackend()


@pytest.fixture(autouse=True)
def patch_settings(monkeypatch):
    """Wire ``get_settings()`` to a duck-typed settings shim."""

    def _settings():
        return SimpleNamespace(
            knemon=SimpleNamespace(
                weekly_budget_cap_usd=0.0,
                parsed_provider_budget_caps_usd=lambda: {},
                session_burn_requests_per_hour=0,
                session_burn_window_seconds=3600,
                subscription_preferred_utilization_pct=80.0,
                subscription_near_cap_pct=90.0,
                low_priority_api_cost_ceiling_usd=0.10,
            )
        )

    monkeypatch.setattr(B, "get_settings", _settings)
    yield


def _reset_db_migration_cache() -> None:
    """Make ``ensure_knemon_schema`` re-evaluate on the next call.

    The migrations hook caches by ``id(backend)``; our test backend is
    a fresh sqlite each fixture so the id differs, but if a previous
    test installed an in-memory backend whose reference is still held
    by the cache, the next test would skip migration. We defensively
    clear it.
    """
    from mnemos.domain.knemon import migrations as _mig

    _mig._MIGRATED_BACKENDS.clear()


# ---------------------------------------------------------------------------
# canonical_budget_key
# ---------------------------------------------------------------------------


def test_canonical_budget_key_collapses_claude_alias():
    """The ``claude`` alias must collapse onto ``anthropic``.

    F05 (PANTHEON) MUST call this helper so a budget cap set under
    either name applies to requests arriving under the other.
    """
    canon_p, canon_m = canonical_budget_key("claude", "claude-sonnet-4-6")
    assert canon_p == "anthropic"
    assert canon_m == "claude-sonnet-4-6"


def test_canonical_budget_key_lowercases_and_strips():
    canon_p, canon_m = canonical_budget_key("  OpenAI ", " GPT-4o ")
    assert canon_p == "openai"
    assert canon_m == "gpt-4o"


def test_canonical_budget_key_handles_empty_inputs():
    assert canonical_budget_key(None, None) == ("", "")
    assert canonical_budget_key("", "") == ("", "")


# ---------------------------------------------------------------------------
# Reviewer scenario: $1 remaining, twenty concurrent $1 reserves
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reserve_cost_atomic_admission_under_concurrent_load(backend):
    """Reviewer's reproduction: with $1 remaining, twenty concurrent $1 reserves
    must NOT all be admitted.

    This is the exact reviewer finding that motivated F04. The budget
    cap is $1; twenty concurrent reserves each estimating $1 must
    yield exactly ONE admitted reservation and nineteen
    ``CostReservationConflict`` rejections.
    """
    _reset_db_migration_cache()
    # Inject the cap via the patched get_settings.
    B.get_settings = lambda: SimpleNamespace(  # type: ignore[assignment]
        knemon=SimpleNamespace(
            weekly_budget_cap_usd=1.0,
            parsed_provider_budget_caps_usd=lambda: {},
        )
    )

    async def attempt(idx: int) -> ReservationHandle | Exception:
        try:
            return await reserve_cost(
                backend,
                provider="openai",
                model="gpt-4o",
                estimated_cost_usd=1.0,
                attempt_id=f"attempt-{idx}",
                caller_subsystem="pantheon",
            )
        except CostReservationConflict as exc:
            return exc

    results = await asyncio.gather(*[attempt(i) for i in range(20)])
    admitted = [r for r in results if isinstance(r, ReservationHandle)]
    rejected = [r for r in results if isinstance(r, CostReservationConflict)]
    assert len(admitted) == 1, (
        f"F04 fix did not hold: {len(admitted)} concurrent $1 reserves "
        f"were admitted against a $1 cap; expected exactly 1"
    )
    assert len(rejected) == 19
    rows = backend.reservation_rows(caller_subsystem="pantheon")
    assert len(rows) == 1
    assert rows[0]["attempt_id"] == admitted[0].attempt_id


@pytest.mark.asyncio
async def test_reserve_cost_pending_amount_counts_against_subsequent_admission(backend):
    """A pending reservation's amount must reduce subsequent-admission headroom.

    F04 contract: pending reservations DO count against the cap until
    they are settled or released. Otherwise a caller can chain a
    follow-up admission against headroom that is still nominally
    promised to the first attempt.
    """
    _reset_db_migration_cache()
    B.get_settings = lambda: SimpleNamespace(  # type: ignore[assignment]
        knemon=SimpleNamespace(
            weekly_budget_cap_usd=5.0,
            parsed_provider_budget_caps_usd=lambda: {},
        )
    )

    first = await reserve_cost(
        backend,
        provider="openai",
        model="gpt-4o",
        estimated_cost_usd=3.0,
        attempt_id="first",
        caller_subsystem="pantheon",
    )
    assert isinstance(first, ReservationHandle)
    # A second $3 reservation should be REJECTED: 3 pending + 3 would = 6 > 5.
    with pytest.raises(CostReservationConflict):
        await reserve_cost(
            backend,
            provider="openai",
            model="gpt-4o",
            estimated_cost_usd=3.0,
            attempt_id="second",
            caller_subsystem="pantheon",
        )
    # A $2 reservation should fit: 3 pending + 2 = 5 == 5 (cap reached).
    second = await reserve_cost(
        backend,
        provider="openai",
        model="gpt-4o",
        estimated_cost_usd=2.0,
        attempt_id="second-fits",
        caller_subsystem="pantheon",
    )
    assert isinstance(second, ReservationHandle)
    # A third $0.01 reservation should now be rejected — headroom gone.
    with pytest.raises(CostReservationConflict):
        await reserve_cost(
            backend,
            provider="openai",
            model="gpt-4o",
            estimated_cost_usd=0.01,
            attempt_id="third-too-much",
            caller_subsystem="pantheon",
        )


@pytest.mark.asyncio
async def test_reserve_cost_release_returns_headroom(backend):
    """Releasing a pending reservation must restore its headroom."""
    _reset_db_migration_cache()
    B.get_settings = lambda: SimpleNamespace(  # type: ignore[assignment]
        knemon=SimpleNamespace(
            weekly_budget_cap_usd=5.0,
            parsed_provider_budget_caps_usd=lambda: {},
        )
    )

    first = await reserve_cost(
        backend,
        provider="openai",
        model="gpt-4o",
        estimated_cost_usd=4.0,
        attempt_id="to-be-released",
        caller_subsystem="pantheon",
    )
    assert isinstance(first, ReservationHandle)
    # $1 follow-up should fit exactly at the cap ($4 + $1 = $5).
    fits = await reserve_cost(
        backend,
        provider="openai",
        model="gpt-4o",
        estimated_cost_usd=1.0,
        attempt_id="fits",
        caller_subsystem="pantheon",
    )
    assert isinstance(fits, ReservationHandle)
    # A $0.01 follow-up should be rejected — headroom gone.
    with pytest.raises(CostReservationConflict):
        await reserve_cost(
            backend,
            provider="openai",
            model="gpt-4o",
            estimated_cost_usd=0.01,
            attempt_id="blocks",
            caller_subsystem="pantheon",
        )
    # Release BOTH pending attempts; the follow-up should now be admitted.
    await release_cost(backend, attempt_id="to-be-released")
    await release_cost(backend, attempt_id="fits")
    second = await reserve_cost(
        backend,
        provider="openai",
        model="gpt-4o",
        estimated_cost_usd=1.0,
        attempt_id="now-fits",
        caller_subsystem="pantheon",
    )
    assert isinstance(second, ReservationHandle)
    rows = backend.reservation_rows(caller_subsystem="pantheon")
    by_id = {r["attempt_id"]: r for r in rows}
    assert by_id["to-be-released"]["status"] == "released"
    assert by_id["now-fits"]["status"] == "pending"


@pytest.mark.asyncio
async def test_reserve_cost_settle_then_admission_reflects_actual(backend):
    """After settle, the resolved cost (actual) replaces the reserved estimate.

    If the provider actually cost less than the estimate, the freed
    headroom must become available to subsequent reservations.
    """
    _reset_db_migration_cache()
    B.get_settings = lambda: SimpleNamespace(  # type: ignore[assignment]
        knemon=SimpleNamespace(
            weekly_budget_cap_usd=5.0,
            parsed_provider_budget_caps_usd=lambda: {},
        )
    )

    first = await reserve_cost(
        backend,
        provider="openai",
        model="gpt-4o",
        estimated_cost_usd=4.0,
        attempt_id="first-call",
        caller_subsystem="pantheon",
    )
    assert isinstance(first, ReservationHandle)
    # Real cost came in at $1.50 — settle and the difference is freed.
    await settle_cost(backend, attempt_id="first-call", actual_cost_usd=1.50)
    # Actual spend remains counted; only $3.50 of the $5 cap is available.
    with pytest.raises(B.CostReservationConflict):
        await reserve_cost(
            backend,
            provider="openai",
            model="gpt-4o",
            estimated_cost_usd=4.0,
            attempt_id="over-cap",
        )
    second = await reserve_cost(
        backend,
        provider="openai",
        model="gpt-4o",
        estimated_cost_usd=3.5,
        attempt_id="after-settle",
        caller_subsystem="pantheon",
    )
    assert isinstance(second, ReservationHandle)
    rows = backend.reservation_rows(caller_subsystem="pantheon")
    by_id = {r["attempt_id"]: r for r in rows}
    assert by_id["first-call"]["status"] == "settled"
    assert by_id["first-call"]["actual_cost_usd"] == pytest.approx(1.50)
    assert by_id["after-settle"]["status"] == "pending"


# ---------------------------------------------------------------------------
# Idempotency contract
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reserve_cost_settle_double_settle_idempotent_no_double_count(backend):
    """Settling the same attempt_id twice with the SAME actual must not double-count."""
    _reset_db_migration_cache()
    B.get_settings = lambda: SimpleNamespace(  # type: ignore[assignment]
        knemon=SimpleNamespace(
            weekly_budget_cap_usd=10.0,
            parsed_provider_budget_caps_usd=lambda: {},
        )
    )

    await reserve_cost(
        backend,
        provider="openai",
        model="gpt-4o",
        estimated_cost_usd=3.0,
        attempt_id="settle-once",
        caller_subsystem="pantheon",
    )
    await settle_cost(backend, attempt_id="settle-once", actual_cost_usd=2.50)
    # Idempotent re-settle with same actual cost must be a no-op.
    await settle_cost(backend, attempt_id="settle-once", actual_cost_usd=2.50)
    rows = backend.reservation_rows(caller_subsystem="pantheon")
    assert rows[0]["status"] == "settled"
    assert rows[0]["actual_cost_usd"] == pytest.approx(2.50)


@pytest.mark.asyncio
async def test_reserve_cost_settle_with_different_actual_is_a_conflict(backend):
    """Settling the same attempt_id twice with DIFFERENT actuals raises."""
    _reset_db_migration_cache()
    B.get_settings = lambda: SimpleNamespace(  # type: ignore[assignment]
        knemon=SimpleNamespace(
            weekly_budget_cap_usd=10.0,
            parsed_provider_budget_caps_usd=lambda: {},
        )
    )
    await reserve_cost(
        backend,
        provider="openai",
        model="gpt-4o",
        estimated_cost_usd=3.0,
        attempt_id="settle-conflict",
        caller_subsystem="pantheon",
    )
    await settle_cost(backend, attempt_id="settle-conflict", actual_cost_usd=2.50)
    with pytest.raises(CostReservationConflict):
        await settle_cost(backend, attempt_id="settle-conflict", actual_cost_usd=3.50)


@pytest.mark.asyncio
async def test_reserve_cost_release_double_release_is_a_noop(backend):
    """Defensive double-release must not raise."""
    _reset_db_migration_cache()
    B.get_settings = lambda: SimpleNamespace(  # type: ignore[assignment]
        knemon=SimpleNamespace(
            weekly_budget_cap_usd=10.0,
            parsed_provider_budget_caps_usd=lambda: {},
        )
    )
    await reserve_cost(
        backend,
        provider="openai",
        model="gpt-4o",
        estimated_cost_usd=2.0,
        attempt_id="release-twice",
        caller_subsystem="pantheon",
    )
    await release_cost(backend, attempt_id="release-twice")
    # Second call is a no-op, not an error.
    await release_cost(backend, attempt_id="release-twice")
    rows = backend.reservation_rows(caller_subsystem="pantheon")
    assert rows[0]["status"] == "released"


@pytest.mark.asyncio
async def test_reserve_cost_release_after_settle_is_a_noop(backend):
    """Cleanup code that releases a settled attempt_id must not raise."""
    _reset_db_migration_cache()
    B.get_settings = lambda: SimpleNamespace(  # type: ignore[assignment]
        knemon=SimpleNamespace(
            weekly_budget_cap_usd=10.0,
            parsed_provider_budget_caps_usd=lambda: {},
        )
    )
    await reserve_cost(
        backend,
        provider="openai",
        model="gpt-4o",
        estimated_cost_usd=2.0,
        attempt_id="settled-then-released",
        caller_subsystem="pantheon",
    )
    await settle_cost(backend, attempt_id="settled-then-released", actual_cost_usd=1.75)
    # Defensive release on a settled attempt — must not raise.
    await release_cost(backend, attempt_id="settled-then-released")
    rows = backend.reservation_rows(caller_subsystem="pantheon")
    assert rows[0]["status"] == "settled"


@pytest.mark.asyncio
async def test_reserve_cost_release_unknown_attempt_is_a_noop(backend):
    """Defensive release on an unknown attempt_id is a no-op."""
    await release_cost(backend, attempt_id="never-existed")  # no raise


@pytest.mark.asyncio
async def test_reserve_cost_idempotent_for_same_attempt_id(backend):
    """Re-reserving the same attempt_id returns the existing handle."""
    _reset_db_migration_cache()
    B.get_settings = lambda: SimpleNamespace(  # type: ignore[assignment]
        knemon=SimpleNamespace(
            weekly_budget_cap_usd=10.0,
            parsed_provider_budget_caps_usd=lambda: {},
        )
    )
    first = await reserve_cost(
        backend,
        provider="openai",
        model="gpt-4o",
        estimated_cost_usd=2.0,
        attempt_id="retry-aware",
        caller_subsystem="pantheon",
    )
    second = await reserve_cost(
        backend,
        provider="openai",
        model="gpt-4o",
        estimated_cost_usd=2.0,
        attempt_id="retry-aware",
        caller_subsystem="pantheon",
    )
    assert first.attempt_id == second.attempt_id
    # Only one row exists — no double-book.
    rows = backend.reservation_rows(caller_subsystem="pantheon")
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_reserve_cost_mints_attempt_id_when_omitted(backend):
    """When ``attempt_id`` is not supplied, KNEMON mints a UUID for the caller."""
    _reset_db_migration_cache()
    B.get_settings = lambda: SimpleNamespace(  # type: ignore[assignment]
        knemon=SimpleNamespace(
            weekly_budget_cap_usd=10.0,
            parsed_provider_budget_caps_usd=lambda: {},
        )
    )
    handle = await reserve_cost(
        backend,
        provider="openai",
        model="gpt-4o",
        estimated_cost_usd=1.0,
        caller_subsystem="pantheon",
    )
    # The attempt_id is a valid UUID-shaped string.
    assert handle.attempt_id
    parsed_uuid = uuid.UUID(handle.attempt_id)
    assert str(parsed_uuid) == handle.attempt_id


# ---------------------------------------------------------------------------
# Provider-scoped caps
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reserve_cost_provider_cap_blocks_only_that_provider(backend):
    """Provider-scoped cap must NOT bleed across providers."""
    _reset_db_migration_cache()
    B.get_settings = lambda: SimpleNamespace(  # type: ignore[assignment]
        knemon=SimpleNamespace(
            weekly_budget_cap_usd=100.0,
            parsed_provider_budget_caps_usd=lambda: {"openai": 2.0},
        )
    )
    # A $2 reservation on openai exactly fills its provider cap.
    openai_handle = await reserve_cost(
        backend,
        provider="openai",
        model="gpt-4o",
        estimated_cost_usd=2.0,
        attempt_id="openai-fill",
        caller_subsystem="pantheon",
    )
    assert isinstance(openai_handle, ReservationHandle)
    # A second $0.01 reservation on openai must be rejected.
    with pytest.raises(CostReservationConflict):
        await reserve_cost(
            backend,
            provider="openai",
            model="gpt-4o",
            estimated_cost_usd=0.01,
            attempt_id="openai-overflow",
            caller_subsystem="pantheon",
        )
    # But anthropic (no provider cap) has plenty of room under the
    # global $100 cap.
    anthropic_handle = await reserve_cost(
        backend,
        provider="anthropic",
        model="claude-sonnet-4-6",
        estimated_cost_usd=2.0,
        attempt_id="anthropic-ok",
        caller_subsystem="pantheon",
    )
    assert isinstance(anthropic_handle, ReservationHandle)


@pytest.mark.asyncio
async def test_reserve_cost_canonicalises_provider_alias(backend):
    """Reserve must canonicalise the provider before computing the cap.

    F05 (PANTHEON) calls ``canonical_budget_key`` and passes the
    canonical key. KNEMON's reserve applies the same alias table so a
    cap set under ``anthropic`` is enforced for requests arriving
    under ``claude``.
    """
    _reset_db_migration_cache()
    B.get_settings = lambda: SimpleNamespace(  # type: ignore[assignment]
        knemon=SimpleNamespace(
            weekly_budget_cap_usd=100.0,
            parsed_provider_budget_caps_usd=lambda: {"anthropic": 1.0},
        )
    )
    # ``claude`` aliases to ``anthropic`` — the cap must apply.
    handle = await reserve_cost(
        backend,
        provider="claude",
        model="claude-sonnet-4-6",
        estimated_cost_usd=1.0,
        attempt_id="alias-reserve",
        caller_subsystem="pantheon",
    )
    assert handle.provider == "anthropic"
    # A follow-up on the aliased provider must hit the canonical cap.
    with pytest.raises(CostReservationConflict):
        await reserve_cost(
            backend,
            provider="claude",
            model="claude-haiku",
            estimated_cost_usd=0.01,
            attempt_id="alias-overflow",
            caller_subsystem="pantheon",
        )


# ---------------------------------------------------------------------------
# Pre-flight admission (evaluate_usage_budget) sees pending reservations
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_evaluate_usage_budget_counts_pending_reservations(backend):
    """``evaluate_usage_budget`` must include pending reservations in headroom math."""
    _reset_db_migration_cache()
    B.get_settings = lambda: SimpleNamespace(  # type: ignore[assignment]
        knemon=SimpleNamespace(
            weekly_budget_cap_usd=10.0,
            parsed_provider_budget_caps_usd=lambda: {},
        )
    )
    # First reservation books $7.
    await reserve_cost(
        backend,
        provider="openai",
        model="gpt-4o",
        estimated_cost_usd=7.0,
        attempt_id="eats-7",
        caller_subsystem="pantheon",
    )
    # evaluate_usage_budget for a $4 estimate must now DENY (7 pending + 4 = 11 > 10).
    decision = await evaluate_usage_budget(
        backend,
        estimated_cost_usd=4.0,
        caller_subsystem="pantheon",
        provider="openai",
    )
    assert not decision.allowed
    assert "would exceed" in decision.reason


@pytest.mark.asyncio
async def test_reserve_budget_pending_reservations_excludes_settled_and_released(
    backend,
):
    """Settled / released reservations must NOT count against the cap."""
    _reset_db_migration_cache()
    B.get_settings = lambda: SimpleNamespace(  # type: ignore[assignment]
        knemon=SimpleNamespace(
            weekly_budget_cap_usd=10.0,
            parsed_provider_budget_caps_usd=lambda: {},
        )
    )
    await reserve_cost(
        backend,
        provider="openai",
        model="gpt-4o",
        estimated_cost_usd=4.0,
        attempt_id="to-settle",
        caller_subsystem="pantheon",
    )
    await reserve_cost(
        backend,
        provider="openai",
        model="gpt-4o",
        estimated_cost_usd=3.0,
        attempt_id="to-release",
        caller_subsystem="pantheon",
    )
    pending_mid = await pending_reservations_usd(
        backend, caller_subsystem="pantheon", provider="openai"
    )
    assert pending_mid == pytest.approx(7.0)
    await settle_cost(backend, attempt_id="to-settle", actual_cost_usd=1.50)
    await release_cost(backend, attempt_id="to-release")
    pending_after = await pending_reservations_usd(
        backend, caller_subsystem="pantheon", provider="openai"
    )
    assert pending_after == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# F04 review: ledger write failures must raise, not swallow
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reserve_cost_raises_on_persistence_failure():
    """F04 review #1: settlement / reservation failures must raise loudly.

    A broken backend (transactional raises) must propagate, not be
    swallowed (the pre-F04 bug). We use a backend whose ``transactional``
    is a coroutine that always raises.
    """

    class _BrokenBackend:
        @asynccontextmanager
        async def transactional(self):
            raise RuntimeError("backend offline")
            yield  # pragma: no cover — for type-checker only

    backend = _BrokenBackend()
    with pytest.raises(Exception):
        await reserve_cost(
            backend,
            provider="openai",
            model="gpt-4o",
            estimated_cost_usd=1.0,
            attempt_id="broken",
            caller_subsystem="pantheon",
        )
    with pytest.raises(Exception):
        await settle_cost(backend, attempt_id="broken", actual_cost_usd=1.0)
    with pytest.raises(Exception):
        await release_cost(backend, attempt_id="broken")


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reserve_budget_cleanup_expires_stale_pending(backend):
    """Stale pending reservations are auto-released by the cleanup pass."""
    _reset_db_migration_cache()
    B.get_settings = lambda: SimpleNamespace(  # type: ignore[assignment]
        knemon=SimpleNamespace(
            weekly_budget_cap_usd=10.0,
            parsed_provider_budget_caps_usd=lambda: {},
        )
    )
    await reserve_cost(
        backend,
        provider="openai",
        model="gpt-4o",
        estimated_cost_usd=2.0,
        attempt_id="stale-pending",
        caller_subsystem="pantheon",
        ttl_seconds=1,
    )
    # Move the clock forward past the TTL.
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    from mnemos.domain.knemon.budget import cleanup_cost_reservations

    await cleanup_cost_reservations(backend, now=future)
    rows = backend.reservation_rows(caller_subsystem="pantheon")
    assert rows[0]["status"] == "settled"
    assert rows[0]["actual_cost_usd"] == 2.0
    assert await B.effective_spend_usd(backend, now=future) == 2.0


# ---------------------------------------------------------------------------
# Migration coverage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reserve_cost_migration_creates_reservations_table_on_sqlite(tmp_path):
    """The 0054 migration must create the reservation tables on a fresh DB."""
    import sqlite3 as _sql

    db_path = tmp_path / "fresh.sqlite"
    conn = _sql.connect(str(db_path))
    try:
        conn.executescript(
            """
            CREATE TABLE usage_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                session_id TEXT,
                provider TEXT NOT NULL,
                caller_subsystem TEXT NOT NULL,
                est_cost_usd REAL NOT NULL DEFAULT 0,
                request_count INTEGER NOT NULL DEFAULT 1,
                path_kind TEXT NOT NULL DEFAULT 'api'
            );
            """
        )
        conn.commit()
    finally:
        conn.close()

    class _SqliteFileBackend:
        def __init__(self, path: str) -> None:
            self._path = path

        @asynccontextmanager
        async def transactional(self):
            conn = _sql.connect(self._path)
            try:
                conn.execute("BEGIN IMMEDIATE")
                yield _TxConn(conn)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    backend = _SqliteFileBackend(str(db_path))
    await ensure_knemon_schema(backend)
    # Re-open to verify the table exists.
    check_conn = _sql.connect(str(db_path))
    try:
        rows = check_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='knemon_cost_reservations'"
        ).fetchall()
        assert rows, "0054 migration did not create knemon_cost_reservations"
    finally:
        check_conn.close()
