"""Regression test for the read-path spend aggregate (F04 review #3 / perf #1).

This pins the contract the F04 review demanded: KNEMON's budget
admission check (``effective_spend_usd``) currently re-sums the full
rolling-7-day ``usage_ledger`` plus outstanding pending reservations
on every call. The read-path optimization replaces that O(N) re-scan
with a small maintained counter (``knemon_spend_window_aggregates``)
that ``reserve_cost`` / ``settle_cost`` / ``release_cost`` keep
incrementally in sync, and that ``reconcile_spend_aggregates`` can
rebuild from scratch when drift is detected.

The test asserts the central correctness-of-aggregation invariant:

* After a series of ``reserve_cost`` / ``settle_cost`` /
  ``release_cost`` calls, the maintained aggregate must equal a fresh
  full-scan SUM of the underlying ``usage_ledger`` plus the
  outstanding pending reservations. Drift detection
  (``reconcile_spend_aggregates``) must report zero drift on this
  scale.
* When the rolling window rolls forward, contributions older than the
  new window lower-bound must drop out of both the aggregate AND the
  fresh ledger SUM, and the two must still agree.

The fixture backend is the same in-memory sqlite3 backend that
``test_knemon_budget_reservation_atomic.py`` uses; we add the
``knemon_spend_window_aggregates`` table here so the aggregate code
path can run without ``ensure_knemon_schema``.
"""

from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest
from mnemos.domain.knemon import budget as B
from mnemos.domain.knemon.budget import (
    AggregateDriftReport,
    effective_spend_usd_aggregate,
    reconcile_spend_aggregates,
    release_cost,
    reserve_cost,
    settle_cost,
)


async def _fresh_spend_usd(
    backend: "_AggregateBackend",
    *,
    caller_subsystem: str,
    provider: str | None,
    now: datetime,
) -> float:
    """Same SUM the aggregate's reconciliation runs (ledger + settled reservations + pending).

    The standard ``effective_spend_usd`` reads only ``usage_ledger``,
    which is the pre-F04 path. F04 callers record spend on the
    ``knemon_cost_reservations`` table via ``settle_cost`` instead of
    writing to ``usage_ledger``. The aggregate covers BOTH sources so
    its "fresh SUM" comparison must too — this helper is the
    comparison oracle the regression test uses.
    """
    window_seconds = int(
        os.environ.get("MNEMOS_KNEMON_SPEND_AGGREGATE_WINDOW_SECONDS", "60")
    )
    since = now - timedelta(seconds=window_seconds)
    with backend._lock:
        cur = backend._conn.cursor()
        try:
            # Pass ``datetime`` objects (NOT ``since.isoformat()``).
            # sqlite3's default datetime adapter stores datetimes with a
            # *space* separator (``"2026-09-14 19:52:47..."``) while
            # ``datetime.isoformat()`` uses ``"T"`` — passing the T form
            # silently compares lexicographically (' ' < 'T') and
            # returns zero rows even when timestamps are in range. The
            # aggregate's reconciliation uses ``datetime`` objects too,
            # so the oracle must do the same to compare apples-to-apples.
            provider_clause = ""
            params: list[Any] = [caller_subsystem, since]
            if provider:
                provider_clause = "AND LOWER(provider) = ?"
                params.append(provider.strip().lower())
            cur.execute(
                "SELECT COALESCE(SUM(est_cost_usd), 0) FROM usage_ledger "
                "WHERE caller_subsystem = ? AND julianday(ts) >= julianday(?) "
                + provider_clause,
                tuple(params),
            )
            ledger_committed = float(cur.fetchone()[0] or 0.0)
            cur.execute(
                "SELECT COALESCE(SUM(actual_cost_usd), 0) FROM knemon_cost_reservations "
                "WHERE status = 'settled' AND caller_subsystem = ? AND julianday(settled_at) >= julianday(?) "
                + provider_clause,
                tuple(params),
            )
            settled_committed = float(cur.fetchone()[0] or 0.0)
            cur.execute(
                "SELECT COALESCE(SUM(estimated_cost_usd), 0) FROM knemon_cost_reservations "
                "WHERE status = 'pending' AND caller_subsystem = ? AND expires_at > ? "
                + provider_clause,
                tuple(params),
            )
            pending = float(cur.fetchone()[0] or 0.0)
        finally:
            cur.close()
    return ledger_committed + settled_committed + pending


# ---------------------------------------------------------------------------
# Backend with the aggregate schema pre-applied
# ---------------------------------------------------------------------------


class _TxConn:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn


class _AggregateBackend:
    """sqlite3 in-memory backend that mirrors the F04 + aggregate schema.

    Production uses ``ensure_knemon_schema`` to apply migration 0055
    alongside 0052/0053/0054. The atomic-reservation tests apply the
    schema directly because their backend is wired to bypass
    ``mnemos.persistence.*``. We follow the same pattern here so the
    aggregate path can be exercised in isolation.
    """

    def __init__(self) -> None:
        self._conn = sqlite3.connect(":memory:")
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
            # _setup_schema runs before any caller has a chance to
            # start a transaction; safe to commit explicitly so the
            # connection is left in a clean (no-implicit-tx) state
            # for the next operation.
            self._conn.commit()

    @contextmanager
    def _acquire_cursor(self):
        with self._lock:
            cur = self._conn.cursor()
            try:
                yield cur
            finally:
                cur.close()

    def _rollback_leaked_txn(self) -> None:
        """Roll back any implicit transaction a prior writer left open.

        Python's ``sqlite3`` module defaults to autocommit-OFF; a
        bare ``cur.execute("UPDATE …")`` followed by ``cur.close()``
        silently leaves an implicit transaction hanging on the
        connection. The next ``BEGIN IMMEDIATE`` then fails with
        ``cannot start a transaction within a transaction``. The
        ``_AggregateBackend`` helpers that simulate window rollover
        (``update_ledger_ts_for_test``) intentionally run a raw
        UPDATE outside the ``transactional()`` context manager so
        they can mutate ``usage_ledger.ts`` without going through
        F04's reserve/settle flow. We defensively roll back here so
        ``transactional()`` always starts from a clean state.
        """
        try:
            self._conn.rollback()
        except sqlite3.OperationalError:
            # No transaction is open — that's the happy case.
            pass

    @asynccontextmanager
    async def transactional(self):
        with self._lock:
            self._rollback_leaked_txn()
            cur = self._conn.cursor()
            try:
                cur.execute("BEGIN IMMEDIATE")
                yield _TxConn(self._conn)
                cur.execute("COMMIT")
            except Exception:
                try:
                    cur.execute("ROLLBACK")
                except sqlite3.OperationalError:
                    pass
                raise
            finally:
                cur.close()

    # --- Test helpers ----------------------------------------------------

    def insert_ledger_row(
        self,
        *,
        ts: datetime,
        provider: str,
        caller_subsystem: str,
        est_cost_usd: float,
        session_id: str | None = None,
    ) -> int:
        with self._lock:
            cur = self._conn.cursor()
            try:
                cur.execute(
                    "INSERT INTO usage_ledger (ts, session_id, request_count, provider, caller_subsystem, est_cost_usd) "
                    "VALUES (?, ?, 1, ?, ?, ?)",
                    (
                        ts.isoformat(),
                        session_id,
                        provider,
                        caller_subsystem,
                        est_cost_usd,
                    ),
                )
                rowid = cur.lastrowid
            finally:
                cur.close()
            # Commit so the INSERT does not leave an implicit
            # transaction hanging on the connection (sqlite3 default
            # isolation_level is autocommit-OFF, and bare writes do
            # not auto-commit on close).
            self._conn.commit()
        return rowid

    def update_ledger_ts_for_test(
        self,
        *,
        caller_subsystem: str,
        new_ts: datetime,
        est_cost_usd: float | None = None,
    ) -> None:
        """Rewrite ``usage_ledger.ts`` to simulate window rollover.

        The aggregate must agree with the fresh full-scan SUM after
        the window rolls. This helper ages rows out of the rolling
        window so ``reconcile_spend_aggregates`` can verify it
        correctly drops the stale contribution. We ``COMMIT`` the
        implicit write transaction immediately so the next
        ``transactional()`` block does not fail with ``cannot start a
        transaction within a transaction``.
        """
        with self._lock:
            cur = self._conn.cursor()
            try:
                if est_cost_usd is None:
                    cur.execute(
                        "UPDATE usage_ledger SET ts = ? WHERE caller_subsystem = ?",
                        (new_ts.isoformat(), caller_subsystem),
                    )
                else:
                    cur.execute(
                        "UPDATE usage_ledger SET ts = ? "
                        "WHERE caller_subsystem = ? AND est_cost_usd = ?",
                        (new_ts.isoformat(), caller_subsystem, est_cost_usd),
                    )
            finally:
                cur.close()
            self._conn.commit()

    def age_out_settled_reservations_for_test(
        self,
        *,
        caller_subsystem: str,
        new_settled_at: datetime,
        actual_cost_usd: float | None = None,
    ) -> None:
        """Rewrite ``knemon_cost_reservations.settled_at`` to age rows out.

        Pairs with ``update_ledger_ts_for_test`` for window-rollover
        simulations: the aggregate's reconciliation sums BOTH
        ``usage_ledger.ts`` AND ``knemon_cost_reservations.settled_at``
        when computing the committed total. Aging out only the ledger
        rows would leave the settled-reservation contribution still
        in window, defeating the test. We ``COMMIT`` the implicit
        write transaction immediately so the next ``transactional()``
        block does not fail with ``cannot start a transaction within
        a transaction``.
        """
        with self._lock:
            cur = self._conn.cursor()
            try:
                if actual_cost_usd is None:
                    cur.execute(
                        "UPDATE knemon_cost_reservations "
                        "SET settled_at = ? "
                        "WHERE caller_subsystem = ? AND status = 'settled'",
                        (new_settled_at.isoformat(), caller_subsystem),
                    )
                else:
                    cur.execute(
                        "UPDATE knemon_cost_reservations "
                        "SET settled_at = ? "
                        "WHERE caller_subsystem = ? AND status = 'settled' "
                        "AND actual_cost_usd = ?",
                        (new_settled_at.isoformat(), caller_subsystem, actual_cost_usd),
                    )
            finally:
                cur.close()
            self._conn.commit()

    def ledger_rows(self, *, caller_subsystem: str | None = None) -> list[dict]:
        with self._lock:
            cur = self._conn.cursor()
            try:
                if caller_subsystem is None:
                    cur.execute(
                        "SELECT id, ts, provider, caller_subsystem, est_cost_usd "
                        "FROM usage_ledger ORDER BY id"
                    )
                else:
                    cur.execute(
                        "SELECT id, ts, provider, caller_subsystem, est_cost_usd "
                        "FROM usage_ledger WHERE caller_subsystem = ? ORDER BY id",
                        (caller_subsystem,),
                    )
                cols = [c[0] for c in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
            finally:
                cur.close()

    def reservation_rows(self, *, caller_subsystem: str | None = None) -> list[dict]:
        with self._lock:
            cur = self._conn.cursor()
            try:
                if caller_subsystem is None:
                    cur.execute(
                        "SELECT attempt_id, provider, model, caller_subsystem, status, estimated_cost_usd, actual_cost_usd "
                        "FROM knemon_cost_reservations ORDER BY reserved_at"
                    )
                else:
                    cur.execute(
                        "SELECT attempt_id, provider, model, caller_subsystem, status, estimated_cost_usd, actual_cost_usd "
                        "FROM knemon_cost_reservations WHERE caller_subsystem = ? ORDER BY reserved_at",
                        (caller_subsystem,),
                    )
                cols = [c[0] for c in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
            finally:
                cur.close()

    def aggregate_rows(self, *, caller_subsystem: str | None = None) -> list[dict]:
        with self._lock:
            cur = self._conn.cursor()
            try:
                if caller_subsystem is None:
                    cur.execute(
                        "SELECT caller_subsystem, provider, window_start_ts, committed_usd, pending_usd, last_reconciled_at "
                        "FROM knemon_spend_window_aggregates ORDER BY caller_subsystem, provider"
                    )
                else:
                    cur.execute(
                        "SELECT caller_subsystem, provider, window_start_ts, committed_usd, pending_usd, last_reconciled_at "
                        "FROM knemon_spend_window_aggregates WHERE caller_subsystem = ? "
                        "ORDER BY caller_subsystem, provider",
                        (caller_subsystem,),
                    )
                cols = [c[0] for c in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
            finally:
                cur.close()


@pytest.fixture
def backend() -> _AggregateBackend:
    return _AggregateBackend()


@pytest.fixture(autouse=True)
def _patch_settings(monkeypatch):
    """Wire ``get_settings()`` to a duck-typed shim with caps disabled.

    The aggregate code path does not look at caps, but the reservation
    helpers (``reserve_cost`` / ``settle_cost``) do, so we keep the
    F04-era shim from the atomic-reservation tests.
    """

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


@pytest.fixture(autouse=True)
def _short_aggregate_window(monkeypatch):
    """Pin the aggregate window to a value the test can advance past.

    The default is 7 days, which the test cannot reasonably wait
    out. We use 60 seconds; the ``_advance_past_window`` helper
    manipulates ``usage_ledger.ts`` so we can simulate window
    rollover without actually waiting for clock time.
    """
    monkeypatch.setenv("MNEMOS_KNEMON_SPEND_AGGREGATE_WINDOW_SECONDS", "60")
    yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fresh_ledger_spend_usd(
    backend: _AggregateBackend,
    *,
    caller_subsystem: str,
    now: datetime,
) -> float:
    """Return the SUM(est_cost_usd) of ``usage_ledger`` rows within the rolling window,
    PLUS the SUM(actual_cost_usd) of settled reservations, PLUS outstanding pending.

    Mirrors the maintained aggregate's reconciliation SUM (see
    ``_reconcile_spend_aggregates_tx`` in ``budget.py``) so the test
    compares the aggregate against the same query the reconciliation
    itself runs. Without the settled-reservation term the F04-only
    callers would never see a fresh SUM > 0, because F04 writes to
    ``knemon_cost_reservations.actual_cost_usd`` and not to
    ``usage_ledger``.
    """
    window_seconds = int(
        os.environ.get("MNEMOS_KNEMON_SPEND_AGGREGATE_WINDOW_SECONDS", "60")
    )
    since = now - timedelta(seconds=window_seconds)
    with backend._lock:
        cur = backend._conn.cursor()
        try:
            # Pass ``datetime`` objects (not isoformat strings) so
            # sqlite3's deprecated-default datetime adapter formats
            # both sides of the comparison byte-identically. Passing
            # ``since.isoformat()`` (which uses 'T' between date and
            # time) silently mismatches against the space-separated
            # form sqlite3 stores and yields zero rows.
            cur.execute(
                "SELECT COALESCE(SUM(est_cost_usd), 0) FROM usage_ledger "
                "WHERE caller_subsystem = ? AND julianday(ts) >= julianday(?)",
                (caller_subsystem, since),
            )
            ledger_committed = float(cur.fetchone()[0] or 0.0)
            cur.execute(
                "SELECT COALESCE(SUM(actual_cost_usd), 0) FROM knemon_cost_reservations "
                "WHERE status = 'settled' AND caller_subsystem = ? AND julianday(settled_at) >= julianday(?)",
                (caller_subsystem, since),
            )
            settled_committed = float(cur.fetchone()[0] or 0.0)
            cur.execute(
                "SELECT COALESCE(SUM(estimated_cost_usd), 0) FROM knemon_cost_reservations "
                "WHERE status = 'pending' AND caller_subsystem = ? AND expires_at > ?",
                (caller_subsystem, now),
            )
            pending = float(cur.fetchone()[0] or 0.0)
        finally:
            cur.close()
    return ledger_committed + settled_committed + pending


# ---------------------------------------------------------------------------
# aggregate vs ledger parity
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_aggregate_matches_fresh_ledger_after_settle_cycle(backend):
    """After reserve+settle, aggregate == fresh ledger SUM.

    Pins the central invariant the F04 review demanded: the maintained
    aggregate must agree with the fresh ``SUM()`` over the rolling
    window, otherwise the O(1) read path silently poisons the budget
    math.
    """
    now = datetime.now(timezone.utc)
    caller = "pantheon"
    provider = "openai"

    # Five reserve+settle pairs across the rolling window.
    for i in range(5):
        attempt = f"settled-{i}"
        await reserve_cost(
            backend,
            provider=provider,
            model="gpt-4o",
            estimated_cost_usd=1.0,
            attempt_id=attempt,
            caller_subsystem=caller,
            now=now,
        )
        await settle_cost(
            backend,
            attempt_id=attempt,
            actual_cost_usd=1.0,
            now=now,
        )

    aggregate = await effective_spend_usd_aggregate(
        backend, caller_subsystem=caller, provider=None, now=now
    )
    fresh = await _fresh_spend_usd(
        backend, caller_subsystem=caller, provider=None, now=now
    )
    assert aggregate == pytest.approx(5.0)
    assert fresh == pytest.approx(5.0)
    assert aggregate == pytest.approx(fresh)


@pytest.mark.asyncio
async def test_aggregate_matches_fresh_ledger_after_reserve_cycle(backend):
    """After reserve+release (no settle), aggregate must still equal fresh SUM.

    ``release_cost`` returns the pending headroom. The aggregate's
    pending counter is decremented in the same transaction; the fresh
    SUM() picks the same answer up because the reservation row is
    no longer ``status='pending'``.
    """
    now = datetime.now(timezone.utc)
    caller = "pantheon"

    for i in range(3):
        attempt = f"reserved-{i}"
        await reserve_cost(
            backend,
            provider="openai",
            model="gpt-4o",
            estimated_cost_usd=2.0,
            attempt_id=attempt,
            caller_subsystem=caller,
            now=now,
        )

    pending_before = await effective_spend_usd_aggregate(
        backend, caller_subsystem=caller, provider=None, now=now
    )
    fresh_pending_before = await _fresh_spend_usd(
        backend, caller_subsystem=caller, provider=None, now=now
    )
    assert pending_before == pytest.approx(6.0)
    assert pending_before == pytest.approx(fresh_pending_before)

    await release_cost(backend, attempt_id="reserved-1", now=now)

    pending_after = await effective_spend_usd_aggregate(
        backend, caller_subsystem=caller, provider=None, now=now
    )
    fresh_pending_after = await _fresh_spend_usd(
        backend, caller_subsystem=caller, provider=None, now=now
    )
    assert pending_after == pytest.approx(4.0)
    assert pending_after == pytest.approx(fresh_pending_after)


@pytest.mark.asyncio
async def test_reconcile_spend_aggregates_reports_zero_drift(backend):
    """A fresh reconcile after the reserve+settle cycle reports zero drift."""
    now = datetime.now(timezone.utc)
    caller = "pantheon"

    for i in range(4):
        attempt = f"recon-{i}"
        await reserve_cost(
            backend,
            provider="openai",
            model="gpt-4o",
            estimated_cost_usd=0.5,
            attempt_id=attempt,
            caller_subsystem=caller,
            now=now,
        )
        await settle_cost(backend, attempt_id=attempt, actual_cost_usd=0.5, now=now)

    reports = await reconcile_spend_aggregates(
        backend, caller_subsystem=caller, provider=None, now=now
    )
    assert reports, "reconcile must return at least one report"
    report = reports[0]
    assert isinstance(report, AggregateDriftReport)
    # The maintain→rebuild→compare flow:
    #   1. Snapshot the maintained counter BEFORE rebuild.
    #   2. Compute fresh = SUM(usage_ledger) + SUM(settled reservations)
    #      + SUM(pending reservations with expires_at > now).
    #   3. Write ``fresh`` into the maintained row.
    #   4. Report ``drift = fresh - maintained`` so the operator can
    #      see what was wrong BEFORE the rebuild corrected it.
    # After 4 reserve+settle cycles of 0.5 USD each, settle_cost
    # incremented the maintained counter by 2.0 USD total — and the
    # pending decrement zero'd out the pending counter when each
    # reserve was paired with its settle. The fresh SUM agrees, so
    # drift is zero on both legs.
    assert report.committed_drift_usd == pytest.approx(0.0)
    assert report.pending_drift_usd == pytest.approx(0.0)
    assert report.committed_maintained_usd == pytest.approx(2.0)
    assert report.pending_maintained_usd == pytest.approx(0.0)
    assert report.committed_fresh_usd == pytest.approx(2.0)
    assert report.pending_fresh_usd == pytest.approx(0.0)
    assert not report.has_drift


# ---------------------------------------------------------------------------
# Window rollover — the aggregate must expire old contributions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_aggregate_drops_old_spend_after_window_rollover(backend):
    """Old spend must drop out of the aggregate after the window rolls.

    This is the regression the F04 review demanded: a counter that
    monotonically accumulates forever is worse than the original
    full-scan SUM, because it silently lets stale spend keep counting
    against the budget cap. The aggregate must expire old
    contributions when the window lower-bound advances.

    We can't actually wait 60 seconds in CI; instead we directly
    rewrite ``usage_ledger.ts`` to a value older than the window
    and ask ``effective_spend_usd_aggregate`` to fall back to a
    fresh full-scan SUM (which sees the aged row) and rebuild the
    counter from there.
    """
    now = datetime.now(timezone.utc)
    caller = "pantheon"
    provider = "openai"

    # Three settle events at "now" so they fall inside the rolling
    # window. We use ``reserve_cost`` + ``settle_cost`` (F04) so the
    # settled row materializes in the ledger and the aggregate.
    for i in range(3):
        attempt = f"current-{i}"
        await reserve_cost(
            backend,
            provider=provider,
            model="gpt-4o",
            estimated_cost_usd=2.0,
            attempt_id=attempt,
            caller_subsystem=caller,
            now=now,
        )
        await settle_cost(backend, attempt_id=attempt, actual_cost_usd=2.0, now=now)

    # Sanity: pre-rollover, the aggregate reports 6 USD and matches
    # the fresh ledger SUM.
    assert await effective_spend_usd_aggregate(
        backend, caller_subsystem=caller, provider=None, now=now
    ) == pytest.approx(6.0)
    assert await _fresh_spend_usd(
        backend, caller_subsystem=caller, provider=None, now=now
    ) == pytest.approx(6.0)

    # Rewrite the ledger rows so they fall OUTSIDE the rolling
    # window. The aggregate's incremental counter would otherwise
    # still report 6 USD because nothing has fired an incremental
    # decrement; this is the failure mode the F04 review warned
    # about. The reconciliation must correct it.
    aged = now - timedelta(hours=2)
    backend.update_ledger_ts_for_test(caller_subsystem=caller, new_ts=aged)
    # The aggregate's reconciliation also SUMs
    # ``knemon_cost_reservations.settled_at`` (F04-settled spend is
    # tracked in the reservations table, not the ledger). Aging out
    # only the ledger rows would still leave the settled-reservation
    # contribution in-window. Roll the F04 settled_at back too so
    # both legs of the fresh SUM agree the spend has rolled out.
    backend.age_out_settled_reservations_for_test(
        caller_subsystem=caller, new_settled_at=aged
    )

    # Reconciliation rebuilds the aggregate from a fresh SUM; the
    # aged rows are now outside the rolling window and drop out.
    reports = await reconcile_spend_aggregates(
        backend, caller_subsystem=caller, provider=None, now=now
    )
    assert reports
    report = reports[0]
    assert report.committed_fresh_usd == pytest.approx(0.0)
    assert report.committed_drift_usd == pytest.approx(-6.0)
    assert report.has_drift
    # The drift surfaces to the caller so they can investigate; the
    # counter has already been rebuilt to 0 by the reconciliation.

    # Subsequent reads return the rebuilt value: 0 USD (aged rows
    # are outside the window). The fresh ledger SUM agrees.
    post_rollover_aggregate = await effective_spend_usd_aggregate(
        backend, caller_subsystem=caller, provider=None, now=now
    )
    post_rollover_fresh = await _fresh_spend_usd(
        backend, caller_subsystem=caller, provider=None, now=now
    )
    assert post_rollover_aggregate == pytest.approx(0.0)
    assert post_rollover_aggregate == pytest.approx(post_rollover_fresh)


@pytest.mark.asyncio
async def test_aggregate_keeps_in_window_spend_after_window_rollover(backend):
    """Window rollover must drop ONLY out-of-window rows.

    Pins the partial-rollover case: half the spend is inside the
    new window, half is outside. The aggregate must retain the
    in-window half, drop the out-of-window half, and stay in
    agreement with the fresh ledger SUM.
    """
    now = datetime.now(timezone.utc)
    caller = "pantheon"

    # Two settle events at "now" (in-window).
    for i in range(2):
        attempt = f"keep-{i}"
        await reserve_cost(
            backend,
            provider="openai",
            model="gpt-4o",
            estimated_cost_usd=1.0,
            attempt_id=attempt,
            caller_subsystem=caller,
            now=now,
        )
        await settle_cost(backend, attempt_id=attempt, actual_cost_usd=1.0, now=now)

    # Two more events that we will age OUT of the window.
    for i in range(2):
        attempt = f"drop-{i}"
        await reserve_cost(
            backend,
            provider="openai",
            model="gpt-4o",
            estimated_cost_usd=3.0,
            attempt_id=attempt,
            caller_subsystem=caller,
            now=now,
        )
        await settle_cost(backend, attempt_id=attempt, actual_cost_usd=3.0, now=now)

    # Move the "drop" rows out of the window.
    aged = now - timedelta(hours=2)
    backend.update_ledger_ts_for_test(
        caller_subsystem=caller, new_ts=aged, est_cost_usd=3.0
    )
    # Same caveat as ``test_aggregate_drops_old_spend_after_window_rollover``:
    # F04-settled spend is read off ``knemon_cost_reservations.settled_at``,
    # so a ledger-only age-out would leave the "drop" reservation rows
    # still in-window and silently count toward the budget cap. Roll
    # the matching settled_at back too.
    backend.age_out_settled_reservations_for_test(
        caller_subsystem=caller, new_settled_at=aged, actual_cost_usd=3.0
    )

    # Reconcile so the maintained counter picks up the aged-out rows.
    await reconcile_spend_aggregates(
        backend, caller_subsystem=caller, provider=None, now=now
    )

    aggregate = await effective_spend_usd_aggregate(
        backend, caller_subsystem=caller, provider=None, now=now
    )
    fresh = _fresh_ledger_spend_usd(backend, caller_subsystem=caller, now=now)
    # In-window spend: 2 events * 1.0 = 2.0.
    assert aggregate == pytest.approx(2.0)
    assert aggregate == pytest.approx(fresh)


# ---------------------------------------------------------------------------
# Provider-scoped parity
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_provider_scoped_aggregate_matches_provider_filter(backend):
    """The aggregate must agree with the fresh SUM at provider granularity too.

    The admission check ``evaluate_usage_budget`` consults both the
    unfiltered (``provider=None``) and the per-provider aggregate.
    Both rows are maintained by ``reserve_cost`` / ``settle_cost``;
    both must agree with the corresponding ``effective_spend_usd``
    full-scan.
    """
    now = datetime.now(timezone.utc)
    caller = "pantheon"

    # Spend against openai.
    for i in range(3):
        attempt = f"openai-{i}"
        await reserve_cost(
            backend,
            provider="openai",
            model="gpt-4o",
            estimated_cost_usd=1.0,
            attempt_id=attempt,
            caller_subsystem=caller,
            now=now,
        )
        await settle_cost(backend, attempt_id=attempt, actual_cost_usd=1.0, now=now)

    # Spend against anthropic.
    for i in range(2):
        attempt = f"anthropic-{i}"
        await reserve_cost(
            backend,
            provider="anthropic",
            model="claude-sonnet-4-6",
            estimated_cost_usd=2.5,
            attempt_id=attempt,
            caller_subsystem=caller,
            now=now,
        )
        await settle_cost(backend, attempt_id=attempt, actual_cost_usd=2.5, now=now)

    # Unfiltered (provider=None): both totals = 3*1 + 2*2.5 = 8.
    unfiltered_agg = await effective_spend_usd_aggregate(
        backend, caller_subsystem=caller, provider=None, now=now
    )
    unfiltered_fresh = await _fresh_spend_usd(
        backend, caller_subsystem=caller, provider=None, now=now
    )
    assert unfiltered_agg == pytest.approx(8.0)
    assert unfiltered_agg == pytest.approx(unfiltered_fresh)

    # Provider-scoped: openai = 3.0; anthropic = 5.0.
    openai_agg = await effective_spend_usd_aggregate(
        backend, caller_subsystem=caller, provider="openai", now=now
    )
    openai_fresh = await _fresh_spend_usd(
        backend, caller_subsystem=caller, provider="openai", now=now
    )
    anthropic_agg = await effective_spend_usd_aggregate(
        backend, caller_subsystem=caller, provider="anthropic", now=now
    )
    anthropic_fresh = await _fresh_spend_usd(
        backend, caller_subsystem=caller, provider="anthropic", now=now
    )
    assert openai_agg == pytest.approx(3.0)
    assert openai_agg == pytest.approx(openai_fresh)
    assert anthropic_agg == pytest.approx(5.0)
    assert anthropic_agg == pytest.approx(anthropic_fresh)
