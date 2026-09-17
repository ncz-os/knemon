"""Regression: KNEMON's budget queries must never over-bind on Oracle.

Production incident, PYTHIA (authoritative MNEMOS, Oracle-backed, v7.0.0),
2026-09-17: EVERY GRAEAE consultation returned HTTP 503, for every mode and
every provider including the free and local ones. The service log said:

    mnemos.domain.knemon.budget: KNEMON budget ledger query failed; denying
    under configured cap: DPY-4008: no bind placeholder named ":now_ts" was
    found in the SQL text

``_spend_components_tx`` built ONE bind dict --
``{caller_subsystem, since_ts, now_ts}`` -- and handed the same dict to three
statements with three different bind sets. ``:now_ts`` appears in none of the
three; ``:since_ts`` appears in only two. Every other supported driver ignores
a bind its statement does not reference (the PostgreSQL/MySQL/Db2 rewriters
build their argument list by walking the SQL text; SQLite's driver ignores
extra dict keys), so the defect was invisible on every backend CI exercises.
python-oracledb rejects it outright.

The blast radius came from the deliberate fail-closed design: any exception in
the ledger query denies the request, and the budget gate runs BEFORE provider
selection, so providers that would have cost $0 were refused before being
tried. The fail-closed behavior is correct and is not what these tests pin.

Two independent guards are covered here, so neither alone is load-bearing:

1. ``_named_params`` gives the named-bind (Oracle) execute path the same
   tolerance the other four dialects already have.
2. The budget module's own call sites pass per-statement binds, verified with
   guard 1 disabled so a regression in ``budget.py`` cannot hide behind it.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from mnemos.domain.knemon import budget as B
from mnemos.domain.knemon import router as R


class OracleDatabaseError(Exception):
    """Stand-in for ``oracledb.DatabaseError``."""


class _StrictOracleCursor:
    """Cursor that rejects extra binds exactly as python-oracledb does.

    python-oracledb raises DPY-4008 when the caller supplies a bind the
    statement text does not contain. Reproducing that here is what makes this
    file able to fail on a backend CI has no service container for.
    """

    def __init__(self, recorder: list[tuple[str, dict[str, Any]]]) -> None:
        self._recorder = recorder
        self.description: list[tuple[str, ...]] = []
        self._rows: list[tuple[Any, ...]] = []
        self.rowcount = 0

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        values = params or {}
        self._recorder.append((sql, dict(values)))
        present = {match.group(1) for match in R._NAMED_PARAM_RE.finditer(sql)}
        for name in values:
            if name not in present:
                raise OracleDatabaseError(
                    f'DPY-4008: no bind placeholder named ":{name}" was '
                    "found in the SQL text"
                )
        self.description, self._rows = _canned_result(sql)

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows

    def close(self) -> None:
        return None


def _canned_result(sql: str) -> tuple[list[tuple[str, ...]], list[tuple[Any, ...]]]:
    """Minimal result shapes, enough to walk the real code paths."""
    if "COALESCE(SUM(" in sql:
        return [("amount",)], [(0.0,)]
    if "SELECT DISTINCT caller_subsystem" in sql:
        return [("caller_subsystem",)], [("graeae",)]
    if "SELECT attempt_id FROM knemon_cost_reservations" in sql:
        return [("attempt_id",)], [("attempt-1",)]
    if "FROM knemon_cost_reservation_locks" in sql:
        return [("caller_subsystem",)], [("graeae",)]
    return [], []


class _StrictOracleConnection:
    """``_connection_dialect`` resolves this to "oracle" via the class name.

    It deliberately has no ``fetch`` attribute, which is what that helper
    checks first to detect asyncpg.
    """

    def __init__(self) -> None:
        self.statements: list[tuple[str, dict[str, Any]]] = []

    def cursor(self) -> _StrictOracleCursor:
        return _StrictOracleCursor(self.statements)


class _StrictOracleBackend:
    def __init__(self) -> None:
        self.conn = _StrictOracleConnection()

    @asynccontextmanager
    async def transactional(self):
        yield self.conn


@pytest.fixture
def unguarded_named_params(monkeypatch: pytest.MonkeyPatch):
    """Disable the ``_named_params`` safety net for the duration of a test.

    Without this the helper would filter the extra binds away before the
    driver ever saw them, and a regression reintroduced in ``budget.py`` would
    pass unnoticed. Tests that use this fixture assert the budget module is
    correct on its own.
    """
    monkeypatch.setattr(R, "_named_params", lambda sql, params: dict(params or {}))


def test_named_params_drops_binds_the_statement_does_not_reference() -> None:
    sql = "SELECT 1 FROM t WHERE a = :kept AND b >= :also_kept"
    filtered = R._named_params(sql, {"kept": 1, "also_kept": 2, "extra": 3})
    assert filtered == {"kept": 1, "also_kept": 2}


def test_named_params_keeps_every_bind_the_statement_does_reference() -> None:
    """The filter must only ever remove; a referenced bind is never dropped.

    This is the property that keeps a genuine name mismatch loud: the statement
    keeps its unbound ``:typo`` placeholder and the driver still raises, rather
    than the mismatch being silently swallowed.
    """
    sql = "UPDATE t SET x = :now_ts WHERE id = :id"
    assert R._named_params(sql, {"now_ts": 1, "id": 2}) == {"now_ts": 1, "id": 2}
    assert R._named_params(sql, {}) == {}


@pytest.mark.parametrize("provider", [None, "*", "claude", "groq"])
@pytest.mark.asyncio
async def test_effective_spend_never_over_binds_on_oracle(
    unguarded_named_params, provider: str | None
) -> None:
    """The exact production failure: the budget read path under Oracle.

    Reverting the ``_spend_components_tx`` fix makes this raise DPY-4008 on the
    first statement, which is what took every GRAEAE consultation down.
    """
    conn = _StrictOracleConnection()
    now = datetime.now(timezone.utc)

    spend = await B.effective_spend_usd_tx(
        conn, caller_subsystem="graeae", provider=provider, now=now
    )

    assert spend == 0.0
    assert len(conn.statements) == 3
    for sql, params in conn.statements:
        present = {match.group(1) for match in R._NAMED_PARAM_RE.finditer(sql)}
        assert set(params) <= present, f"over-bound {set(params) - present} in {sql}"
    # The window-scoped reads carry :since_ts; the pending sum is not
    # window-scoped and must not be handed one.
    assert "since_ts" in conn.statements[0][1]
    assert "since_ts" in conn.statements[1][1]
    assert "since_ts" not in conn.statements[2][1]
    assert not any("now_ts" in params for _, params in conn.statements)


@pytest.mark.asyncio
async def test_cleanup_cost_reservations_never_over_binds_on_oracle(
    unguarded_named_params,
) -> None:
    """Second site found by the same audit: the expiry/recovery INSERT.

    ``cleanup_cost_reservations`` shared one dict between an INSERT that
    references only ``:id`` and an UPDATE that also references ``:now_ts``.
    GRAEAE never reaches this path, so it was a separate latent Oracle outage
    in the reservation-expiry sweep rather than a second symptom of the same
    one.
    """
    backend = _StrictOracleBackend()

    await B.cleanup_cost_reservations(backend, now=datetime.now(timezone.utc))

    statements = backend.conn.statements
    assert statements, "expected the cleanup sweep to issue statements"
    for sql, params in statements:
        present = {match.group(1) for match in R._NAMED_PARAM_RE.finditer(sql)}
        assert set(params) <= present, f"over-bound {set(params) - present} in {sql}"
    inserts = [
        params
        for sql, params in statements
        if "INSERT INTO knemon_cost_recovery" in sql
    ]
    assert inserts, "expected the expired reservation to be recorded for recovery"
    assert all(set(params) == {"id"} for params in inserts)


@pytest.mark.asyncio
async def test_named_params_guard_absorbs_an_over_bound_statement() -> None:
    """Guard 1 on its own: an over-bound call survives the Oracle path.

    This is the net that stops the *class* of defect from reaching production
    again, not just the two sites fixed here.
    """
    conn = _StrictOracleConnection()

    rows = await R._tx_rows(
        conn,
        "SELECT COALESCE(SUM(est_cost_usd), 0) AS amount FROM usage_ledger "
        "WHERE caller_subsystem = :caller_subsystem",
        {"caller_subsystem": "graeae", "now_ts": datetime.now(timezone.utc)},
    )

    assert rows == [{"amount": 0.0}]
    assert set(conn.statements[0][1]) == {"caller_subsystem"}


@pytest.mark.asyncio
async def test_strict_cursor_fake_actually_rejects_extra_binds() -> None:
    """The fake must fail the way oracledb does, or every test above is hollow.

    Without this, a fake that quietly accepted anything would make the whole
    file pass against the unfixed code.
    """
    conn = _StrictOracleConnection()
    cursor = conn.cursor()

    with pytest.raises(OracleDatabaseError, match="DPY-4008"):
        cursor.execute(
            "SELECT 1 FROM usage_ledger WHERE caller_subsystem = :caller_subsystem",
            {"caller_subsystem": "graeae", "now_ts": 1},
        )

    assert R._connection_dialect(conn) == "oracle"


@pytest.mark.asyncio
async def test_window_binds_still_scope_the_ledger_read() -> None:
    """The fix must not quietly drop the 7-day window from the read path."""
    conn = _StrictOracleConnection()
    now = datetime.now(timezone.utc)

    await B.effective_spend_usd_tx(conn, caller_subsystem="graeae", now=now)

    ledger_sql, ledger_params = conn.statements[0]
    assert ":since_ts" in ledger_sql
    assert ledger_params["since_ts"] == now - timedelta(days=7)
