from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import pytest

from mnemos.api.routes import knemon_dashboard, knemon_utilization
from mnemos.domain.knemon.router import _qmark_sql, _row_limited_sql


class _AsyncpgConnection:
    def __init__(self) -> None:
        self.calls = []

    async def fetch(self, sql, *params):
        self.calls.append((sql, params))
        return [{"VALUE": 7}]


class _Backend:
    def __init__(self) -> None:
        self.conn = _AsyncpgConnection()

    @asynccontextmanager
    async def transactional(self):
        yield self.conn


@pytest.mark.parametrize("module", [knemon_dashboard, knemon_utilization])
@pytest.mark.asyncio
async def test_endpoint_rows_support_asyncpg_connections(monkeypatch, module):
    backend = _Backend()
    monkeypatch.setattr(module, "backend_or_503", lambda: backend)

    rows = await module._rows("SELECT :value AS value", {"value": 7})

    assert rows == [{"value": 7}]
    assert backend.conn.calls == [("SELECT $1 AS value", (7,))]


def test_db2_native_named_parameters_convert_to_qmark_positional():
    sql, params = _qmark_sql(
        "SELECT :session_id FROM knemon_burn_sessions WHERE session_id = :session_id",
        {"session_id": "worker-1"},
    )

    assert sql == "SELECT ? FROM knemon_burn_sessions WHERE session_id = ?"
    assert params == ["worker-1", "worker-1"]


class _Db2Cursor:
    description = [("VALUE",)]

    def __init__(self) -> None:
        self.executed = None

    async def execute(self, sql, params):
        self.executed = (sql, params)

    async def fetchall(self):
        return [(7,)]

    async def close(self):
        return None


class _Db2Connection:
    def __init__(self) -> None:
        self.cursor_obj = _Db2Cursor()

    async def cursor(self):
        return self.cursor_obj


_Db2Connection.__module__ = "ibm_db_dbi"


@pytest.mark.asyncio
async def test_dashboard_rows_use_db2_qmark_paramstyle():
    conn = _Db2Connection()

    rows = await knemon_dashboard._connection_rows(
        conn, "SELECT :value AS value", {"value": 7}
    )

    assert rows == [{"value": 7}]
    assert conn.cursor_obj.executed == ("SELECT ? AS value", (7,))


@pytest.mark.asyncio
async def test_utilization_rows_use_db2_qmark_paramstyle(monkeypatch):
    conn = _Db2Connection()

    class Backend:
        @asynccontextmanager
        async def transactional(self):
            yield conn

    monkeypatch.setattr(knemon_utilization, "backend_or_503", lambda: Backend())

    rows = await knemon_utilization._rows("SELECT :value AS value", {"value": 7})

    assert rows == [{"value": 7}]
    assert conn.cursor_obj.executed == ("SELECT ? AS value", (7,))


@pytest.mark.parametrize(
    ("dialect", "clause"),
    [
        ("postgres", "LIMIT 50"),
        ("sqlite", "LIMIT 50"),
        ("mysql", "LIMIT 50"),
        ("oracle", "FETCH FIRST 50 ROWS ONLY"),
        ("db2", "FETCH FIRST 50 ROWS ONLY"),
    ],
)
def test_dashboard_limit_is_applied_in_each_supported_database(dialect, clause):
    sql = knemon_dashboard._limited_sql("SELECT id FROM usage_ledger ORDER BY ts DESC", dialect, 50)

    assert sql.endswith(clause)


@pytest.mark.parametrize(
    ("dialect", "clause"),
    [
        ("postgres", "LIMIT 50"),
        ("sqlite", "LIMIT 50"),
        ("mysql", "LIMIT 50"),
        ("oracle", "FETCH FIRST 50 ROWS ONLY"),
        ("db2", "FETCH FIRST 50 ROWS ONLY"),
    ],
)
def test_reservation_reconciliation_limit_is_applied_for_each_database(dialect, clause):
    sql = _row_limited_sql(
        "SELECT reservation_id FROM knemon_burn_reservations ORDER BY reserved_at",
        dialect,
        50,
    )

    assert sql.endswith(clause)


@pytest.mark.asyncio
async def test_dashboard_cache_is_lru_bounded_and_configurable(monkeypatch):
    knemon_dashboard._CACHE.clear()
    monkeypatch.setenv("MNEMOS_KNEMON_DASHBOARD_CACHE_MAX_ENTRIES", "2")

    async def load(value):
        return value

    assert await knemon_dashboard._cached(("a",), lambda: load("a")) == "a"
    assert await knemon_dashboard._cached(("b",), lambda: load("b")) == "b"
    assert await knemon_dashboard._cached(("a",), lambda: load("new-a")) == "a"
    assert await knemon_dashboard._cached(("c",), lambda: load("c")) == "c"

    assert list(knemon_dashboard._CACHE) == [("a",), ("c",)]
    knemon_dashboard._CACHE.clear()


@pytest.mark.parametrize("dialect", ["postgres", "sqlite", "mysql", "oracle", "db2"])
@pytest.mark.parametrize("bucket", ["1h", "1d"])
def test_dashboard_timeline_groups_in_each_supported_database(dialect, bucket):
    sql = knemon_dashboard._timeline_sql(dialect, bucket)

    assert "GROUP BY" in sql
    assert "SELECT ts, est_cost_usd" not in sql
    assert "COUNT(*) AS row_count" in sql
    assert "SUM(est_cost_usd)" in sql


def test_sqlite_dashboard_queries_bound_and_aggregate_populated_ledger():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE usage_ledger (id INTEGER, ts TEXT, est_cost_usd NUMERIC)"
    )
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    conn.executemany(
        "INSERT INTO usage_ledger VALUES (?, ?, ?)",
        [
            (index, (now - timedelta(hours=index % 4)).isoformat(), 0.25)
            for index in range(1000)
        ],
    )

    recent_sql = knemon_dashboard._limited_sql(
        "SELECT id FROM usage_ledger ORDER BY ts DESC", "sqlite", 50
    )
    recent_rows = conn.execute(recent_sql).fetchall()
    timeline_rows = conn.execute(
        knemon_dashboard._timeline_sql("sqlite", "1h"),
        {
            "since_ts": (now - timedelta(days=7)).isoformat(),
            "until_ts": now.isoformat(),
        },
    ).fetchall()
    daily_rows = conn.execute(
        knemon_dashboard._timeline_sql("sqlite", "1d"),
        {
            "since_ts": (now - timedelta(days=7)).isoformat(),
            "until_ts": now.isoformat(),
        },
    ).fetchall()

    assert len(recent_rows) == 50
    assert len(timeline_rows) == 4
    assert sum(row[-2] for row in timeline_rows) == 1000
    assert len(daily_rows) <= 2
    assert sum(row[-2] for row in daily_rows) == 1000


@pytest.mark.parametrize("dialect", ["postgres", "sqlite", "mysql", "oracle", "db2"])
def test_model_registry_predicate_matches_each_backend_boolean_shape(dialect):
    """Regression: the recommendations route 500'd on PostgreSQL.

    ``model_registry.available`` / ``.deprecated`` are BOOLEAN on Postgres but
    integer-family columns elsewhere (INTEGER on SQLite, NUMBER(1) on Oracle,
    SMALLINT on Db2). Postgres has no implicit boolean-to-integer cast, so the
    previously hardcoded ``available = 1 AND COALESCE(deprecated, 0) = 0``
    raised ``operator does not exist: boolean = integer`` there.
    """
    predicate = knemon_dashboard._model_registry_live_predicate(dialect)
    assert "available" in predicate
    assert "deprecated" in predicate
    if dialect == "postgres":
        assert "= 1" not in predicate
        assert "COALESCE(deprecated, 0)" not in predicate
        assert predicate == "available = TRUE AND deprecated IS NOT TRUE"
    else:
        assert predicate == "available = 1 AND COALESCE(deprecated, 0) = 0"


def test_model_registry_predicate_filters_live_rows_on_sqlite_integer_columns():
    """Execute the non-Postgres branch for real, both directions."""
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE model_registry (provider TEXT, model_id TEXT, "
        "available INTEGER NOT NULL DEFAULT 1, "
        "deprecated INTEGER NOT NULL DEFAULT 0)"
    )
    conn.executemany(
        "INSERT INTO model_registry VALUES (?, ?, ?, ?)",
        [
            ("live", "m-live", 1, 0),
            ("unavailable", "m-gone", 0, 0),
            ("deprecated", "m-old", 1, 1),
        ],
    )
    predicate = knemon_dashboard._model_registry_live_predicate("sqlite")
    rows = conn.execute(
        f"SELECT provider FROM model_registry WHERE {predicate}"
    ).fetchall()
    conn.close()
    assert [row[0] for row in rows] == ["live"]
