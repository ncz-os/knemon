from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import pytest

from mnemos.api.routes import knemon_dashboard, knemon_utilization
from mnemos.domain.knemon.router import _qmark_sql


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
