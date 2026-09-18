"""Regression coverage for Oracle binds in KNEMON utilization queries."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from mnemos.api.routes import knemon_dashboard as D
from mnemos.api.routes import knemon_utilization as U
from mnemos.domain.knemon import router as R


class OracleDatabaseError(Exception):
    """Stand-in for ``oracledb.DatabaseError``."""


class _StrictOracleCursor:
    """Reject extra named binds exactly as python-oracledb does."""

    def __init__(self, statements: list[tuple[str, dict[str, Any]]]) -> None:
        self._statements = statements
        self.description = [
            ("ROW_COUNT",),
            ("REQUESTS_USED",),
            ("COST_USD",),
            ("TOKENS",),
        ]

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        values = dict(params or {})
        self._statements.append((sql, values))
        present = {match.group(1) for match in R._NAMED_PARAM_RE.finditer(sql)}
        for name in values:
            if name not in present:
                raise OracleDatabaseError(
                    f'DPY-4008: no bind placeholder named ":{name}" was '
                    "found in the SQL text"
                )
        if "path_kind" in present:
            raise OracleDatabaseError('ORA-00904: "PATH_KIND": invalid identifier')

    def fetchall(self) -> list[tuple[int, int, float, int]]:
        return [(1, 9, 0.25, 1234)]

    def close(self) -> None:
        return None


class _StrictOracleConnection:
    """Oracle-shaped connection without asyncpg's ``fetch`` method."""

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


@pytest.mark.asyncio
async def test_dashboard_named_bind_path_filters_unused_oracle_params() -> None:
    """Dashboard's shared named-bind path must use the same Oracle guard."""
    conn = _StrictOracleConnection()

    await D._connection_rows(
        conn,
        "SELECT :provider AS row_count, 0 AS requests_used, "
        "0 AS cost_usd, 0 AS tokens FROM dual",
        {"provider": "anthropic", "unused": "rejected by python-oracledb"},
    )

    assert conn.statements[0][1] == {"provider": "anthropic"}


@pytest.mark.asyncio
async def test_usage_legacy_schema_fallback_never_over_binds_oracle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fallback must omit new-schema binds rejected by python-oracledb."""
    backend = _StrictOracleBackend()
    monkeypatch.setattr(U, "backend_or_503", lambda: backend)
    start = datetime(2026, 9, 1, tzinfo=UTC)
    end = start + timedelta(days=30)

    usage = await U._usage_for_plan(
        {
            "provider": "anthropic",
            "plan_name": "max",
            "auth_method": "subscription",
            "path_kind": "subscription",
        },
        start,
        end,
        "monthly:2026-09",
    )

    assert usage == {
        "row_count": 1,
        "requests_used": 9,
        "cost_usd": 0.25,
        "tokens": 1234,
    }
    assert len(backend.conn.statements) == 2
    primary_sql, primary_params = backend.conn.statements[0]
    fallback_sql, fallback_params = backend.conn.statements[1]
    assert "path_kind" in primary_sql
    assert "path_kind" not in fallback_sql
    assert set(primary_params) == {
        "provider",
        "plan_name",
        "path_kind",
        "legacy_path_kind",
        "start_ts",
        "end_ts",
        "window_id",
    }
    assert set(fallback_params) == {
        "provider",
        "plan_name",
        "start_ts",
        "end_ts",
        "window_id",
    }
    for sql, params in backend.conn.statements:
        present = {match.group(1) for match in R._NAMED_PARAM_RE.finditer(sql)}
        assert set(params) <= present
