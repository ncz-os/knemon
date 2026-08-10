from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from mnemos.api.routes import knemon_dashboard, knemon_utilization


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
