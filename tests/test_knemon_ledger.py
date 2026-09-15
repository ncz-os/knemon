from __future__ import annotations

import sqlite3
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
from importlib.resources import files
from types import SimpleNamespace

import mnemos.api.routes.ledger as ledger_mod
import mnemos.core.lifecycle as lifecycle
import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from mnemos.api.dependencies import UserContext, get_current_user
from mnemos.api.routes.ledger import router as ledger_router
from mnemos.core.config import get_settings
from mnemos.core.plan_windows import _PLAN_WINDOWS
from mnemos.domain.knemon.router import (
    _reserve_session_burn,
    reconcile_burn_reservation,
)
from mnemos.persistence.base import UsageLedgerRecord, UsageLedgerResult
from mnemos.persistence.postgres import PostgresBackend, PostgresTransaction


class _RawTx:
    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None


class _RecorderConn:
    def __init__(self) -> None:
        self.sql: str | None = None
        self.args: tuple | None = None
        self.row = {
            "id": 42,
            "est_cost_usd": Decimal("0.000324"),
            "registry_match": True,
            "auth_method": "api",
        }

    async def fetchrow(self, sql: str, *args):
        self.sql = sql
        self.args = args
        return self.row


class _OracleVar:
    def __init__(self) -> None:
        self.value = None

    def getvalue(self):
        return self.value

    def setvalue(self, value):
        self.value = value


class _OracleLedgerCursor:
    def __init__(self) -> None:
        self.sql_calls: list[str] = []

    def var(self, _kind):
        return _OracleVar()

    async def execute(self, sql: str, params: dict):
        self.sql_calls.append(sql)
        if "FROM subscription_plans" in sql:
            raise Exception("ORA-00942: table or view does not exist")
        if "FROM model_registry" in sql:
            raise Exception("ORA-00942: table or view does not exist")
        params["rid"].setvalue(77)
        params["rcost"].setvalue(Decimal("0"))

    async def close(self) -> None:
        return None


class _OracleLedgerConn:
    def __init__(self) -> None:
        self.cursor_obj = _OracleLedgerCursor()

    def cursor(self):
        return self.cursor_obj


class _Db2MissingTableError(Exception):
    sqlstate = "42704"
    sqlcode = "-204"


class _Db2LedgerCursor:
    def __init__(self) -> None:
        self.sql_calls: list[str] = []
        self.params_calls: list[tuple] = []
        self.row = (88, Decimal("0"))

    async def execute(self, sql: str, params: tuple):
        self.sql_calls.append(sql)
        self.params_calls.append(params)
        if "subscription_plans" in sql or "model_registry" in sql:
            raise _Db2MissingTableError(
                "SQL0204N model_registry is an undefined name. SQLSTATE=42704"
            )

    async def fetchone(self):
        return self.row

    async def close(self) -> None:
        return None


class _Db2LedgerConn:
    def __init__(self) -> None:
        self.cursor_obj = _Db2LedgerCursor()

    def cursor(self):
        return self.cursor_obj


def _settings():
    return SimpleNamespace(database=SimpleNamespace(embedding_dim=768))


@pytest.mark.asyncio
async def test_postgres_record_usage_ledger_inserts_with_registry_prices():
    conn = _RecorderConn()
    tx = PostgresTransaction(conn, _RawTx())  # type: ignore[arg-type]
    backend = PostgresBackend(pool=SimpleNamespace(), settings=_settings())  # type: ignore[arg-type]

    result = await backend.record_usage_ledger(
        tx,
        UsageLedgerRecord(
            provider="openai",
            model="gpt-4o",
            task_kind="code",
            tokens_in=1200,
            tokens_out=340,
            tokens_reasoning=10,
            latency_ms=1240,
            outcome="ok",
            caller_subsystem="pantheon",
            tier="standard",
        ),
    )

    assert result == UsageLedgerResult(id=42, est_cost_usd=Decimal("0.000324"))
    assert conn.args == (
        "openai",
        "gpt-4o",
        "code",
        1200,
        340,
        10,
        1240,
        "ok",
        "pantheon",
        "standard",
        None,
        1,
        None,
        "api",
    )
    assert conn.sql is not None
    assert "FROM model_registry" in conn.sql
    assert "INSERT INTO usage_ledger" in conn.sql
    assert "est_cost_usd" in conn.sql
    assert "$1" in conn.sql and "$14" in conn.sql
    assert "resolved_prices" in conn.sql
    assert "resolved_plan" in conn.sql


@pytest.mark.asyncio
async def test_postgres_record_usage_ledger_records_and_warns_for_unknown_model(caplog):
    conn = _RecorderConn()
    conn.row = {
        "id": 43,
        "est_cost_usd": Decimal("0"),
        "registry_match": False,
        "auth_method": "api",
    }
    tx = PostgresTransaction(conn, _RawTx())  # type: ignore[arg-type]
    backend = PostgresBackend(pool=SimpleNamespace(), settings=_settings())  # type: ignore[arg-type]

    with caplog.at_level("WARNING"):
        result = await backend.record_usage_ledger(
            tx,
            UsageLedgerRecord(
                provider="unknown",
                model="missing",
                task_kind="code",
                tokens_in=1,
                tokens_out=1,
                tokens_reasoning=0,
                latency_ms=1,
                outcome="ok",
                caller_subsystem="pantheon",
                tier="standard",
            ),
        )

    assert result == UsageLedgerResult(id=43, est_cost_usd=Decimal("0"))
    assert "model_registry price missing" in caplog.text


@pytest.mark.asyncio
async def test_oracle_record_usage_ledger_records_when_model_registry_missing(
    monkeypatch, caplog
):
    monkeypatch.setitem(
        sys.modules, "oracledb", SimpleNamespace(DB_TYPE_NUMBER=object())
    )

    from mnemos.persistence.oracle import OracleBackend

    conn = _OracleLedgerConn()
    backend = OracleBackend(pool=SimpleNamespace(), settings=SimpleNamespace())  # type: ignore[arg-type]

    with caplog.at_level("WARNING"):
        result = await backend.record_usage_ledger(
            SimpleNamespace(conn=conn),
            UsageLedgerRecord(
                provider="unknown",
                model="missing",
                task_kind="code",
                tokens_in=1,
                tokens_out=1,
                tokens_reasoning=0,
                latency_ms=1,
                outcome="ok",
                caller_subsystem="pantheon",
                tier="standard",
            ),
        )

    assert result == UsageLedgerResult(id=77, est_cost_usd=Decimal("0"))
    assert len(conn.cursor_obj.sql_calls) == 3
    assert "FROM subscription_plans" in conn.cursor_obj.sql_calls[0]
    assert "FROM model_registry" in conn.cursor_obj.sql_calls[1]
    assert "FROM model_registry" not in conn.cursor_obj.sql_calls[2]
    assert "model_registry table missing" in caplog.text


@pytest.mark.asyncio
async def test_db2_record_usage_ledger_records_when_model_registry_missing(caplog):
    from mnemos.persistence.db2 import Db2Backend

    conn = _Db2LedgerConn()
    backend = Db2Backend(pool=SimpleNamespace(), settings=SimpleNamespace())  # type: ignore[arg-type]

    with caplog.at_level("WARNING"):
        result = await backend.record_usage_ledger(
            SimpleNamespace(conn=conn),
            UsageLedgerRecord(
                provider="unknown",
                model="missing",
                task_kind="code",
                tokens_in=1,
                tokens_out=1,
                tokens_reasoning=0,
                latency_ms=1,
                outcome="ok",
                caller_subsystem="pantheon",
                tier="standard",
            ),
        )

    assert result == UsageLedgerResult(id=88, est_cost_usd=Decimal("0"))
    assert len(conn.cursor_obj.sql_calls) == 3
    assert "subscription_plans" in conn.cursor_obj.sql_calls[0]
    assert "FROM model_registry" in conn.cursor_obj.sql_calls[1]
    assert "FROM model_registry" not in conn.cursor_obj.sql_calls[2]
    assert conn.cursor_obj.params_calls[2] == (
        "unknown",
        "missing",
        "code",
        1,
        1,
        0,
        1,
        "ok",
        "pantheon",
        "standard",
        None,
        1,
        None,
        "api",
        0,
    )
    assert "model_registry table missing" in caplog.text


class _EndpointBackend:
    def __init__(self) -> None:
        self.records: list[UsageLedgerRecord] = []
        self.conn = sqlite3.connect(":memory:")
        self.conn.executescript(
            """
            CREATE TABLE knemon_burn_sessions (
              session_id TEXT PRIMARY KEY,
              touched_at TIMESTAMP NOT NULL
            );
            CREATE TABLE knemon_burn_reservations (
              reservation_id TEXT PRIMARY KEY,
              session_id TEXT NOT NULL,
              reserved_at TIMESTAMP NOT NULL,
              expires_at TIMESTAMP NOT NULL,
              request_count INTEGER NOT NULL DEFAULT 1,
              status TEXT NOT NULL,
              reconciled_at TIMESTAMP,
              ledger_id INTEGER
            );
            CREATE TABLE usage_ledger (
              id INTEGER PRIMARY KEY,
              ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              session_id TEXT,
              caller_subsystem TEXT,
              request_count INTEGER NOT NULL DEFAULT 1
            );
            """
        )

    @asynccontextmanager
    async def transactional(self):
        try:
            yield self.conn
        except BaseException:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()

    async def record_usage_ledger(
        self, tx, record: UsageLedgerRecord
    ) -> UsageLedgerResult:
        self.records.append(record)
        tx.execute(
            "INSERT INTO usage_ledger (id, ts, session_id, request_count) VALUES (7, ?, ?, ?)",
            (datetime.now(timezone.utc), record.session_id, record.request_count),
        )
        return UsageLedgerResult(id=7, est_cost_usd=Decimal("0.000324"))


def _user() -> UserContext:
    return UserContext(
        user_id="alice",
        group_ids=[],
        role="user",
        namespace="default",
        authenticated=True,
    )


def _root() -> UserContext:
    return UserContext(
        user_id="root",
        group_ids=[],
        role="root",
        namespace="default",
        authenticated=True,
    )


@pytest.mark.asyncio
async def test_ledger_endpoint_requires_root_and_records(monkeypatch):
    backend = _EndpointBackend()
    monkeypatch.setattr(lifecycle, "_persistence_backend", backend)

    app = FastAPI()
    app.include_router(ledger_router)
    app.dependency_overrides[get_current_user] = _user

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        forbidden_response = await client.post(
            "/v1/ledger",
            json={
                "provider": "openai",
                "model": "gpt-4o",
                "task_kind": "code",
                "tokens_in": 1200,
                "tokens_out": 340,
                "tokens_reasoning": 0,
                "latency_ms": 1240,
                "outcome": "ok",
                "caller_subsystem": "pantheon",
                "tier": "standard",
            },
        )
    assert forbidden_response.status_code == 403
    assert backend.records == []

    app.dependency_overrides[get_current_user] = _root
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/v1/ledger",
            json={
                "provider": "openai",
                "model": "gpt-4o",
                "task_kind": "code",
                "tokens_in": 1200,
                "tokens_out": 340,
                "tokens_reasoning": 0,
                "latency_ms": 1240,
                "outcome": "ok",
                "caller_subsystem": "pantheon",
                "tier": "standard",
            },
        )
        invalid_response = await client.post(
            "/v1/ledger",
            json={
                "provider": "openai",
                "model": "gpt-4o",
                "task_kind": "code",
                "tokens_in": -1,
                "tokens_out": 340,
                "latency_ms": 1240,
                "outcome": "bad",
                "caller_subsystem": "pantheon",
                "tier": "standard",
            },
        )

    assert response.status_code == 200
    assert response.json()["id"] == 7
    assert Decimal(str(response.json()["est_cost_usd"])) == Decimal("0.000324")
    assert len(backend.records) == 1
    recorded = backend.records[0]
    assert recorded.provider == "openai"
    assert recorded.model == "gpt-4o"
    assert recorded.session_id == "user:root"
    assert recorded.request_count == 1
    assert recorded.path_kind == "api"
    assert recorded.plan_window_id is not None
    assert recorded.plan_window_id.startswith("openai-standard-")
    assert invalid_response.status_code == 422


@pytest.mark.asyncio
async def test_ledger_endpoint_infers_known_subscription_path_kind(monkeypatch):
    backend = _EndpointBackend()
    monkeypatch.setattr(lifecycle, "_persistence_backend", backend)

    app = FastAPI()
    app.include_router(ledger_router)
    app.dependency_overrides[get_current_user] = _root

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/v1/ledger",
            json={
                "provider": "openai",
                "model": "gpt-5.5",
                "task_kind": "code",
                "tokens_in": 1200,
                "tokens_out": 340,
                "tokens_reasoning": 0,
                "latency_ms": 1240,
                "outcome": "ok",
                "caller_subsystem": "pantheon",
                "tier": "chatgpt_plus",
            },
        )

    assert response.status_code == 200
    assert len(backend.records) == 1
    recorded = backend.records[0]
    assert recorded.path_kind == "interactive"
    assert recorded.plan_window_id is not None
    assert recorded.plan_window_id.startswith("openai-chatgpt_plus-")


@pytest.mark.asyncio
async def test_audit_session_usage_is_enforced_against_authenticated_principal(
    monkeypatch,
):
    backend = _EndpointBackend()
    monkeypatch.setattr(lifecycle, "_persistence_backend", backend)
    monkeypatch.setattr(get_settings().knemon, "session_burn_requests_per_hour", 10)

    app = FastAPI()
    app.include_router(ledger_router)
    app.dependency_overrides[get_current_user] = _root
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/v1/ledger",
            json={
                "provider": "openai",
                "model": "gpt-4o",
                "task_kind": "code",
                "tokens_in": 1,
                "tokens_out": 1,
                "latency_ms": 1,
                "outcome": "ok",
                "caller_subsystem": "stiphos",
                "tier": "standard",
                "session_id": "stiphos-job:123",
                "request_count": 10,
            },
        )

    assert response.status_code == 200
    assert backend.records[0].session_id == "stiphos-job:123"
    mapped_usage = backend.conn.execute(
        """
        SELECT SUM(ledger.request_count)
        FROM usage_ledger ledger
        JOIN knemon_ledger_principals principals ON principals.ledger_id = ledger.id
        WHERE principals.principal_id = 'user:root'
        """
    ).fetchone()[0]
    assert mapped_usage == 10
    reservation = await _reserve_session_burn(backend, "user:root")
    assert reservation.burned is True


@pytest.mark.asyncio
async def test_ledger_endpoint_uses_active_plan_window_configuration(monkeypatch):
    backend = _EndpointBackend()
    monkeypatch.setattr(lifecycle, "_persistence_backend", backend)

    async def plans(_backend):
        return {
            "openai": [
                {
                    "provider": "openai",
                    "plan_name": "chatgpt_plus",
                    "reset_anchor": "rolling",
                    "msg_window_seconds": 10800,
                }
            ]
        }

    captured = {}

    def compute(provider, tier, *args, **kwargs):
        captured.update(kwargs)
        return "configured-window"

    monkeypatch.setattr(ledger_mod, "_plans_by_provider", plans)
    monkeypatch.setattr(ledger_mod, "compute_plan_window_id", compute)

    app = FastAPI()
    app.include_router(ledger_router)
    app.dependency_overrides[get_current_user] = _root
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/v1/ledger",
            json={
                "provider": "openai",
                "model": "gpt-5.5",
                "task_kind": "code",
                "tokens_in": 1,
                "tokens_out": 1,
                "latency_ms": 1,
                "outcome": "ok",
                "caller_subsystem": "pantheon",
                "tier": "chatgpt_plus",
            },
        )

    assert response.status_code == 200
    assert captured == {"reset_anchor": "rolling", "window_seconds": 10800}
    assert backend.records[0].plan_window_id == "configured-window"


@pytest.mark.asyncio
async def test_ledger_endpoint_reconciles_explicit_route_reservation(monkeypatch):
    backend = _EndpointBackend()
    reservation_id = "2f1feabc-3d2f-4ccd-81c8-2dca1e5d81e4"
    backend.conn.execute(
        "INSERT INTO knemon_burn_sessions VALUES (?, CURRENT_TIMESTAMP)",
        ("user:root",),
    )
    backend.conn.execute(
        """
        INSERT INTO knemon_burn_reservations (
          reservation_id, session_id, reserved_at, expires_at, request_count, status
        ) VALUES (?, 'user:root', CURRENT_TIMESTAMP, '2999-01-01 00:00:00', 1, 'pending')
        """,
        (reservation_id,),
    )
    backend.conn.commit()
    monkeypatch.setattr(lifecycle, "_persistence_backend", backend)

    app = FastAPI()
    app.include_router(ledger_router)
    app.dependency_overrides[get_current_user] = _root
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/v1/ledger",
            json={
                "provider": "openai",
                "model": "gpt-4o",
                "task_kind": "code",
                "tokens_in": 1,
                "tokens_out": 1,
                "latency_ms": 1,
                "outcome": "ok",
                "caller_subsystem": "pantheon",
                "tier": "standard",
                "session_id": "client-rotated-label",
                "reservation_id": reservation_id,
            },
        )

    assert response.status_code == 200
    assert response.json()["reconciled_reservation_ids"] == [reservation_id]
    assert backend.records[0].session_id == "client-rotated-label"
    status, ledger_id = backend.conn.execute(
        "SELECT status, ledger_id FROM knemon_burn_reservations WHERE reservation_id = ?",
        (reservation_id,),
    ).fetchone()
    assert (status, ledger_id) == ("reconciled", 7)
    principal = backend.conn.execute(
        "SELECT principal_id FROM knemon_ledger_principals WHERE ledger_id = 7"
    ).fetchone()
    assert principal == ("user:root",)


@pytest.mark.asyncio
async def test_tokenless_reconciliation_skips_expired_reservation():
    backend = _EndpointBackend()
    backend.conn.execute(
        "INSERT INTO knemon_burn_sessions VALUES ('user:root', CURRENT_TIMESTAMP)"
    )
    backend.conn.executemany(
        """
        INSERT INTO knemon_burn_reservations (
          reservation_id, session_id, reserved_at, expires_at, request_count, status
        ) VALUES (?, 'user:root', ?, ?, 1, 'pending')
        """,
        [
            ("00000000-0000-0000-0000-000000000001", "2026-01-01", "2026-01-02"),
            ("00000000-0000-0000-0000-000000000002", "2026-08-11", "2999-01-01"),
        ],
    )
    backend.conn.commit()

    async with backend.transactional() as tx:
        reconciled = await reconcile_burn_reservation(
            tx,
            reservation_id=None,
            session_id="user:root",
            ledger_id=7,
            request_count=1,
        )

    assert reconciled == ["00000000-0000-0000-0000-000000000002"]
    statuses = backend.conn.execute(
        "SELECT reservation_id, status FROM knemon_burn_reservations ORDER BY reservation_id"
    ).fetchall()
    assert statuses == [
        ("00000000-0000-0000-0000-000000000001", "pending"),
        ("00000000-0000-0000-0000-000000000002", "reconciled"),
    ]


@pytest.mark.asyncio
async def test_ledger_endpoint_migrates_missing_reservation_schema(monkeypatch):
    backend = _EndpointBackend()
    backend.conn.execute("DROP TABLE knemon_burn_reservations")
    backend.conn.commit()
    monkeypatch.setattr(lifecycle, "_persistence_backend", backend)

    app = FastAPI()
    app.include_router(ledger_router)
    app.dependency_overrides[get_current_user] = _root
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/v1/ledger",
            json={
                "provider": "openai",
                "model": "gpt-4o",
                "task_kind": "code",
                "tokens_in": 1,
                "tokens_out": 1,
                "latency_ms": 1,
                "outcome": "ok",
                "caller_subsystem": "pantheon",
                "tier": "standard",
            },
        )

    assert response.status_code == 200
    assert (
        backend.conn.execute("SELECT COUNT(*) FROM knemon_burn_sessions").fetchone()[0]
        == 1
    )


def test_burn_reservation_migrations_cover_all_supported_backends():
    root = files("mnemos.domain.knemon.db_migrations")
    migrations = [
        root / "migrations/0052_knemon_burn_reservations.sql",
        root / "migrations_sqlite/0052_knemon_burn_reservations.sql",
        root / "migrations_mariadb/0052_knemon_burn_reservations.sql",
        root / "migrations_oracle/0052_knemon_burn_reservations.sql",
        root / "migrations_db2/0052_knemon_burn_reservations.sql",
    ]

    for migration in migrations:
        sql = migration.read_text(encoding="utf-8").lower()
        assert "knemon_burn_sessions" in sql
        assert "knemon_burn_reservations" in sql
        assert "pending" in sql and "reconciled" in sql and "released" in sql
        assert "retention" in sql


def test_usage_ledger_migrations_preserve_constraint_parity():
    migrations = files("mnemos.db_migrations")
    pg = (migrations / "migrations/0032_usage_ledger.sql").read_text()
    oracle = (migrations / "migrations_oracle/0032_usage_ledger.sql").read_text()
    db2 = (migrations / "migrations_db2/0032_usage_ledger.sql").read_text()

    for sql in (pg, oracle, db2):
        normalized = " ".join(sql.lower().split())
        assert "tokens_reasoning" in normalized and "not null" in normalized
        assert (
            "est_cost_usd" in normalized and "check (est_cost_usd >= 0)" in normalized
        )
        assert "check (outcome in ('ok','err','timeout'))" in normalized
        assert "check (latency_ms >= 0)" in normalized


def test_plan_window_hints_preserve_expired_claude_rows_for_historical_ledgers():
    assert _PLAN_WINDOWS[("anthropic", "claude_max_interactive_post_jun15")] == (
        "rolling",
        18000,
    )
    assert _PLAN_WINDOWS[("anthropic", "agent_sdk_credit_pool_post_jun15")] == (
        "monthly",
        None,
    )
