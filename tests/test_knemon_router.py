from __future__ import annotations

import asyncio
import json
import sqlite3
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone

import mnemos.domain.knemon.router as knemon_router
import pytest
from mnemos.api.dependencies import UserContext
from mnemos.api.routes import knemon_router as knemon_router_api
from mnemos.core import config as config_mod
from mnemos.core.config import get_settings
from mnemos.core.plan_windows import compute_plan_window_id
from mnemos.domain.knemon.router import (
    _LAST_RESERVATION_CLEANUP,
    _SESSION_LOCKS,
    KnemonRouteRequest,
    NoModelAvailable,
    _apply_priority_ceiling,
    _best_plan,
    _cleanup_burn_reservations,
    _fallback_bucket,
    _plan_is_effective,
    _plans_by_provider,
    _reserve_session_burn,
    _rows,
    _subscription_pool_aliases,
    _worker_pools_for_session,
    route,
)


class _SqliteKnemonBackend:
    def __init__(
        self,
        utilization_pct: int,
        *,
        burned_session: str | None = None,
        burned_request_count: int = 11,
        burned_age_seconds: int = 0,
        agent_session_id: str | None = None,
        subscription_pools: list[str] | None = None,
        extra_subscription_utilization_pct: int | None = None,
    ) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.active = 0
        self.max_active = 0
        self._create_schema()
        self._seed(
            utilization_pct,
            burned_session=burned_session,
            burned_request_count=burned_request_count,
            burned_age_seconds=burned_age_seconds,
            agent_session_id=agent_session_id,
            subscription_pools=subscription_pools,
            extra_subscription_utilization_pct=extra_subscription_utilization_pct,
        )

    def _create_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE model_registry (
              provider TEXT NOT NULL,
              model_id TEXT NOT NULL,
              display_name TEXT,
              capabilities TEXT NOT NULL DEFAULT '[]',
              input_cost_per_mtok REAL NOT NULL DEFAULT 0,
              output_cost_per_mtok REAL NOT NULL DEFAULT 0,
              context_window INTEGER,
              arena_score REAL,
              graeae_weight REAL NOT NULL DEFAULT 0,
              available INTEGER NOT NULL DEFAULT 1,
              deprecated INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE subscription_plans (
              provider TEXT NOT NULL,
              plan_name TEXT NOT NULL,
              auth_method TEXT NOT NULL,
              path_kind TEXT NOT NULL DEFAULT 'api',
              monthly_usd NUMERIC,
              msg_cap NUMERIC,
              msg_window_seconds NUMERIC,
              token_cap NUMERIC,
              token_window_seconds NUMERIC,
              reset_anchor TEXT,
              overage_pricing_per_mtok_in NUMERIC,
              overage_pricing_per_mtok_out NUMERIC,
              effective_from DATE NOT NULL DEFAULT '2026-01-01',
              effective_until DATE,
              parent_plan_id TEXT
            );
            CREATE TABLE usage_ledger (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TIMESTAMP NOT NULL,
              provider TEXT NOT NULL,
              model TEXT NOT NULL,
              task_kind TEXT NOT NULL,
              tokens_in INTEGER NOT NULL,
              tokens_out INTEGER NOT NULL,
              tokens_reasoning INTEGER NOT NULL,
              est_cost_usd NUMERIC NOT NULL,
              latency_ms INTEGER NOT NULL,
              outcome TEXT NOT NULL,
              caller_subsystem TEXT NOT NULL,
              tier TEXT NOT NULL,
              session_id TEXT,
              request_count NUMERIC NOT NULL DEFAULT 1,
              plan_window_id TEXT,
              path_kind TEXT NOT NULL DEFAULT 'api',
              subscription_amortized INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE agents (
              urn TEXT PRIMARY KEY,
              kind TEXT NOT NULL,
              host TEXT NOT NULL,
              session_id TEXT NOT NULL,
              last_heartbeat REAL NOT NULL,
              status TEXT NOT NULL,
              subscription_pools TEXT
            );
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
            """
        )

    def _seed(
        self,
        utilization_pct: int,
        *,
        burned_session: str | None,
        burned_request_count: int,
        burned_age_seconds: int,
        agent_session_id: str | None,
        subscription_pools: list[str] | None,
        extra_subscription_utilization_pct: int | None,
    ) -> None:
        now = datetime.now(timezone.utc)
        window_id = compute_plan_window_id(
            "openai",
            "chatgpt_plus",
            now,
            reset_anchor="rolling",
            window_seconds=18000,
        )
        msg_cap = 160
        used = round(msg_cap * utilization_pct / 100)
        self.conn.executescript(
            """
            INSERT INTO model_registry VALUES
              ('openai', 'gpt-sub', 'GPT Sub', '["chat","code"]', 2, 8, 200000, 1400, 0.95, 1, 0),
              ('openai', 'gpt-c-tier', 'GPT C Tier', '["vision"]', 2, 8, 200000, 900, 0.70, 1, 0),
              ('nvidia', 'ngc-free', 'NGC Free', '["chat","code"]', 0, 0, 200000, 1250, 0.88, 1, 0),
              ('xai', 'grok-api', 'Grok API', '["chat","code"]', 3, 15, 200000, 1220, 0.86, 1, 0);
            INSERT INTO subscription_plans VALUES
              ('openai', 'chatgpt_plus', 'subscription', 'interactive', 20, 160, 18000,
               NULL, NULL, 'rolling', NULL, NULL, '2026-01-01', NULL, NULL),
              ('nvidia', 'ngc_inference', 'free', 'free', 0, NULL, NULL,
               NULL, NULL, 'monthly', 0, 0, '2026-01-01', NULL, NULL),
              ('xai', 'api', 'api', 'api', NULL, NULL, NULL,
               NULL, NULL, 'monthly', NULL, NULL, '2026-01-01', NULL, NULL);
            """
        )
        self.conn.execute(
            """
            INSERT INTO usage_ledger (
              ts, provider, model, task_kind, tokens_in, tokens_out, tokens_reasoning,
              est_cost_usd, latency_ms, outcome, caller_subsystem, tier, session_id,
              request_count, plan_window_id, path_kind, subscription_amortized
            ) VALUES (?, 'openai', 'gpt-sub', 'chat', 100, 50, 0, 0, 100, 'ok',
                      'test', 'chatgpt_plus', 'usage-fixture', ?, ?, 'api', 1)
            """,
            (now, used, window_id),
        )
        if burned_session:
            burn_ts = now - timedelta(seconds=burned_age_seconds)
            self.conn.execute(
                """
                INSERT INTO usage_ledger (
                  ts, provider, model, task_kind, tokens_in, tokens_out, tokens_reasoning,
                  est_cost_usd, latency_ms, outcome, caller_subsystem, tier, session_id,
                  request_count, plan_window_id, path_kind, subscription_amortized
                ) VALUES (?, 'xai', 'grok-api', 'chat', 100, 50, 0, 0, 100, 'ok',
                          'test', 'api', ?, ?, ?, 'api', 0)
                """,
                (burn_ts, burned_session, burned_request_count, window_id),
            )
        if extra_subscription_utilization_pct is not None:
            extra_window_id = compute_plan_window_id(
                "anthropic",
                "claude_max_200",
                now,
                reset_anchor="rolling",
                window_seconds=18000,
            )
            extra_used = round(40 * extra_subscription_utilization_pct / 100)
            self.conn.execute(
                """
                INSERT INTO model_registry VALUES
                  ('anthropic', 'claude-sub', 'Claude Sub', '["chat","code"]', 3, 15,
                   200000, 1300, 0.87, 1, 0)
                """
            )
            self.conn.execute(
                """
                INSERT INTO subscription_plans VALUES
                  ('anthropic', 'claude_max_200', 'subscription', 'interactive', 200, 40, 18000,
                   NULL, NULL, 'rolling', NULL, NULL, '2026-01-01', NULL, NULL)
                """
            )
            self.conn.execute(
                """
                INSERT INTO usage_ledger (
                  ts, provider, model, task_kind, tokens_in, tokens_out, tokens_reasoning,
                  est_cost_usd, latency_ms, outcome, caller_subsystem, tier, session_id,
                  request_count, plan_window_id, path_kind, subscription_amortized
                ) VALUES (?, 'anthropic', 'claude-sub', 'chat', 100, 50, 0, 0, 100, 'ok',
                          'test', 'claude_max_200', 'usage-fixture', ?, ?, 'api', 1)
                """,
                (now, extra_used, extra_window_id),
            )
        if agent_session_id or burned_session:
            effective_agent_session = agent_session_id or burned_session
            effective_pools = (
                subscription_pools if agent_session_id else ["chatgpt_plus"]
            )
            self.conn.execute(
                """
                INSERT INTO agents (
                  urn, kind, host, session_id, last_heartbeat, status, subscription_pools
                ) VALUES ('urn:agent:test:1', 'codex', 'pytest', ?, ?, 'online', ?)
                """,
                (
                    effective_agent_session,
                    now.timestamp(),
                    json.dumps(effective_pools or []),
                ),
            )
        self.conn.commit()

    @asynccontextmanager
    async def transactional(self):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        await asyncio.sleep(0)
        try:
            yield self.conn
        finally:
            self.active -= 1


def _req(priority: int, session_id: str | None = None) -> KnemonRouteRequest:
    return KnemonRouteRequest(
        task_kind="code-fix",
        priority=priority,
        est_tokens_in=10_000,
        est_tokens_out=2_000,
        caller_session_id=session_id,
        caller_subsystem="pytest",
        require_capability=["code"],
    )


@pytest.mark.asyncio
async def test_single_est_tokens_request_shape_supported():
    req = KnemonRouteRequest(
        task_kind="code-fix",
        priority=14,
        est_tokens=12_000,
        caller_subsystem="pytest",
        require_capability=["code"],
    )
    decision = await route(req, _SqliteKnemonBackend(40))
    assert decision.provider == "openai"
    assert decision.plan_name == "chatgpt_plus"
    assert decision.auth_method == "subscription"


@pytest.mark.asyncio
async def test_decision_exposes_exact_selected_leaf_plan_name():
    backend = _SqliteKnemonBackend(
        40,
        agent_session_id="leaf-plan-worker",
        subscription_pools=["codex_pro_200"],
    )
    backend.conn.execute(
        """
        UPDATE subscription_plans
        SET plan_name = 'codex_pro_200_20x', parent_plan_id = 'codex_pro_200'
        WHERE provider = 'openai'
        """
    )
    backend.conn.commit()

    decision = await route(_req(14, "leaf-plan-worker"), backend)

    assert decision.plan_name == "codex_pro_200_20x"


@pytest.mark.parametrize("utilization_pct", [40, 70, 92, 100])
@pytest.mark.asyncio
async def test_synthetic_utilization_fixtures_route(utilization_pct):
    decision = await route(_req(14), _SqliteKnemonBackend(utilization_pct))
    assert decision.provider in {"openai", "xai"}


@pytest.mark.asyncio
async def test_low_priority_92_pct_subscription_routes_to_free_ngc():
    decision = await route(_req(5), _SqliteKnemonBackend(92))
    assert decision.provider == "nvidia"
    assert decision.model_id == "ngc-free"
    assert decision.auth_method == "free"


@pytest.mark.asyncio
async def test_high_priority_92_pct_subscription_still_routes_to_subscription():
    decision = await route(_req(14), _SqliteKnemonBackend(92))
    assert decision.provider == "openai"
    assert decision.auth_method == "subscription"
    assert decision.sub_window_utilization_pct > 90


@pytest.mark.asyncio
async def test_g1_at_100_pct_subscription_routes_to_api_fallback():
    decision = await route(_req(14), _SqliteKnemonBackend(100))
    assert decision.provider == "xai"
    assert decision.auth_method == "api"


@pytest.mark.asyncio
async def test_fallback_chain_uses_actual_utilization_for_unvisited_subscription():
    backend = _SqliteKnemonBackend(92, extra_subscription_utilization_pct=92)
    decision = await route(_req(5), backend)
    assert decision.provider == "nvidia"
    assert decision.fallback_chain[0][:2] == ("xai", "grok-api")


def test_fallback_bucket_boundaries():
    assert _fallback_bucket({"auth_method": "free"}) == 0
    assert (
        _fallback_bucket(
            {"auth_method": "subscription", "sub_window_utilization_pct": 69.99}
        )
        == 1
    )
    assert (
        _fallback_bucket(
            {"auth_method": "subscription", "sub_window_utilization_pct": 70.0}
        )
        == 4
    )
    assert _fallback_bucket({"auth_method": "api", "estimated_cost_usd": 0.499999}) == 2
    assert _fallback_bucket({"auth_method": "api", "estimated_cost_usd": 0.50}) == 3


def test_g1_quality_floor_survives_burn_downgrade():
    candidates = [
        {"quality": 0.80, "tier": "B", "graeae_weight": 0.99},
        {"quality": 0.86, "tier": "A", "graeae_weight": 0.80},
    ]

    assert _apply_priority_ceiling(candidates, 13, requested_priority=14) == [
        candidates[1]
    ]


def test_best_plan_prefers_matching_workspace_pool_over_highest_price():
    plans = {
        "openai": [
            {
                "provider": "openai",
                "plan_name": "chatgpt_pro_200_codex",
                "parent_plan_id": "chatgpt_pro",
                "auth_method": "subscription",
                "monthly_usd": 200,
                "msg_cap": 300,
            },
            {
                "provider": "openai",
                "plan_name": "chatgpt_plus",
                "auth_method": "subscription",
                "monthly_usd": 20,
                "msg_cap": 15,
            },
        ]
    }

    assert _best_plan(plans, "openai", {"chatgpt_plus"})["plan_name"] == "chatgpt_plus"
    assert (
        _best_plan(plans, "openai", {"chatgpt_pro"})["plan_name"]
        == "chatgpt_pro_200_codex"
    )
    assert _best_plan(plans, "openai")["plan_name"] == "chatgpt_pro_200_codex"


def test_plan_effective_dates_filter_expired_and_future_rows():
    today = date(2026, 6, 1)

    assert not _plan_is_effective(
        {"effective_from": "2026-05-01", "effective_until": "2026-05-31"},
        today,
    )
    assert _plan_is_effective(
        {"effective_from": "2026-06-01", "effective_until": None},
        today,
    )
    assert not _plan_is_effective(
        {"effective_from": date(2026, 6, 2), "effective_until": None},
        today,
    )
    assert _plan_is_effective({"provider": "legacy"}, today)


@pytest.mark.asyncio
async def test_mid_priority_rejects_tier_c_ceiling():
    req = KnemonRouteRequest(
        task_kind="vision",
        priority=10,
        est_tokens_in=10_000,
        est_tokens_out=2_000,
        caller_session_id=None,
        caller_subsystem="pytest",
        require_capability=["vision"],
    )
    with pytest.raises(NoModelAvailable, match="priority tier"):
        await route(req, _SqliteKnemonBackend(40))


@pytest.mark.asyncio
async def test_max_cost_tier_cap_rejects_higher_tier_candidates():
    req = KnemonRouteRequest(
        task_kind="code-fix",
        priority=14,
        est_tokens_in=10_000,
        est_tokens_out=2_000,
        caller_subsystem="zeroclaw",
        require_capability=["code"],
        max_cost_tier="A",
    )

    decision = await route(req, _SqliteKnemonBackend(40))

    assert decision.dispatch_cost_tier == "A"
    assert decision.provider in {"nvidia", "xai"}


@pytest.mark.asyncio
async def test_subscription_requires_caller_workspace_pool():
    session_id = "workspace-without-openai"
    backend = _SqliteKnemonBackend(
        40, agent_session_id=session_id, subscription_pools=["anthropic_subscription"]
    )
    decision = await route(_req(14, session_id), backend)
    assert decision.provider == "xai"
    assert decision.auth_method == "api"
    assert "lacks pool" in decision.reasoning


@pytest.mark.asyncio
async def test_unknown_worker_session_cannot_use_subscription_pool():
    decision = await route(_req(14, "missing-worker"), _SqliteKnemonBackend(40))

    assert decision.provider == "xai"
    assert decision.auth_method == "api"


@pytest.mark.asyncio
async def test_worker_lookup_permission_error_is_not_swallowed():
    class PermissionBackend:
        @asynccontextmanager
        async def transactional(self):
            class Conn:
                def cursor(self):
                    class Cursor:
                        async def execute(self, sql, params):
                            raise RuntimeError("permission denied for table agents")

                        async def close(self):
                            return None

                    return Cursor()

            yield Conn()

    with pytest.raises(RuntimeError, match="permission denied"):
        await _worker_pools_for_session(PermissionBackend(), "worker")


@pytest.mark.asyncio
async def test_mysql_rows_convert_named_parameters_to_positional():
    class MySQLCursor:
        description = [("provider",)]

        def __init__(self):
            self.executed = None

        async def execute(self, sql, params):
            self.executed = (sql, params)

        async def fetchall(self):
            return [("openai",)]

        async def close(self):
            return None

    class MySQLConnection:
        def __init__(self):
            self.cursor_obj = MySQLCursor()

        def cursor(self):
            return self.cursor_obj

    class Backend:
        def __init__(self):
            self.conn = MySQLConnection()

        @asynccontextmanager
        async def transactional(self):
            yield self.conn

    backend = Backend()
    rows = await _rows(
        backend,
        "SELECT provider FROM model_registry WHERE provider = :provider OR model_id = :provider",
        {"provider": "openai"},
    )

    assert rows == [{"provider": "openai"}]
    assert backend.conn.cursor_obj.executed == (
        "SELECT provider FROM model_registry WHERE provider = %s OR model_id = %s",
        ("openai", "openai"),
    )


@pytest.mark.asyncio
async def test_subscription_allows_matching_caller_workspace_pool():
    session_id = "workspace-with-openai"
    backend = _SqliteKnemonBackend(
        40, agent_session_id=session_id, subscription_pools=["chatgpt_plus"]
    )
    decision = await route(_req(14, session_id), backend)
    assert decision.provider == "openai"
    assert decision.auth_method == "subscription"


@pytest.mark.asyncio
async def test_subscription_plan_selection_respects_exact_workspace_pool():
    session_id = "workspace-with-plus-only"
    backend = _SqliteKnemonBackend(
        40, agent_session_id=session_id, subscription_pools=["chatgpt_plus"]
    )
    backend.conn.execute(
        """
        INSERT INTO subscription_plans VALUES
          ('openai', 'chatgpt_pro', 'subscription', 'unmetered', 200, 200, 18000,
           NULL, NULL, 'rolling', NULL, NULL, '2026-01-01', NULL, NULL)
        """
    )
    backend.conn.commit()

    decision = await route(_req(14, session_id), backend)
    assert decision.provider == "openai"
    assert decision.auth_method == "subscription"
    assert "selected subscription under" in decision.reasoning


@pytest.mark.asyncio
async def test_openai_no_workspace_pool_uses_chatgpt_family_for_generic_gpt_model():
    backend = _SqliteKnemonBackend(40)
    backend.conn.execute(
        """
        INSERT INTO subscription_plans VALUES
          ('openai', 'codex_pro_200_20x', 'subscription', 'interactive', 200, 300, 18000,
           NULL, NULL, 'rolling', NULL, NULL, '2026-01-01', NULL, 'codex_pro_200')
        """
    )
    backend.conn.commit()

    decision = await route(_req(14), backend)

    assert decision.provider == "openai"
    assert decision.auth_method == "subscription"
    assert decision.sub_window_utilization_pct == 40.0


@pytest.mark.asyncio
async def test_parallel_same_session_burn_tracking_serializes():
    backend = _SqliteKnemonBackend(92, burned_session="burned-session")
    decisions = await asyncio.gather(
        *(route(_req(14, "burned-session"), backend) for _ in range(10))
    )
    assert {decision.provider for decision in decisions} == {"xai"}
    assert backend.max_active == 1
    assert _SESSION_LOCKS == {}


@pytest.mark.asyncio
async def test_route_memoizes_subscription_usage_per_plan_for_fallbacks(monkeypatch):
    backend = _SqliteKnemonBackend(40)
    backend.conn.execute(
        """
        INSERT INTO model_registry VALUES
          ('openai', 'gpt-sub-alt', 'GPT Sub Alt', '["chat","code"]',
           2, 8, 200000, 1300, 0.90, 1, 0)
        """
    )
    backend.conn.commit()
    calls = 0
    original = knemon_router._usage_for_plan

    async def counted_usage(*args, **kwargs):
        nonlocal calls
        calls += 1
        return await original(*args, **kwargs)

    monkeypatch.setattr(knemon_router, "_usage_for_plan", counted_usage)

    decision = await route(_req(14), backend)

    assert decision.provider == "openai"
    assert calls == 1


@pytest.mark.asyncio
async def test_pending_route_reservation_closes_burn_threshold_race():
    session_id = "reservation-boundary"
    backend = _SqliteKnemonBackend(
        92, burned_session=session_id, burned_request_count=9
    )

    decisions = await asyncio.gather(
        *(route(_req(14, session_id), backend) for _ in range(10))
    )

    assert [decision.provider for decision in decisions].count("openai") == 1
    assert [decision.provider for decision in decisions].count("xai") == 9
    assert all(decision.reservation_id for decision in decisions)
    pending = backend.conn.execute(
        """
        SELECT COUNT(*) FROM knemon_burn_reservations
        WHERE session_id = ? AND status = 'pending'
        """,
        (session_id,),
    ).fetchone()[0]
    assert pending == 10


@pytest.mark.asyncio
async def test_reservations_are_shared_across_backend_instances(tmp_path):
    database_path = tmp_path / "burn-reservations.sqlite3"
    setup = sqlite3.connect(database_path)
    setup.executescript(
        """
            CREATE TABLE usage_ledger (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TIMESTAMP NOT NULL,
              session_id TEXT,
              caller_subsystem TEXT,
              request_count INTEGER NOT NULL
        );
        CREATE TABLE knemon_burn_sessions (
          session_id TEXT PRIMARY KEY,
          touched_at TIMESTAMP NOT NULL
        );
        CREATE TABLE knemon_burn_reservations (
          reservation_id TEXT PRIMARY KEY,
          session_id TEXT NOT NULL,
          reserved_at TIMESTAMP NOT NULL,
          expires_at TIMESTAMP NOT NULL,
          request_count INTEGER NOT NULL,
          status TEXT NOT NULL,
          reconciled_at TIMESTAMP,
          ledger_id INTEGER
        );
        """
    )
    setup.execute(
        "INSERT INTO usage_ledger (ts, session_id, request_count) VALUES (?, 'shared-session', 9)",
        (datetime.now(timezone.utc),),
    )
    setup.commit()
    setup.close()

    class SharedSqliteBackend:
        @asynccontextmanager
        async def transactional(self):
            conn = sqlite3.connect(database_path, timeout=5)
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except BaseException:
                conn.rollback()
                raise
            else:
                conn.commit()
            finally:
                conn.close()

    first = await _reserve_session_burn(SharedSqliteBackend(), "shared-session")
    second = await _reserve_session_burn(SharedSqliteBackend(), "shared-session")

    assert first.burned is False
    assert second.burned is True
    assert first.reservation_id != second.reservation_id


@pytest.mark.asyncio
async def test_failed_route_releases_its_reservation():
    session_id = "no-model-release"
    backend = _SqliteKnemonBackend(
        40, agent_session_id=session_id, subscription_pools=["chatgpt_plus"]
    )
    backend.conn.execute("DELETE FROM model_registry")
    backend.conn.commit()

    with pytest.raises(NoModelAvailable):
        await route(_req(14, session_id), backend)

    statuses = backend.conn.execute(
        "SELECT status FROM knemon_burn_reservations WHERE session_id = ?",
        (session_id,),
    ).fetchall()
    assert statuses == [("released",)]


@pytest.mark.asyncio
async def test_missing_reservation_schema_is_migrated_before_default_route():
    backend = _SqliteKnemonBackend(40)
    usage_rows_before = backend.conn.execute(
        "SELECT COUNT(*) FROM usage_ledger"
    ).fetchone()[0]
    backend.conn.execute("DROP TABLE knemon_burn_reservations")
    backend.conn.commit()

    decision = await route(_req(14, "migration-required"), backend)

    assert decision.reservation_id is not None
    assert (
        backend.conn.execute(
            "SELECT COUNT(*) FROM knemon_burn_reservations"
        ).fetchone()[0]
        == 1
    )
    assert (
        backend.conn.execute("SELECT COUNT(*) FROM usage_ledger").fetchone()[0]
        == usage_rows_before
    )
    indexes = {
        row[1]
        for row in backend.conn.execute("PRAGMA index_list(knemon_burn_reservations)")
    }
    assert "idx_knemon_burn_reservations_retention" in indexes


@pytest.mark.asyncio
async def test_reservation_cleanup_removes_only_rows_past_retention(monkeypatch):
    backend = _SqliteKnemonBackend(40)
    now = datetime.now(timezone.utc)
    old = now - timedelta(days=8)
    recent = now - timedelta(days=1)
    backend.conn.execute(
        "INSERT OR IGNORE INTO knemon_burn_sessions VALUES ('old', ?)", (old,)
    )
    backend.conn.execute(
        "INSERT OR IGNORE INTO knemon_burn_sessions VALUES ('recent', ?)", (recent,)
    )
    backend.conn.executemany(
        """
        INSERT INTO knemon_burn_reservations (
          reservation_id, session_id, reserved_at, expires_at, request_count,
          status, reconciled_at, ledger_id
        ) VALUES (?, ?, ?, ?, 1, 'reconciled', ?, 1)
        """,
        [
            ("old-reservation", "old", old, old, old),
            ("recent-reservation", "recent", recent, recent, recent),
        ],
    )
    backend.conn.commit()
    _LAST_RESERVATION_CLEANUP.pop(id(backend), None)
    monkeypatch.setenv(
        "MNEMOS_KNEMON_BURN_RESERVATION_RETENTION_SECONDS", str(7 * 86400)
    )

    await _cleanup_burn_reservations(backend, now=now)

    assert backend.conn.execute(
        "SELECT reservation_id FROM knemon_burn_reservations ORDER BY reservation_id"
    ).fetchall() == [("recent-reservation",)]


@pytest.mark.asyncio
async def test_api_session_rotation_does_not_rotate_burn_principal(monkeypatch):
    backend = _SqliteKnemonBackend(
        92, burned_session="user:root", burned_request_count=10
    )
    now = datetime.now(timezone.utc).timestamp()
    backend.conn.executemany(
        """
        INSERT INTO agents (urn, kind, host, session_id, last_heartbeat, status, subscription_pools)
        VALUES (?, 'codex', 'pytest', ?, ?, 'online', '["chatgpt_plus"]')
        """,
        [("urn:agent:r11", "r11", now), ("urn:agent:r12", "r12", now)],
    )
    backend.conn.commit()
    monkeypatch.setattr(knemon_router_api, "backend_or_503", lambda: backend)
    root = UserContext(
        user_id="root",
        group_ids=[],
        role="root",
        namespace="default",
        authenticated=True,
    )

    decisions = []
    for caller_session_id in ("r11", "r12"):
        decisions.append(
            await knemon_router_api.route_knemon(
                task_kind="code-fix",
                priority=14,
                est_tokens_in=10_000,
                est_tokens_out=2_000,
                caller_session_id=caller_session_id,
                caller_subsystem="api",
                exclude_providers=None,
                require_capability="code",
                max_cost_tier=None,
                user=root,
            )
        )

    assert {decision["provider"] for decision in decisions} == {"xai"}
    principals = backend.conn.execute(
        "SELECT DISTINCT session_id FROM knemon_burn_reservations"
    ).fetchall()
    assert principals == [("user:root",)]


@pytest.mark.asyncio
async def test_missing_reservation_schema_has_explicit_development_opt_out(monkeypatch):
    backend = _SqliteKnemonBackend(40)
    backend.conn.execute("DROP TABLE knemon_burn_reservations")
    backend.conn.commit()
    monkeypatch.setenv("MNEMOS_KNEMON_ALLOW_NONATOMIC_BURN_DEVELOPMENT", "true")

    decision = await route(_req(14, "development-only"), backend)

    assert decision.reservation_id is None


@pytest.mark.asyncio
async def test_burned_g1_session_loses_near_cap_subscription_lane():
    session_id = "burned-near-cap"
    backend = _SqliteKnemonBackend(
        70, burned_session=session_id, burned_request_count=10
    )
    decision = await route(_req(14, session_id), backend)

    assert decision.provider == "xai"
    assert decision.auth_method == "api"
    assert "skipped subscription at 70.00% utilization" in decision.reasoning


@pytest.mark.parametrize(
    ("request_count", "expected_provider"),
    [
        (9, "openai"),
        (10, "xai"),
    ],
)
@pytest.mark.asyncio
async def test_session_burn_threshold_defaults_to_ten_requests(
    request_count, expected_provider
):
    session_id = f"burn-boundary-{request_count}"
    backend = _SqliteKnemonBackend(
        92, burned_session=session_id, burned_request_count=request_count
    )
    decision = await route(_req(14, session_id), backend)
    assert decision.provider == expected_provider


@pytest.mark.asyncio
async def test_session_burn_ignores_requests_outside_rolling_window():
    session_id = "burn-window-expired"
    backend = _SqliteKnemonBackend(
        92,
        burned_session=session_id,
        burned_request_count=10,
        burned_age_seconds=get_settings().knemon.session_burn_window_seconds + 1,
    )
    decision = await route(_req(14, session_id), backend)
    assert decision.provider == "openai"


@pytest.mark.asyncio
async def test_session_burn_threshold_is_configurable(monkeypatch):
    session_id = "burn-configurable"
    backend = _SqliteKnemonBackend(
        92, burned_session=session_id, burned_request_count=10
    )
    monkeypatch.setattr(get_settings().knemon, "session_burn_requests_per_hour", 11)
    decision = await route(_req(14, session_id), backend)
    assert decision.provider == "openai"


@pytest.mark.parametrize(
    ("item", "expected_bucket"),
    [
        ({"auth_method": "free"}, 0),
        ({"auth_method": "subscription", "sub_window_utilization_pct": 69.999}, 1),
        ({"auth_method": "api", "estimated_cost_usd": 0.499999}, 2),
        ({"auth_method": "token", "estimated_cost_usd": 0.499999}, 2),
        ({"auth_method": "api", "estimated_cost_usd": 0.50}, 3),
        ({"auth_method": "subscription", "sub_window_utilization_pct": 70.0}, 4),
        ({"auth_method": "oauth"}, 4),
    ],
)
def test_fallback_bucket_order_and_strict_boundaries(
    monkeypatch, item, expected_bucket
):
    monkeypatch.setattr(
        get_settings().knemon, "low_priority_api_cost_ceiling_usd", 0.50
    )
    monkeypatch.setattr(
        get_settings().knemon, "subscription_preferred_utilization_pct", 70.0
    )
    assert _fallback_bucket(item) == expected_bucket


@pytest.mark.asyncio
async def test_fallback_chain_checks_subscription_utilization_for_unevaluated_candidates():
    backend = _SqliteKnemonBackend(92)
    now = datetime.now(timezone.utc)
    window_id = compute_plan_window_id(
        "anthropic",
        "claude_max_100",
        now,
        reset_anchor="rolling",
        window_seconds=18000,
    )
    backend.conn.execute(
        """
        INSERT INTO model_registry VALUES
          ('anthropic', 'claude-sub', 'Claude Sub', '["chat","code"]',
           1, 5, 200000, 1200, 0.80, 1, 0)
        """
    )
    backend.conn.execute(
        """
        INSERT INTO subscription_plans VALUES
          ('anthropic', 'claude_max_100', 'subscription', 'interactive', 100, 160, 18000,
           NULL, NULL, 'rolling', NULL, NULL, '2026-01-01', NULL, NULL)
        """
    )
    backend.conn.execute(
        """
        INSERT INTO usage_ledger (
          ts, provider, model, task_kind, tokens_in, tokens_out, tokens_reasoning,
          est_cost_usd, latency_ms, outcome, caller_subsystem, tier, session_id,
          request_count, plan_window_id, path_kind, subscription_amortized
        ) VALUES (?, 'anthropic', 'claude-sub', 'chat', 100, 50, 0, 0, 100, 'ok',
                  'test', 'claude_max_100', 'usage-fixture', 147, ?, 'api', 1)
        """,
        (now, window_id),
    )
    backend.conn.commit()

    decision = await route(_req(5), backend)

    assert decision.provider == "nvidia"
    assert decision.fallback_chain[0][0:2] == ("xai", "grok-api")


def test_openai_subscription_aliases_split_chatgpt_and_codex():
    chatgpt_aliases = _subscription_pool_aliases(
        {"provider": "openai", "plan_name": "chatgpt_plus"}
    )
    codex_aliases = _subscription_pool_aliases(
        {"provider": "openai", "plan_name": "codex_plus"}
    )

    assert "openai_subscription" in chatgpt_aliases
    assert "chatgpt_subscription" in chatgpt_aliases
    assert "codex_subscription" not in chatgpt_aliases
    assert "openai_subscription" in codex_aliases
    assert "codex_subscription" in codex_aliases
    assert "chatgpt_subscription" not in codex_aliases


def test_best_plan_honors_chatgpt_and_codex_workspace_pools():
    plans = {
        "openai": [
            {
                "provider": "openai",
                "plan_name": "chatgpt_pro",
                "auth_method": "subscription",
            },
            {
                "provider": "openai",
                "plan_name": "chatgpt_pro_100",
                "auth_method": "subscription",
            },
            {
                "provider": "openai",
                "plan_name": "codex_pro_200_20x",
                "auth_method": "subscription",
                "parent_plan_id": "codex_pro_200",
            },
            {
                "provider": "openai",
                "plan_name": "codex_plus",
                "auth_method": "subscription",
            },
        ]
    }

    assert (
        _best_plan(plans, "openai", {"chatgpt_pro_100"})["plan_name"]
        == "chatgpt_pro_100"
    )
    assert (
        _best_plan(plans, "openai", {"chatgpt_subscription"})["plan_name"]
        == "chatgpt_pro"
    )
    assert (
        _best_plan(plans, "openai", {"codex_subscription"})["plan_name"]
        == "codex_pro_200_20x"
    )
    assert (
        _best_plan(plans, "openai", {"codex_pro_200"})["plan_name"]
        == "codex_pro_200_20x"
    )
    assert _best_plan(plans, "openai", {"codex_plus"})["plan_name"] == "codex_plus"


def test_best_plan_infers_openai_family_without_workspace_pool():
    plans = {
        "openai": [
            {
                "provider": "openai",
                "plan_name": "codex_pro_200_20x",
                "auth_method": "subscription",
            },
            {
                "provider": "openai",
                "plan_name": "chatgpt_pro",
                "auth_method": "subscription",
            },
        ]
    }

    chatgpt_candidate = {
        "provider": "openai",
        "model_id": "gpt-5.5",
        "display_name": "GPT-5.5",
    }
    codex_candidate = {
        "provider": "openai",
        "model_id": "gpt-5.3-codex",
        "display_name": "GPT-5.3 Codex",
    }

    assert (
        _best_plan(plans, "openai", candidate=chatgpt_candidate)["plan_name"]
        == "chatgpt_pro"
    )
    assert (
        _best_plan(plans, "openai", candidate=codex_candidate)["plan_name"]
        == "codex_pro_200_20x"
    )


def test_g1_priority_quality_floor_boundary(monkeypatch):
    monkeypatch.setattr(get_settings().knemon, "g1_quality_floor", 0.85)
    candidates = [
        {
            "provider": "cheap",
            "model_id": "below",
            "quality": 0.8499,
            "tier": "A",
            "graeae_weight": 0.99,
        },
        {
            "provider": "good",
            "model_id": "at-floor",
            "quality": 0.85,
            "tier": "B",
            "graeae_weight": 0.75,
        },
    ]
    assert _apply_priority_ceiling(candidates, 14) == [candidates[1]]


def test_burned_g1_session_keeps_requested_quality_floor(monkeypatch):
    monkeypatch.setattr(get_settings().knemon, "g1_quality_floor", 0.85)
    candidates = [
        {
            "provider": "cheap",
            "model_id": "below",
            "quality": 0.80,
            "tier": "B",
            "graeae_weight": 0.99,
        },
        {
            "provider": "good",
            "model_id": "frontier",
            "quality": 0.90,
            "tier": "A",
            "graeae_weight": 0.75,
        },
    ]
    assert _apply_priority_ceiling(candidates, 13, requested_priority=14) == [
        candidates[1]
    ]


@pytest.mark.parametrize(
    ("as_of", "expected_plan"),
    [
        (date(2026, 5, 31), "claude_max_200"),
        (date(2026, 6, 1), "claude_max_100"),
    ],
)
@pytest.mark.asyncio
async def test_date_aware_plan_selection_honors_tier_flip_date(
    as_of: date, expected_plan: str
):
    backend = _SqliteKnemonBackend(0)
    backend.conn.executescript(
        """
        DROP TABLE subscription_plans;
        CREATE TABLE subscription_plans (
          provider TEXT NOT NULL,
          plan_name TEXT NOT NULL,
          auth_method TEXT NOT NULL,
          path_kind TEXT NOT NULL,
          monthly_usd NUMERIC,
          msg_cap NUMERIC,
          msg_window_seconds NUMERIC,
          token_cap NUMERIC,
          token_window_seconds NUMERIC,
          reset_anchor TEXT,
          overage_pricing_per_mtok_in NUMERIC,
          overage_pricing_per_mtok_out NUMERIC,
          effective_from DATE NOT NULL,
          effective_until DATE,
          parent_plan_id TEXT
        );
        """
    )
    backend.conn.executemany(
        """
        INSERT INTO subscription_plans VALUES
          ('anthropic', ?, 'subscription', 'interactive', ?, ?, 18000, NULL, NULL,
           'rolling', NULL, NULL, ?, ?, NULL)
        """,
        [
            ("claude_max_200", 200, 900, "2026-04-01", "2026-05-31"),
            ("claude_max_100", 100, 225, "2026-06-01", None),
        ],
    )
    backend.conn.commit()

    plans = await _plans_by_provider(backend, as_of=as_of)

    assert [plan["plan_name"] for plan in plans["anthropic"]] == [expected_plan]


@pytest.mark.asyncio
async def test_session_burn_threshold_honors_env_override(monkeypatch):
    monkeypatch.setenv("MNEMOS_KNEMON_SESSION_BURN_REQUESTS_PER_HOUR", "12")
    config_mod._reset_settings_for_tests()
    try:
        below = _SqliteKnemonBackend(
            70, burned_session="custom-threshold-below", burned_request_count=10
        )
        below_decision = await route(_req(14, "custom-threshold-below"), below)
        assert below_decision.provider == "openai"
        assert below_decision.auth_method == "subscription"

        at_threshold = _SqliteKnemonBackend(
            70, burned_session="custom-threshold-at", burned_request_count=12
        )
        at_threshold_decision = await route(
            _req(14, "custom-threshold-at"), at_threshold
        )
        assert at_threshold_decision.provider == "xai"
        assert at_threshold_decision.auth_method == "api"
    finally:
        config_mod._reset_settings_for_tests()


@pytest.mark.asyncio
async def test_session_below_burn_threshold_keeps_g1_subscription_escalation():
    backend = _SqliteKnemonBackend(
        70, burned_session="not-yet-burned", burned_request_count=9
    )
    decision = await route(_req(14, "not-yet-burned"), backend)
    assert decision.provider == "openai"
    assert decision.auth_method == "subscription"


# ── Task-type differentiation fixtures ───────────────────────────────────────


def _diverse_task_backend() -> _SqliteKnemonBackend:
    """Backend with models covering coding / narrative / reasoning / embedding / routing."""
    backend = _SqliteKnemonBackend(40)
    # Clear the default 4-model seed so capability assertions are exact.
    backend.conn.execute("DELETE FROM model_registry")
    backend.conn.execute("DELETE FROM subscription_plans")
    models = [
        # coding
        (
            "nvidia",
            "qwen/qwen3-coder-480b-a35b-instruct",
            "Qwen3 Coder 480B",
            '["chat","code","reasoning"]',
            1.0,
            3.0,
            0.86,
        ),
        (
            "anthropic",
            "claude-sonnet-4-6",
            "Claude Sonnet 4.6",
            '["chat","code","reasoning"]',
            3.0,
            15.0,
            0.94,
        ),
        # narrative (chat)
        (
            "anthropic",
            "claude-opus-4-6",
            "Claude Opus 4.6",
            '["chat","code","reasoning"]',
            15.0,
            75.0,
            0.99,
        ),
        (
            "gemini",
            "gemini-2.5-flash",
            "Gemini 2.5 Flash",
            '["chat"]',
            0.15,
            0.60,
            0.82,
        ),
        # reasoning
        (
            "deepseek-direct",
            "deepseek-v4-pro",
            "DeepSeek V4 Pro",
            '["chat","code","reasoning"]',
            0.435,
            0.87,
            0.85,
        ),
        # embedding
        ("mnemos-local", "bge-m3", "BGE M3", '["embeddings"]', 0.0, 0.0, 0.82),
        ("voyage", "voyage-3", "Voyage 3", '["embeddings"]', 0.0, 0.0, 0.80),
        # routing (small/fast)
        (
            "groq",
            "llama-3.1-8b-instant",
            "Llama 3.1 8B Instant",
            '["chat","routing"]',
            0.05,
            0.08,
            0.78,
        ),
        (
            "nvidia",
            "moonshotai/kimi-k2.6",
            "Kimi K2.6",
            '["chat","routing"]',
            0.10,
            0.15,
            0.79,
        ),
        # generic fallback (api-only, cheap)
        ("xai", "grok-api", "Grok API", '["chat","code"]', 3.0, 15.0, 0.86),
    ]
    for provider, model_id, display_name, caps, in_cost, out_cost, weight in models:
        backend.conn.execute(
            "INSERT INTO model_registry VALUES (?, ?, ?, ?, ?, ?, 200000, 1200, ?, 1, 0)",
            (provider, model_id, display_name, caps, in_cost, out_cost, weight),
        )
    # Give every provider an api plan so waterfall doesn't skip them.
    for provider in {
        "nvidia",
        "anthropic",
        "gemini",
        "deepseek-direct",
        "mnemos-local",
        "voyage",
        "groq",
        "xai",
    }:
        backend.conn.execute(
            "INSERT INTO subscription_plans VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL,"
            " 'monthly', NULL, NULL, '2026-01-01', NULL, NULL)",
            (provider, f"{provider}_api", "api", "api"),
        )
    backend.conn.commit()
    return backend


def _task_req(task_kind: str, priority: int = 14) -> KnemonRouteRequest:
    """Route request with explicit task_kind and NO caller require_capability
    so the recommendation policy is the sole capability driver."""
    return KnemonRouteRequest(
        task_kind=task_kind,
        priority=priority,
        est_tokens_in=10_000,
        est_tokens_out=2_000,
        caller_subsystem="pytest",
        require_capability=[],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("task_kind", "expected_provider", "expected_model_prefix"),
    [
        ("code-fix", "nvidia", "qwen/qwen3-coder"),
        ("narrative", "anthropic", "claude-sonnet"),  # sonnet not opus
        ("reasoning", "anthropic", "claude-opus"),  # opus for reasoning
        ("embedding", "mnemos-local", "bge-m3"),
        ("routing", "groq", "llama-3.1-8b-instant"),
    ],
)
async def test_task_type_routes_to_different_models(
    task_kind, expected_provider, expected_model_prefix
):
    """Each task_type MUST return a different model — not all the same Opus."""
    backend = _diverse_task_backend()
    decision = await route(_task_req(task_kind), backend)
    assert decision.provider == expected_provider, (
        f"{task_kind}: expected {expected_provider}, got {decision.provider}/{decision.model_id}"
    )
    assert decision.model_id.startswith(expected_model_prefix), (
        f"{task_kind}: expected model starting with '{expected_model_prefix}', got {decision.model_id}"
    )


@pytest.mark.asyncio
async def test_task_types_return_distinct_models():
    """All five task types must route to DIFFERENT model_ids."""
    backend = _diverse_task_backend()
    task_types = ("code-fix", "narrative", "reasoning", "embedding", "routing")
    results = {}
    for tt in task_types:
        decision = await route(_task_req(tt), backend)
        results[tt] = (decision.provider, decision.model_id)

    model_ids = {model_id for _, model_id in results.values()}
    assert len(model_ids) >= 5, (
        f"Expected 5 distinct models, got {len(model_ids)}: {results}"
    )
    # narrative must NOT be opus
    assert "opus" not in results["narrative"][1].lower()
    # embedding must be a dedicated embedding model
    assert results["embedding"][1] == "bge-m3"
    # routing must be small/fast
    assert results["routing"][1] == "llama-3.1-8b-instant"
