"""KNEMON hybrid model router."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import re
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, AsyncIterator, Optional

from mnemos.core.config import get_settings
from mnemos.core.plan_windows import compute_plan_window_id
from mnemos.core.recommendation import (
    _canonical_cap,
    _is_special_purpose,
    _preference_rank,
    recommendation_policy,
)
from mnemos.core.recommendation import (
    _capability_set as _rec_capability_set,
)
from mnemos.core.recommendation import (
    _tier_filter as _rec_tier_filter,
)

# Canonical provider identity — collapses historical/duplicate labels so a
# single policy entry covers all of them (e.g. "claude" -> "anthropic").
_PROVIDER_ALIASES = {"claude": "anthropic"}


def _canon_provider(name: str) -> str:
    canon = (name or "").strip().lower()
    return _PROVIDER_ALIASES.get(canon, canon)


def _csv_providers(raw: str) -> list[str]:
    return [_canon_provider(part) for part in (raw or "").split(",") if part.strip()]


class NoModelAvailable(RuntimeError):
    """Raised when the registry has no model satisfying hard constraints."""


@dataclass
class KnemonRouteRequest:
    task_kind: str
    priority: int
    est_tokens_in: int = 0
    est_tokens_out: int = 0
    caller_session_id: Optional[str] = None
    caller_subsystem: str = "api"
    exclude_providers: list[str] = field(default_factory=list)
    require_capability: list[str] = field(default_factory=list)
    max_cost_tier: Optional[str] = None
    est_tokens: Optional[int] = None
    # Stable, server-derived identity used only for burn enforcement.  The
    # caller session remains an audit/workspace label and must not be trusted
    # as a quota key at authenticated API boundaries.
    burn_principal_id: Optional[str] = None

    def __post_init__(self) -> None:
        if (
            self.est_tokens is not None
            and self.est_tokens_in == 0
            and self.est_tokens_out == 0
        ):
            self.est_tokens_in = max(0, int(self.est_tokens))


@dataclass
class KnemonRouteDecision:
    provider: str
    model_id: str
    auth_method: str
    path_kind: str
    estimated_cost_usd: float
    sub_window_utilization_pct: float
    fallback_chain: list[tuple]
    reasoning: str
    # Model-affinity dispatch (GRAEAE de8f4b2b layering / zeroclaw triage bridge):
    # for zeroclaw callers the decision pins the job to a specific provider/model
    # worker via a model:<provider_model> capability. Empty for non-dispatch
    # callers (api/internal) so existing consumers are unaffected.
    dispatch_kind: str = ""
    dispatch_required_capabilities: list[str] = field(default_factory=list)
    dispatch_cost_tier: str = ""
    dispatch_subscription_pools: list[str] = field(default_factory=list)
    # Exact selected subscription tier. Alias pools are dispatch capabilities,
    # not a stable accounting key, so ledger producers must use this value.
    plan_name: str = ""
    # Durable burn-accounting reservation. Callers should return this token on
    # the corresponding /v1/ledger write. Optional fields preserve the public
    # response contract for clients that have not adopted reservations yet.
    reservation_id: str | None = None
    reservation_expires_at: str | None = None


@dataclass
class _SessionLockEntry:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    users: int = 0


_SESSION_LOCKS: dict[str, _SessionLockEntry] = {}
_LAST_RESERVATION_CLEANUP: dict[int, tuple[Any, datetime]] = {}
_NAMED_PARAM_RE = re.compile(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)")
_TRUE_VALUES = {"1", "true", "yes", "on"}
logger = logging.getLogger(__name__)


class BurnReservationSchemaMissing(RuntimeError):
    """Raised when atomic burn accounting has not been migrated."""


class BurnReservationConflict(RuntimeError):
    """Raised when a ledger write cannot reconcile its reservation token."""


@dataclass(frozen=True)
class _BurnReservation:
    reservation_id: str | None
    expires_at: datetime | None
    burned: bool


# Caller-subsystem -> dispatchable provider allowlist (GRAEAE consult de8f4b2b 2026-05-28:
# Option A centralized allowlist, config-driven/version-controlled, NoModelAvailable -> caller blocks).
# A dispatch worker can only EXECUTE providers that have a gateway agent alias; KNEMON must never
# route it to a provider it cannot run (e.g. anthropic/claude, which is also policy-forbidden in the
# zeroclaw stack per CLAUDE.md directive 5). openai is intentionally absent: it is reserved for the
# explicit codex scarce path (kind=codex/review:/doctor:codex-fix), not the general dispatch route.
CALLER_DISPATCHABLE_PROVIDERS: dict[str, frozenset[str]] = {
    "zeroclaw": frozenset(
        {"groq", "xai", "deepseek", "deepseek-direct", "nvidia", "together", "gemini"}
    ),
}

# Zeroclaw-family provider aliases — providers a zeroclaw worker can execute.
_ZEROCLAW_PROVIDER_ALIASES = {
    "zeroclaw",
    "openclaw",
    "local",
    "local-llamacpp",
    "local-vllm",
}


def _model_capability(provider: str, model_id: str) -> str:
    """Stable model:<provider_model> capability used to pin a job to one worker."""
    safe = _normalize_pool(f"{provider}_{model_id}")
    return f"model:{safe}" if safe else "model:unknown"


def _dispatch_kind(provider: str, caller_subsystem: str = "") -> str:
    """Worker kind that should execute this job (zeroclaw for dispatch callers)."""
    if (caller_subsystem or "").strip().lower() == "zeroclaw":
        return "zeroclaw"
    provider_key = _canon_provider(provider)
    if provider_key in _ZEROCLAW_PROVIDER_ALIASES:
        return "zeroclaw"
    return provider_key or "provider-worker"


def _dispatch_required_capabilities(
    selected: dict[str, Any], required_caps: list[str], caller_subsystem: str = ""
) -> list[str]:
    """Append the model-affinity capability for zeroclaw dispatch callers."""
    out = list(required_caps)
    model_cap = _model_capability(
        str(selected.get("provider") or ""), str(selected.get("model_id") or "")
    )
    if (
        _dispatch_kind(str(selected.get("provider") or ""), caller_subsystem)
        == "zeroclaw"
        and model_cap not in out
    ):
        out.append(model_cap)
    return out


async def _call(value: Any, *args: Any, **kwargs: Any) -> Any:
    result = value(*args, **kwargs) if callable(value) else value
    return await result if inspect.isawaitable(result) else result


def _conn_from_tx(tx: Any) -> Any:
    return getattr(tx, "conn", tx)


def _asyncpg_sql(sql: str, params: dict[str, Any]) -> tuple[str, list[Any]]:
    values: list[Any] = []
    positions: dict[str, int] = {}

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in params:
            return match.group(0)
        if name not in positions:
            positions[name] = len(values) + 1
            values.append(params[name])
        return f"${positions[name]}"

    return _NAMED_PARAM_RE.sub(replace, sql), values


def _mysql_sql(sql: str, params: dict[str, Any]) -> tuple[str, list[Any]]:
    values: list[Any] = []

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in params:
            return match.group(0)
        values.append(params[name])
        return "%s"

    return _NAMED_PARAM_RE.sub(replace, sql), values


def _qmark_sql(sql: str, params: dict[str, Any]) -> tuple[str, list[Any]]:
    values: list[Any] = []

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in params:
            return match.group(0)
        values.append(params[name])
        return "?"

    return _NAMED_PARAM_RE.sub(replace, sql), values


def _named_params(sql: str, params: dict[str, Any]) -> dict[str, Any]:
    """Restrict ``params`` to the binds ``sql`` actually references.

    The PostgreSQL, MySQL and Db2 rewriters above already do this implicitly:
    they build their argument list by walking the SQL text, so a supplied bind
    the statement never mentions is silently unused. SQLite's driver ignores
    extra keys too. python-oracledb does not -- it raises
    ``DPY-4008: no bind placeholder named ":x" was found in the SQL text`` --
    which made Oracle the single backend where a shared bind dict is fatal.
    Filtering here gives the named-bind path the same tolerance as the other
    four dialects.

    This can only ever DROP a name absent from the SQL text; a bind the
    statement does reference is always kept, so a genuine name mismatch still
    surfaces loudly as a missing-bind error rather than being masked.
    """
    if not params:
        return {}
    present = {match.group(1) for match in _NAMED_PARAM_RE.finditer(sql)}
    return {name: value for name, value in params.items() if name in present}


def _is_mysql_connection(conn: Any) -> bool:
    identity = f"{type(conn).__module__}.{type(conn).__name__}".lower()
    return any(
        marker in identity for marker in ("mysql", "mariadb", "aiomysql", "asyncmy")
    )


def _dict_rows(rows: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows or []:
        if isinstance(row, dict):
            out.append({str(key).lower(): value for key, value in row.items()})
            continue
        try:
            items = dict(row).items()
        except (TypeError, ValueError):
            continue
        out.append({str(key).lower(): value for key, value in items})
    return out


async def _rows(
    backend: Any, sql: str, params: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    async with backend.transactional() as tx:
        conn = _conn_from_tx(tx)
        fetch = getattr(conn, "fetch", None)
        if callable(fetch):
            pg_sql, pg_params = _asyncpg_sql(sql, params or {})
            return _dict_rows(await _call(fetch, pg_sql, *pg_params))

        cursor = await _call(conn.cursor)
        try:
            if _is_mysql_connection(conn):
                mysql_sql, mysql_params = _mysql_sql(sql, params or {})
                await _call(cursor.execute, mysql_sql, tuple(mysql_params))
            else:
                await _call(cursor.execute, sql, _named_params(sql, params or {}))
            fetched = await _call(cursor.fetchall)
            description = getattr(cursor, "description", None) or []
            names = [str(col[0]).lower() for col in description]
            return [
                dict(row) if hasattr(row, "keys") else dict(zip(names, row))
                for row in fetched or []
            ]
        finally:
            close = getattr(cursor, "close", None)
            if close is not None:
                await _call(close)


def _connection_dialect(conn: Any) -> str:
    identity = f"{type(conn).__module__}.{type(conn).__name__}".lower()
    if callable(getattr(conn, "fetch", None)):
        return "postgres"
    if _is_mysql_connection(conn):
        return "mysql"
    if "sqlite" in identity:
        return "sqlite"
    if any(marker in identity for marker in ("db2", "ibm_db")):
        return "db2"
    if any(marker in identity for marker in ("oracle", "oracledb", "cx_oracle")):
        return "oracle"
    return "named"


def _row_limited_sql(sql: str, dialect: str, limit: int) -> str:
    """Apply a validated literal row limit for a supported database dialect."""
    row_limit = max(1, int(limit))
    clause = (
        f"LIMIT {row_limit}"
        if dialect in {"postgres", "sqlite", "mysql"}
        else f"FETCH FIRST {row_limit} ROWS ONLY"
    )
    return f"{sql.rstrip()}\n{clause}"


@asynccontextmanager
async def _session_lock(key: str) -> AsyncIterator[None]:
    """Serialize a session while retaining its lock only while it has users."""
    entry = _SESSION_LOCKS.setdefault(key, _SessionLockEntry())
    entry.users += 1
    try:
        async with entry.lock:
            yield
    finally:
        entry.users -= 1
        if entry.users == 0 and _SESSION_LOCKS.get(key) is entry:
            del _SESSION_LOCKS[key]


async def _tx_execute(conn: Any, sql: str, params: dict[str, Any] | None = None) -> Any:
    """Execute named-bind SQL on an already-open backend transaction."""
    values = params or {}
    execute = getattr(conn, "execute", None)
    if callable(getattr(conn, "fetch", None)) and callable(execute):
        pg_sql, pg_params = _asyncpg_sql(sql, values)
        return await _call(execute, pg_sql, *pg_params)

    cursor = await _call(conn.cursor)
    try:
        dialect = _connection_dialect(conn)
        if dialect == "mysql":
            mysql_sql, mysql_params = _mysql_sql(sql, values)
            await _call(cursor.execute, mysql_sql, tuple(mysql_params))
        elif dialect == "db2":
            db2_sql, db2_params = _qmark_sql(sql, values)
            await _call(cursor.execute, db2_sql, tuple(db2_params))
        else:
            await _call(cursor.execute, sql, _named_params(sql, values))
        return getattr(cursor, "rowcount", None)
    finally:
        close = getattr(cursor, "close", None)
        if close is not None:
            await _call(close)


async def _tx_rows(
    conn: Any, sql: str, params: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    """Fetch rows without opening a second transaction."""
    values = params or {}
    fetch = getattr(conn, "fetch", None)
    if callable(fetch):
        pg_sql, pg_params = _asyncpg_sql(sql, values)
        return _dict_rows(await _call(fetch, pg_sql, *pg_params))

    cursor = await _call(conn.cursor)
    try:
        dialect = _connection_dialect(conn)
        if dialect == "mysql":
            mysql_sql, mysql_params = _mysql_sql(sql, values)
            await _call(cursor.execute, mysql_sql, tuple(mysql_params))
        elif dialect == "db2":
            db2_sql, db2_params = _qmark_sql(sql, values)
            await _call(cursor.execute, db2_sql, tuple(db2_params))
        else:
            await _call(cursor.execute, sql, _named_params(sql, values))
        fetched = await _call(cursor.fetchall)
        description = getattr(cursor, "description", None) or []
        names = [str(col[0]).lower() for col in description]
        return [
            dict(row) if hasattr(row, "keys") else dict(zip(names, row))
            for row in fetched or []
        ]
    finally:
        close = getattr(cursor, "close", None)
        if close is not None:
            await _call(close)


def _allow_nonatomic_burn_development() -> bool:
    return (
        os.getenv("MNEMOS_KNEMON_ALLOW_NONATOMIC_BURN_DEVELOPMENT", "").strip().lower()
        in _TRUE_VALUES
    )


def _reservation_ttl_seconds() -> int:
    policy = get_settings().knemon
    raw = os.getenv("MNEMOS_KNEMON_BURN_RESERVATION_TTL_SECONDS", "")
    try:
        configured = (
            int(raw) if raw.strip() else int(policy.session_burn_window_seconds)
        )
    except (TypeError, ValueError):
        configured = int(policy.session_burn_window_seconds)
    return max(1, configured)


def _reservation_retention_seconds() -> int:
    raw = os.getenv("MNEMOS_KNEMON_BURN_RESERVATION_RETENTION_SECONDS", "")
    minimum = max(
        _reservation_ttl_seconds(),
        int(get_settings().knemon.session_burn_window_seconds),
    )
    try:
        configured = int(raw) if raw.strip() else 7 * 24 * 60 * 60
    except (TypeError, ValueError):
        configured = 7 * 24 * 60 * 60
    return max(minimum, configured)


def _reservation_cleanup_interval_seconds() -> int:
    raw = os.getenv("MNEMOS_KNEMON_BURN_RESERVATION_CLEANUP_INTERVAL_SECONDS", "")
    try:
        configured = int(raw) if raw.strip() else 300
    except (TypeError, ValueError):
        configured = 300
    return max(1, configured)


async def _cleanup_burn_reservations(
    backend: Any, *, now: datetime | None = None
) -> None:
    """Bound reservation history during normal routing traffic.

    Cleanup is throttled per backend instance. Errors are deliberately visible
    to the route caller, because silently failing retention would recreate the
    unbounded-growth defect.
    """
    current = now or datetime.now(timezone.utc)
    backend_key = id(backend)
    previous_entry = _LAST_RESERVATION_CLEANUP.get(backend_key)
    previous = (
        previous_entry[1]
        if previous_entry is not None and previous_entry[0] is backend
        else None
    )
    if (
        previous is not None
        and (current - previous).total_seconds()
        < _reservation_cleanup_interval_seconds()
    ):
        return
    cutoff = current - timedelta(seconds=_reservation_retention_seconds())
    async with backend.transactional() as tx:
        conn = _conn_from_tx(tx)
        deleted = await _tx_execute(
            conn,
            """
            DELETE FROM knemon_burn_reservations
            WHERE (status IN ('reconciled', 'released') AND reconciled_at < :cutoff_ts)
               OR (status = 'pending' AND expires_at < :cutoff_ts)
            """,
            {"cutoff_ts": cutoff},
        )
        await _tx_execute(
            conn,
            """
            DELETE FROM knemon_burn_sessions
            WHERE touched_at < :cutoff_ts
              AND NOT EXISTS (
                SELECT 1 FROM knemon_burn_reservations reservations
                WHERE reservations.session_id = knemon_burn_sessions.session_id
              )
            """,
            {"cutoff_ts": cutoff},
        )
    _LAST_RESERVATION_CLEANUP[backend_key] = (backend, current)
    logger.info("KNEMON burn reservation cleanup completed (deleted=%s)", deleted)


def _is_burn_reservation_schema_missing(exc: Exception) -> bool:
    sqlstate = str(getattr(exc, "sqlstate", "") or getattr(exc, "pgcode", ""))
    errno = getattr(exc, "errno", None)
    sqlcode = str(getattr(exc, "sqlcode", ""))
    message = str(exc).lower()
    table_named = any(
        table in message
        for table in (
            "knemon_burn_sessions",
            "knemon_burn_reservations",
            "knemon_ledger_principals",
        )
    )
    return (
        (sqlstate in {"42P01", "42704"} and table_named)
        or (errno == 1146 and table_named)
        or (sqlcode == "-204" and table_named)
        or (
            table_named
            and any(
                marker in message
                for marker in (
                    "no such table",
                    "ora-00942",
                    "does not exist",
                    "sqlstate=42704",
                    "sql0204n",
                )
            )
        )
    )


def _is_unique_violation(exc: Exception) -> bool:
    sqlstate = str(getattr(exc, "sqlstate", "") or getattr(exc, "pgcode", ""))
    errno = getattr(exc, "errno", None)
    code = getattr(exc, "code", None)
    message = str(exc).lower()
    return (
        sqlstate == "23505"
        or errno == 1062
        or code == 1
        or "ora-00001" in message
        or "sql0803" in message
    )


async def _ensure_and_lock_burn_session(
    conn: Any, session_id: str, now: datetime
) -> None:
    dialect = _connection_dialect(conn)
    params = {"session_id": session_id, "now_ts": now}
    if dialect == "postgres":
        insert_sql = """
            INSERT INTO knemon_burn_sessions (session_id, touched_at)
            VALUES (:session_id, :now_ts)
            ON CONFLICT (session_id) DO NOTHING
            """
    elif dialect == "mysql":
        insert_sql = """
            INSERT INTO knemon_burn_sessions (session_id, touched_at)
            VALUES (:session_id, :now_ts)
            ON DUPLICATE KEY UPDATE touched_at = knemon_burn_sessions.touched_at
            """
    elif dialect == "sqlite":
        insert_sql = """
            INSERT INTO knemon_burn_sessions (session_id, touched_at)
            VALUES (:session_id, :now_ts)
            ON CONFLICT (session_id) DO NOTHING
            """
    elif dialect == "oracle":
        insert_sql = """
            MERGE INTO knemon_burn_sessions target
            USING (SELECT :session_id AS session_id, :now_ts AS touched_at FROM dual) source
            ON (target.session_id = source.session_id)
            WHEN NOT MATCHED THEN
              INSERT (session_id, touched_at) VALUES (source.session_id, source.touched_at)
            """
    elif dialect == "db2":
        insert_sql = """
            MERGE INTO knemon_burn_sessions AS target
            USING (VALUES (:session_id, :now_ts)) AS source(session_id, touched_at)
            ON target.session_id = source.session_id
            WHEN NOT MATCHED THEN
              INSERT (session_id, touched_at) VALUES (source.session_id, source.touched_at)
            """
    else:
        insert_sql = """
            INSERT INTO knemon_burn_sessions (session_id, touched_at)
            VALUES (:session_id, :now_ts)
            """
    try:
        await _tx_execute(conn, insert_sql, params)
    except Exception as exc:
        # Oracle and Db2 MERGE can lose a concurrent first-row race. Their
        # drivers roll back the failed statement (not the transaction), so the
        # loser can continue by locking the row inserted by the winner.
        if dialect not in {"oracle", "db2"} or not _is_unique_violation(exc):
            raise
    lock_suffix = "" if dialect == "sqlite" else " FOR UPDATE"
    await _tx_rows(
        conn,
        "SELECT session_id FROM knemon_burn_sessions WHERE session_id = :session_id"
        + lock_suffix,
        {"session_id": session_id},
    )
    await _tx_execute(
        conn,
        "UPDATE knemon_burn_sessions SET touched_at = :now_ts WHERE session_id = :session_id",
        params,
    )


async def _reserve_session_burn(
    backend: Any, session_id: str | None
) -> _BurnReservation:
    """Atomically check session burn and reserve this routed request."""
    if not session_id:
        return _BurnReservation(None, None, False)
    policy = get_settings().knemon
    threshold = int(policy.session_burn_requests_per_hour)
    if threshold <= 0:
        return _BurnReservation(None, None, False)
    if _allow_nonatomic_burn_development():
        return _BurnReservation(None, None, await _session_burned(backend, session_id))

    from mnemos.domain.knemon.migrations import ensure_knemon_schema

    try:
        await ensure_knemon_schema(backend)
    except Exception as exc:
        raise BurnReservationSchemaMissing(
            f"KNEMON schema migrations 0052_knemon_burn_reservations and "
            f"0053_knemon_ledger_principals failed: {exc}"
        ) from exc
    now = datetime.now(timezone.utc)
    await _cleanup_burn_reservations(backend, now=now)
    since = now - timedelta(seconds=int(policy.session_burn_window_seconds))
    expires_at = now + timedelta(seconds=_reservation_ttl_seconds())
    reservation_id = str(uuid.uuid4())
    try:
        async with backend.transactional() as tx:
            conn = _conn_from_tx(tx)
            await _ensure_and_lock_burn_session(conn, session_id, now)
            ledger_rows = await _tx_rows(
                conn,
                """
                SELECT COALESCE(SUM(request_count), 0) AS requests_used
                FROM usage_ledger ledger
                WHERE ts >= :since_ts
                  AND EXISTS (
                    SELECT 1 FROM knemon_ledger_principals principals
                    WHERE principals.ledger_id = ledger.id
                      AND principals.principal_id = :session_id
                  )
                """,
                {"session_id": session_id, "since_ts": since},
            )
            reservation_rows = await _tx_rows(
                conn,
                """
                SELECT COALESCE(SUM(request_count), 0) AS requests_reserved
                FROM knemon_burn_reservations
                WHERE session_id = :session_id
                  AND status = 'pending'
                  AND expires_at > :now_ts
                """,
                {"session_id": session_id, "now_ts": now},
            )
            used = _to_int((ledger_rows[0] if ledger_rows else {}).get("requests_used"))
            reserved = _to_int(
                (reservation_rows[0] if reservation_rows else {}).get(
                    "requests_reserved"
                )
            )
            await _tx_execute(
                conn,
                """
                INSERT INTO knemon_burn_reservations (
                  reservation_id, session_id, reserved_at, expires_at, request_count, status
                ) VALUES (
                  :reservation_id, :session_id, :reserved_at, :expires_at, 1, 'pending'
                )
                """,
                {
                    "reservation_id": reservation_id,
                    "session_id": session_id,
                    "reserved_at": now,
                    "expires_at": expires_at,
                },
            )
        return _BurnReservation(
            reservation_id, expires_at, used + reserved >= threshold
        )
    except Exception as exc:
        if not _is_burn_reservation_schema_missing(exc):
            raise
        if _allow_nonatomic_burn_development():
            return _BurnReservation(
                None, None, await _session_burned(backend, session_id)
            )
        raise BurnReservationSchemaMissing(
            "KNEMON atomic burn reservation schema is missing; apply migration "
            "0052_knemon_burn_reservations and 0053_knemon_ledger_principals, then restart. "
            "For development or a single-worker "
            "deployment only, set MNEMOS_KNEMON_ALLOW_NONATOMIC_BURN_DEVELOPMENT=true "
            "to use legacy non-atomic accounting."
        ) from exc


async def _release_burn_reservation(backend: Any, reservation_id: str | None) -> None:
    if not reservation_id:
        return
    async with backend.transactional() as tx:
        await _tx_execute(
            _conn_from_tx(tx),
            """
            UPDATE knemon_burn_reservations
            SET status = 'released', reconciled_at = :now_ts
            WHERE reservation_id = :reservation_id AND status = 'pending'
            """,
            {"reservation_id": reservation_id, "now_ts": datetime.now(timezone.utc)},
        )


async def reconcile_burn_reservation(
    tx: Any,
    *,
    reservation_id: str | None,
    session_id: str,
    ledger_id: int,
    request_count: int = 1,
) -> list[str]:
    """Reconcile route reservations inside the ledger insert transaction.

    An explicit token is strict. For backwards compatibility, callers without
    a token reconcile the oldest pending reservations for the same session.
    """
    conn = _conn_from_tx(tx)
    now = datetime.now(timezone.utc)
    await _ensure_and_lock_burn_session(conn, session_id, now)
    if reservation_id:
        rows = await _tx_rows(
            conn,
            """
            SELECT reservation_id, session_id, status
            FROM knemon_burn_reservations
            WHERE reservation_id = :reservation_id
            """,
            {"reservation_id": reservation_id},
        )
        row = rows[0] if rows else None
        if row is None:
            raise BurnReservationConflict(
                f"burn reservation {reservation_id} was not found"
            )
        if str(row.get("session_id")) != session_id:
            raise BurnReservationConflict(
                f"burn reservation {reservation_id} belongs to session {row.get('session_id')!r}, not {session_id!r}"
            )
        if str(row.get("status")) != "pending":
            raise BurnReservationConflict(
                f"burn reservation {reservation_id} is {row.get('status')!r}, expected 'pending'"
            )
        selected_ids = [reservation_id]
    else:
        row_limit = max(0, int(request_count))
        if row_limit == 0:
            return []
        sql = _row_limited_sql(
            """
            SELECT reservation_id
            FROM knemon_burn_reservations
            WHERE session_id = :session_id AND status = 'pending'
              AND expires_at > :now_ts
            ORDER BY reserved_at, reservation_id
            """,
            _connection_dialect(conn),
            row_limit,
        )
        rows = await _tx_rows(
            conn,
            sql,
            {"session_id": session_id, "now_ts": now},
        )
        selected_ids = [str(row["reservation_id"]) for row in rows]

    for selected_id in selected_ids:
        await _tx_execute(
            conn,
            """
            UPDATE knemon_burn_reservations
            SET status = 'reconciled', reconciled_at = :now_ts, ledger_id = :ledger_id
            WHERE reservation_id = :reservation_id AND status = 'pending'
            """,
            {"reservation_id": selected_id, "now_ts": now, "ledger_id": ledger_id},
        )
    return selected_ids


def _to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    except ValueError:
        pass
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def _plan_is_effective(row: dict[str, Any], today: date | None = None) -> bool:
    if "effective_from" not in row and "effective_until" not in row:
        return True
    today = today or datetime.now(timezone.utc).date()
    effective_from = _to_date(row.get("effective_from"))
    effective_until = _to_date(row.get("effective_until"))
    if effective_from is not None and effective_from > today:
        return False
    if effective_until is not None and effective_until < today:
        return False
    return True


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, Decimal)):
        return bool(value)
    return str(value).strip().lower() in {"1", "t", "true", "y", "yes"}


def _normalize_capabilities(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, (list, tuple, set)):
        return {str(item).strip() for item in value if str(item).strip()}
    raw = str(value).strip()
    if not raw:
        return set()
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return {str(item).strip() for item in parsed if str(item).strip()}
        if isinstance(parsed, dict):
            return {
                str(key).strip()
                for key, enabled in parsed.items()
                if enabled and str(key).strip()
            }
    except json.JSONDecodeError:
        pass
    return {
        item.strip().strip('"').strip("'")
        for item in raw.strip("{}[]").split(",")
        if item.strip()
    }


def _normalize_pool(value: Any) -> str:
    return "".join(
        ch if ch.isalnum() else "_" for ch in str(value or "").strip().lower()
    ).strip("_")


def _normalize_pools(value: Any) -> set[str]:
    return {
        _normalize_pool(pool)
        for pool in _normalize_capabilities(value)
        if _normalize_pool(pool)
    }


def _subscription_pool_aliases(plan: dict[str, Any]) -> set[str]:
    provider = _normalize_pool(plan.get("provider"))
    plan_name = _normalize_pool(plan.get("plan_name"))
    parent_plan_id = _normalize_pool(plan.get("parent_plan_id"))
    aliases = {pool for pool in (plan_name, parent_plan_id) if pool}
    if provider:
        aliases.add(f"{provider}_subscription")
    if provider == "anthropic":
        aliases.add("claude_subscription")
    if provider == "openai":
        if "chatgpt" in plan_name:
            aliases.add("chatgpt_subscription")
        if "codex" in plan_name:
            aliases.add("codex_subscription")
    return aliases


def _candidate_plan_family(candidate: dict[str, Any] | None) -> str | None:
    if not candidate:
        return None
    provider = _normalize_pool(candidate.get("provider"))
    if provider == "anthropic":
        return "claude_subscription"
    if provider != "openai":
        return None
    raw = " ".join(
        str(part or "")
        for part in (
            candidate.get("model_id"),
            candidate.get("display_name"),
            " ".join(candidate.get("capabilities") or []),
        )
    ).lower()
    if "codex" in raw:
        return "codex_subscription"
    return "chatgpt_subscription"


def _is_missing_worker_schema_error(exc: Exception) -> bool:
    """Recognize only driver errors for an absent agents table/column."""
    sqlstate = str(getattr(exc, "sqlstate", "") or getattr(exc, "pgcode", ""))
    errno = getattr(exc, "errno", None)
    sqlcode = str(getattr(exc, "sqlcode", ""))
    message = str(exc).lower()
    if sqlstate in {"42P01", "42703", "42704"}:
        return True
    if errno in {1054, 1146}:
        return True
    if sqlcode in {"-204", "-206"}:
        return True
    return any(
        marker in message
        for marker in (
            "no such table: agents",
            "no such column: subscription_pools",
            "ora-00942",
            "ora-00904",
        )
    )


async def _worker_pools_for_session(
    backend: Any, session_id: str | None
) -> set[str] | None:
    if not session_id:
        return None
    rows: list[dict[str, Any]]
    try:
        rows = await _rows(
            backend,
            """
            SELECT subscription_pools
            FROM agents
            WHERE session_id = :session_id
              AND status IN ('online', 'idle')
            ORDER BY last_heartbeat DESC
            FETCH FIRST 1 ROW ONLY
            """,
            {"session_id": session_id},
        )
    except Exception as exc:
        msg = str(exc).lower()
        if _is_missing_worker_schema_error(exc):
            return set()
        if "fetch" not in msg:
            raise
        try:
            rows = await _rows(
                backend,
                """
                SELECT subscription_pools
                FROM agents
                WHERE session_id = :session_id
                  AND status IN ('online', 'idle')
                ORDER BY last_heartbeat DESC
                LIMIT 1
                """,
                {"session_id": session_id},
            )
        except Exception as fallback_exc:
            if _is_missing_worker_schema_error(fallback_exc):
                return set()
            raise
    if rows:
        return _normalize_pools(rows[0].get("subscription_pools"))
    return set()


def _worker_has_pool(worker_pools: set[str] | None, plan: dict[str, Any]) -> bool:
    if worker_pools is None:
        return True
    return bool(worker_pools.intersection(_subscription_pool_aliases(plan)))


def _quality(row: dict[str, Any]) -> float:
    for key in ("quality_score", "graeae_weight", "weight"):
        value = _to_float(row.get(key), -1.0)
        if value >= 0:
            return value / 100.0 if value > 1.0 else value
    arena = _to_float(row.get("arena_score"), 0.0)
    if arena > 0:
        return max(0.0, min(1.0, arena / 1500.0))
    return 0.0


def _tier(row: dict[str, Any]) -> str:
    raw = str(row.get("cost_tier") or row.get("usage_tier") or "").strip().upper()
    if raw in {"A", "B", "C"}:
        return raw
    quality = _quality(row)
    if quality >= 0.85:
        return "A"
    if quality >= 0.75:
        return "B"
    return "C"


def _tier_allowed(tier: str, max_cost_tier: str | None) -> bool:
    if not max_cost_tier:
        return True
    order = ["A", "B", "C"]
    try:
        return order.index((tier or "C").upper()) <= order.index(max_cost_tier.upper())
    except ValueError:
        return False


def _price(row: dict[str, Any], tokens_in: int, tokens_out: int) -> float:
    return round(
        (
            max(0, tokens_in) * _to_float(row.get("input_cost_per_mtok"))
            + max(0, tokens_out) * _to_float(row.get("output_cost_per_mtok"))
        )
        / 1_000_000.0,
        6,
    )


async def _registry_candidates(
    req: KnemonRouteRequest, backend: Any
) -> list[dict[str, Any]]:
    rows = await _rows(
        backend,
        """
        SELECT provider, model_id, display_name, capabilities,
               input_cost_per_mtok, output_cost_per_mtok,
               context_window, arena_score, graeae_weight,
               available, deprecated
        FROM model_registry
        ORDER BY graeae_weight DESC
        """,
    )
    providers_cfg = get_settings().providers
    # Deployment-default excludes (canonicalised) merged with per-request set.
    excluded = set(_csv_providers(providers_cfg.knemon_exclude_providers))
    excluded |= {_canon_provider(p) for p in req.exclude_providers if p.strip()}
    allowed = CALLER_DISPATCHABLE_PROVIDERS.get(
        (req.caller_subsystem or "").strip().lower()
    )
    required = {cap.strip() for cap in req.require_capability if cap.strip()}
    min_context = int(max(0, req.est_tokens_in) * 1.2)
    candidates: list[dict[str, Any]] = []
    for row in rows:
        provider = str(row.get("provider") or "").strip()
        if not provider or _canon_provider(provider) in excluded:
            continue
        # Caller-eligibility allowlist: dispatch workers (e.g. zeroclaw) can only execute providers
        # backed by a gateway alias. Callers absent from the map (api, internal) are unrestricted.
        if allowed is not None and provider.lower() not in allowed:
            continue
        if not _truthy(row.get("available", True)) or _truthy(
            row.get("deprecated", False)
        ):
            continue
        if _to_int(row.get("context_window"), 0) < min_context:
            continue
        caps = _normalize_capabilities(row.get("capabilities"))
        if required and not required.issubset(caps):
            continue
        enriched = dict(row)
        enriched["capabilities"] = sorted(caps)
        enriched["quality"] = _quality(enriched)
        enriched["tier"] = _tier(enriched)
        if not _tier_allowed(enriched["tier"], req.max_cost_tier):
            continue
        enriched["estimated_cost_usd"] = _price(
            enriched, req.est_tokens_in, req.est_tokens_out
        )
        candidates.append(enriched)
    preference = _csv_providers(providers_cfg.knemon_provider_preference)
    if preference:
        rank = {name: idx for idx, name in enumerate(preference)}
        candidates.sort(
            key=lambda row: (
                rank.get(
                    _canon_provider(str(row.get("provider") or "")), len(preference)
                ),
                -_to_float(row.get("graeae_weight")),
            )
        )
    else:
        candidates.sort(
            key=lambda row: _to_float(row.get("graeae_weight")), reverse=True
        )
    return candidates


def _apply_priority_ceiling(
    candidates: list[dict[str, Any]],
    priority: int,
    *,
    requested_priority: int | None = None,
) -> list[dict[str, Any]]:
    policy = get_settings().knemon
    quality_priority = max(
        priority, requested_priority if requested_priority is not None else priority
    )
    if quality_priority >= 14:
        return [row for row in candidates if row["quality"] >= policy.g1_quality_floor]
    if priority >= 10:
        return [
            row
            for row in candidates
            if row["tier"] in {"A", "B"} and row["quality"] >= policy.g2_quality_floor
        ]
    eligible = [row for row in candidates if row["tier"] in {"A", "B"}]
    return sorted(
        eligible,
        key=lambda row: (row["tier"] != "A", -_to_float(row.get("graeae_weight"))),
    )


async def _plans_by_provider(
    backend: Any, as_of: date | None = None
) -> dict[str, list[dict[str, Any]]]:
    sql = """
        SELECT provider, plan_name, auth_method, path_kind, monthly_usd, msg_cap,
               msg_window_seconds, token_cap, token_window_seconds,
               reset_anchor, overage_pricing_per_mtok_in,
               overage_pricing_per_mtok_out, effective_from, effective_until,
               parent_plan_id
        FROM subscription_plans
        ORDER BY provider, COALESCE(monthly_usd, 0) DESC, COALESCE(msg_cap, 0) DESC, plan_name
        """
    try:
        rows = await _rows(backend, sql)
        today = as_of or datetime.now(timezone.utc).date()
        rows = [row for row in rows if _plan_is_effective(row, today)]
    except Exception as exc:
        msg = str(exc).lower()
        if (
            "coalesce" not in msg
            and "effective_from" not in msg
            and "path_kind" not in msg
            and "parent_plan_id" not in msg
        ):
            raise
        rows = await _rows(
            backend,
            """
            SELECT provider, plan_name, auth_method, monthly_usd, msg_cap,
                   msg_window_seconds, token_cap, token_window_seconds,
                   reset_anchor, overage_pricing_per_mtok_in,
                   overage_pricing_per_mtok_out
            FROM subscription_plans
            ORDER BY provider, monthly_usd DESC, msg_cap DESC, plan_name
            """,
        )
    out: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not _plan_is_effective(row, as_of):
            continue
        out.setdefault(str(row.get("provider") or "").lower(), []).append(row)
    return out


def _best_plan(
    plans: dict[str, list[dict[str, Any]]],
    provider: str,
    worker_pools: set[str] | None = None,
    candidate: dict[str, Any] | None = None,
) -> dict[str, Any]:
    provider_plans = plans.get(provider.lower()) or []
    if worker_pools is not None:
        for plan in provider_plans:
            if _worker_has_pool(worker_pools, plan):
                return plan
    if provider_plans:
        if worker_pools is None:
            family = _candidate_plan_family(candidate)
            if family is not None:
                for plan in provider_plans:
                    if family in _subscription_pool_aliases(plan):
                        return plan
        return provider_plans[0]
    return {
        "provider": provider,
        "plan_name": "api",
        "auth_method": "api",
        "path_kind": "api",
    }


def _plan_usage_key(
    plan: dict[str, Any], now: datetime
) -> tuple[str, str, str, str | None, str]:
    plan_name = str(plan.get("plan_name") or "api")
    provider = str(plan.get("provider") or "")
    path_kind, legacy_path_kind = _usage_path_kinds(plan)
    window_id = compute_plan_window_id(
        provider,
        plan_name,
        now,
        reset_anchor=str(plan.get("reset_anchor") or "monthly"),
        window_seconds=_to_int(
            plan.get("msg_window_seconds") or plan.get("token_window_seconds"), 0
        )
        or None,
    )
    return provider, plan_name, path_kind, legacy_path_kind, window_id


async def _usage_for_plan(
    backend: Any,
    plan: dict[str, Any],
    *,
    now: datetime | None = None,
) -> tuple[int, int]:
    provider, plan_name, path_kind, legacy_path_kind, window_id = _plan_usage_key(
        plan, now or datetime.now(timezone.utc)
    )
    sql = """
        SELECT COALESCE(SUM(request_count), 0) AS requests_used,
               COALESCE(SUM(tokens_in + tokens_out + tokens_reasoning), 0) AS tokens_used
        FROM usage_ledger
        WHERE provider = :provider
          AND tier = :plan_name
          AND (path_kind = :path_kind OR (:legacy_path_kind IS NOT NULL AND path_kind = :legacy_path_kind))
          AND plan_window_id LIKE :window_pattern
        """
    params = {
        "provider": provider,
        "plan_name": plan_name,
        "path_kind": path_kind,
        "legacy_path_kind": legacy_path_kind,
        "window_pattern": f"{window_id}%",
    }
    try:
        rows = await _rows(backend, sql, params)
    except Exception as exc:
        if "path_kind" not in str(exc).lower():
            raise
        rows = await _rows(
            backend,
            """
            SELECT COALESCE(SUM(request_count), 0) AS requests_used,
                   COALESCE(SUM(tokens_in + tokens_out + tokens_reasoning), 0) AS tokens_used
            FROM usage_ledger
            WHERE provider = :provider
              AND tier = :plan_name
              AND plan_window_id LIKE :window_pattern
            """,
            params,
        )
    row = rows[0] if rows else {}
    return _to_int(row.get("requests_used")), _to_int(row.get("tokens_used"))


async def _usage_for_plan_cached(
    backend: Any,
    plan: dict[str, Any],
    cache: dict[tuple[str, str, str, str | None, str], tuple[int, int]],
) -> tuple[int, int]:
    now = datetime.now(timezone.utc)
    key = _plan_usage_key(plan, now)
    if key not in cache:
        cache[key] = await _usage_for_plan(backend, plan, now=now)
    return cache[key]


def _utilization(plan: dict[str, Any], requests_used: int, tokens_used: int) -> float:
    msg_cap = _to_float(plan.get("msg_cap"))
    if msg_cap > 0:
        return round((requests_used / msg_cap) * 100.0, 2)
    token_cap = _to_float(plan.get("token_cap"))
    if token_cap > 0:
        return round((tokens_used / token_cap) * 100.0, 2)
    return 0.0


def _usage_path_kinds(plan: dict[str, Any]) -> tuple[str, str | None]:
    path_kind = str(plan.get("path_kind") or "api").lower()
    auth_method = str(plan.get("auth_method") or "api").lower()
    legacy_path_kind = (
        "api" if auth_method == "subscription" and path_kind != "api" else None
    )
    return path_kind, legacy_path_kind


async def _session_burned(backend: Any, session_id: str | None) -> bool:
    if not session_id:
        return False
    policy = get_settings().knemon
    threshold = policy.session_burn_requests_per_hour
    if threshold <= 0:
        return False
    since = datetime.now(timezone.utc) - timedelta(
        seconds=policy.session_burn_window_seconds
    )
    if _allow_nonatomic_burn_development():
        sql = """
            SELECT COALESCE(SUM(request_count), 0) AS requests_used
            FROM usage_ledger
            WHERE session_id = :session_id AND ts >= :since_ts
        """
    else:
        sql = """
            SELECT COALESCE(SUM(request_count), 0) AS requests_used
            FROM usage_ledger ledger
            WHERE ts >= :since_ts
              AND EXISTS (
                SELECT 1 FROM knemon_ledger_principals principals
                WHERE principals.ledger_id = ledger.id
                  AND principals.principal_id = :session_id
              )
        """
    rows = await _rows(backend, sql, {"session_id": session_id, "since_ts": since})
    return _to_int((rows[0] if rows else {}).get("requests_used")) >= threshold


def _downgrade_priority(priority: int) -> int:
    if priority >= 14:
        return 13
    if priority >= 10:
        return 9
    return priority


def _fallback_bucket(item: dict[str, Any]) -> int:
    policy = get_settings().knemon
    auth = str(item.get("auth_method") or "api").lower()
    util = _to_float(item.get("sub_window_utilization_pct"))
    cost = _to_float(item.get("estimated_cost_usd"))
    if auth == "free":
        return 0
    if auth == "subscription" and util < policy.subscription_preferred_utilization_pct:
        return 1
    if auth in {"api", "token"} and cost < policy.low_priority_api_cost_ceiling_usd:
        return 2
    if auth in {"api", "token"}:
        return 3
    return 4


def _fallback_chain(
    candidates: list[dict[str, Any]], selected: dict[str, Any]
) -> list[tuple]:
    alternates = [
        row
        for row in candidates
        if (row["provider"], row["model_id"])
        != (selected["provider"], selected["model_id"])
    ]
    alternates.sort(
        key=lambda row: (
            _fallback_bucket(row),
            _to_float(row.get("estimated_cost_usd")),
            -_to_float(row.get("graeae_weight")),
        )
    )
    return [
        (
            row["provider"],
            row["model_id"],
            row.get("auth_method", "api"),
            row.get("path_kind", "api"),
            row.get("estimated_cost_usd", 0.0),
        )
        for row in alternates[:3]
    ]


async def _route_locked(
    req: KnemonRouteRequest,
    backend: Any,
    reservation: _BurnReservation | None = None,
) -> KnemonRouteDecision:
    candidates = await _registry_candidates(req, backend)
    if not candidates:
        raise NoModelAvailable(
            "no model satisfies required capabilities, provider exclusions, and context window"
        )

    rec_policy = recommendation_policy(req.task_kind)
    reservation = reservation or await _reserve_session_burn(
        backend, req.burn_principal_id or req.caller_session_id
    )
    session_burned = reservation.burned
    effective_priority = (
        _downgrade_priority(req.priority) if session_burned else req.priority
    )
    priority_exempt_task = rec_policy.task_type in {"embedding", "routing"}
    if not priority_exempt_task:
        candidates = _apply_priority_ceiling(
            candidates, effective_priority, requested_priority=req.priority
        )
        if not candidates:
            raise NoModelAvailable(
                "no model satisfies priority tier and quality constraints"
            )

    # ── Task-type capability filter (recommendation policy) ──────────────────
    # Narrow candidates by the task_kind's required/any/excluded capabilities,
    # strip special-purpose models, apply tier preferences, and rank by
    # preferred models so that code-fix routes to a coder, narrative to a chat
    # model, embedding to an embedding model, routing to a small/fast model, etc.
    # Canonicalise policy caps through the same alias table used for model
    # caps so that "code"→"coding" and CSVs match (router side of the
    # _caps_match fix in recommendation.py).
    _task_required_caps: set[str] = {
        _canonical_cap(c) for c in rec_policy.required_caps
    } | {_canonical_cap(cap.strip()) for cap in req.require_capability if cap.strip()}
    _task_any_caps: set[str] = {_canonical_cap(c) for c in rec_policy.any_caps}
    _task_excluded_caps: set[str] = {
        _canonical_cap(c) for c in rec_policy.excluded_caps
    }
    _task_filtered: list[dict[str, Any]] = []
    for row in candidates:
        # Use the recommendation module's capability normalizer which
        # handles aliases (code→coding, coder→coding, embed→embedding, etc.)
        caps = _rec_capability_set(row.get("capabilities"))
        # required: ALL must be present
        if _task_required_caps and not _task_required_caps.issubset(caps):
            continue
        # any_caps: at least ONE if policy defines them
        if _task_any_caps and not any(cap in caps for cap in _task_any_caps):
            continue
        # excluded: NONE may be present
        if any(cap in caps for cap in _task_excluded_caps):
            continue
        _task_filtered.append(row)
    if _task_filtered:
        # Strip special-purpose models (content-safety, moderation, etc.)
        _task_filtered = [r for r in _task_filtered if not _is_special_purpose(r)]
        # Filter by policy-allowed tiers (with fallback)
        _task_filtered = _rec_tier_filter(_task_filtered, rec_policy)
        # Sort by preference rank (preferred models first), then cost, then quality
        _task_filtered.sort(
            key=lambda row: (
                _preference_rank(row, rec_policy),
                _to_float(row.get("estimated_cost_usd")),
                -_to_float(row.get("graeae_weight") or row.get("quality", 0)),
            )
        )
        candidates = _task_filtered
    elif _task_required_caps or _task_any_caps:
        raise NoModelAvailable("no model satisfies task-type capability requirements")
    if not candidates:
        raise NoModelAvailable("no model satisfies task-type capability requirements")

    plans = await _plans_by_provider(backend)
    worker_pools = await _worker_pools_for_session(backend, req.caller_session_id)
    policy = get_settings().knemon
    enriched: list[dict[str, Any]] = []
    selected: dict[str, Any] | None = None
    reasons: list[str] = []
    blocked_subscription_keys: set[tuple[str, str]] = set()
    usage_by_plan: dict[tuple[str, str, str, str | None, str], tuple[int, int]] = {}
    for index, row in enumerate(candidates):
        plan = _best_plan(plans, row["provider"], worker_pools, row)
        auth_method = str(plan.get("auth_method") or "api").lower()
        path_kind = str(plan.get("path_kind") or auth_method).lower()
        requests_used, tokens_used = (
            await _usage_for_plan_cached(backend, plan, usage_by_plan)
            if auth_method == "subscription"
            else (0, 0)
        )
        util = _utilization(plan, requests_used, tokens_used)
        item = {
            **row,
            "auth_method": auth_method,
            "path_kind": path_kind,
            "plan_name": plan.get("plan_name", "api"),
            "sub_window_utilization_pct": util,
        }
        enriched.append(item)
        if auth_method == "subscription":
            if not _worker_has_pool(worker_pools, plan):
                blocked_subscription_keys.add((row["provider"], row["model_id"]))
                required = sorted(_subscription_pool_aliases(plan))
                reasons.append(
                    "skipped subscription because caller workspace lacks pool "
                    f"{required[0] if required else item['plan_name']}"
                )
                continue
            if util < policy.subscription_preferred_utilization_pct:
                selected = item
                reasons.append(
                    "selected subscription under "
                    f"{policy.subscription_preferred_utilization_pct:.0f}% utilization ({util:.2f}%)"
                )
                break
            if (
                not session_burned
                and util <= policy.subscription_near_cap_pct
                and effective_priority >= 12
            ):
                selected = item
                reasons.append(
                    f"selected subscription near cap for priority {effective_priority} ({util:.2f}%)"
                )
                break
            no_other_candidate = index == len(candidates) - 1
            if (
                util > policy.subscription_near_cap_pct
                and util < 100
                and effective_priority >= 14
            ):
                selected = item
                reasons.append(
                    "selected over-"
                    f"{policy.subscription_near_cap_pct:.0f}% subscription for G1 priority "
                    f"{effective_priority} ({util:.2f}%)"
                )
                break
            if util > policy.subscription_near_cap_pct and no_other_candidate:
                selected = item
                reasons.append(
                    "selected over-"
                    f"{policy.subscription_near_cap_pct:.0f}% subscription because no alternate remained "
                    f"({util:.2f}%)"
                )
                break
            reasons.append(f"skipped subscription at {util:.2f}% utilization")
            continue
        if auth_method == "free" and effective_priority < 12:
            selected = item
            reasons.append("selected free plan for low-priority request")
            break
        if auth_method in {"api", "token"}:
            if (
                effective_priority < 10
                and item["estimated_cost_usd"]
                >= policy.low_priority_api_cost_ceiling_usd
            ):
                reasons.append(
                    f"skipped API cost ${item['estimated_cost_usd']:.4f} for low-priority request"
                )
                continue
            selected = item
            reasons.append(f"selected {auth_method} candidate")
            break

    if selected is None:
        raise NoModelAvailable(
            "no model survived subscription, free, and API waterfall rules"
        )

    seen = {(row["provider"], row["model_id"]) for row in enriched}
    fallback_candidates = list(enriched)
    for row in candidates:
        if (row["provider"], row["model_id"]) in seen:
            continue
        plan = _best_plan(plans, row["provider"], worker_pools, row)
        auth_method = str(plan.get("auth_method") or "api").lower()
        path_kind = str(plan.get("path_kind") or auth_method).lower()
        util = 0.0
        if auth_method == "subscription" and not _worker_has_pool(worker_pools, plan):
            blocked_subscription_keys.add((row["provider"], row["model_id"]))
            continue
        if auth_method == "subscription":
            requests_used, tokens_used = await _usage_for_plan_cached(
                backend, plan, usage_by_plan
            )
            util = _utilization(plan, requests_used, tokens_used)
        fallback_candidates.append(
            {
                **row,
                "auth_method": auth_method,
                "path_kind": path_kind,
                "plan_name": plan.get("plan_name", "api"),
                "sub_window_utilization_pct": util,
            }
        )
    if blocked_subscription_keys:
        fallback_candidates = [
            row
            for row in fallback_candidates
            if (row["provider"], row["model_id"]) not in blocked_subscription_keys
        ]
    return KnemonRouteDecision(
        provider=selected["provider"],
        model_id=selected["model_id"],
        auth_method=selected["auth_method"],
        path_kind=selected["path_kind"],
        estimated_cost_usd=float(selected["estimated_cost_usd"]),
        sub_window_utilization_pct=float(selected["sub_window_utilization_pct"]),
        fallback_chain=_fallback_chain(fallback_candidates, selected),
        reasoning="; ".join(reasons),
        dispatch_kind=_dispatch_kind(str(selected["provider"]), req.caller_subsystem),
        dispatch_required_capabilities=_dispatch_required_capabilities(
            selected,
            [c.strip() for c in req.require_capability if c.strip()],
            req.caller_subsystem,
        ),
        dispatch_cost_tier=str(selected.get("tier") or ""),
        dispatch_subscription_pools=(
            sorted(
                _subscription_pool_aliases(
                    {
                        "provider": selected["provider"],
                        "plan_name": selected.get("plan_name"),
                    }
                )
            )
            if selected.get("auth_method") == "subscription"
            else []
        ),
        plan_name=str(selected.get("plan_name") or "api"),
        reservation_id=reservation.reservation_id,
        reservation_expires_at=(
            reservation.expires_at.isoformat() if reservation.expires_at else None
        ),
    )


async def route(req: KnemonRouteRequest, backend: Any) -> KnemonRouteDecision:
    """Route a request to a provider/model using KNEMON waterfall policy."""
    burn_key = req.burn_principal_id or req.caller_session_id
    if burn_key:
        async with _session_lock(burn_key):
            reservation = await _reserve_session_burn(backend, burn_key)
            try:
                return await _route_locked(req, backend, reservation)
            except BaseException:
                try:
                    await asyncio.shield(
                        _release_burn_reservation(backend, reservation.reservation_id)
                    )
                except Exception:
                    logger.exception(
                        "failed to release KNEMON burn reservation after routing failure"
                    )
                raise
    return await _route_locked(req, backend)
