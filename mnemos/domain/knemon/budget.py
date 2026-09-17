"""KNEMON monetary admission and crash-safe per-attempt accounting.

Admission serializes each caller subsystem in the database and counts ledger
spend, unlinked settled costs, and all outstanding reservations. Finalization
links a ledger append in the same transaction. Expiry is recovery provenance,
not proof an upstream request was free; late authoritative usage corrects it.

The aggregate table is a serialized reporting snapshot. Admission uses indexed
rolling SUM queries: it does not rely on a stale cache or claim constant-time
work as the seven-day ledger grows.
"""

from __future__ import annotations

import logging
import math
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from mnemos.core.config import get_settings
from mnemos.domain.knemon.router import (
    _conn_from_tx,
    _connection_dialect,
    _rows,
    _to_float,
    _tx_execute,
    _tx_rows,
)

logger = logging.getLogger(__name__)


class BudgetVerdict(str, Enum):
    ALLOW = "allow"
    DENY = "deny"


class CostReservationConflict(RuntimeError):
    """Raised when reserve/settle/release encounters an unrecoverable state.

    Examples include a unique-violation on an attempt_id that is already
    in a terminal state (settled or released) when reserve is re-invoked
    with a non-zero estimate, or a schema mismatch that prevents durable
    persistence.
    """


class CostReservationSchemaMissing(RuntimeError):
    """Raised when the 0054 cost-reservation schema has not been migrated."""


@dataclass(frozen=True)
class BudgetDecision:
    verdict: BudgetVerdict
    remaining_usd: float
    reason: str
    spent_usd: float = 0.0
    limit_usd: float | None = None

    @property
    def allowed(self) -> bool:
        return self.verdict is BudgetVerdict.ALLOW


@dataclass(frozen=True)
class ReservationHandle:
    """Return shape for ``reserve_cost``.

    ``attempt_id`` is the caller's per-attempt identifier. The handle is
    the token PANTHEON's companion F05 fix should return from its
    admission call site and pass back into ``settle_cost`` /
    ``release_cost``. ``expires_at`` is the durable TTL after which a
    crashed caller's reservation is conservatively settled by recovery.
    """

    attempt_id: str
    provider: str
    model: str
    caller_subsystem: str
    estimated_cost_usd: float
    reserved_at: datetime
    expires_at: datetime


# ---------------------------------------------------------------------------
# Canonical budget key helper
# ---------------------------------------------------------------------------

# Canonical provider identity — collapses historical/duplicate labels so a
# single policy entry covers all of them (e.g. "claude" -> "anthropic").
# This is the SINGLE source of truth for provider canonicalization across the
# KNEMON <-> PANTHEON boundary; PANTHEON's F05 fix MUST consume this helper
# instead of reimplementing the alias table inline. See ``canonical_budget_key``
# below.
_PROVIDER_ALIASES: dict[str, str] = {
    "claude": "anthropic",
    "google": "gemini",
    "ngc": "nvidia",
    "eih": "nvidia",
    "ngc-eih": "nvidia",
}

# Sentinel row key for the read-path spend aggregate. SQLite does NOT
# enforce UNIQUE on NULL values, so a NULLable provider column on the
# aggregate's primary key would allow the unfiltered "all providers"
# row to be inserted an unbounded number of times (and the UPSERT
# would never match). Using a real string sentinel keeps the primary
# key unique. The Python code translates a Python ``None`` provider
# into this sentinel before talking to SQL.
_AGGREGATE_ALL_PROVIDERS_SENTINEL = "*"


def _canon_provider(name: str | None) -> str:
    """Lowercase + strip + alias-collapse a provider label.

    Empty / None input returns the empty string — callers downstream treat
    that as "no provider key" (legacy ``evaluate_usage_budget`` semantics).
    """
    canon = (name or "").strip().lower()
    return _PROVIDER_ALIASES.get(canon, canon)


def _canonical_caps(caps: dict[str, float]) -> dict[str, float]:
    result = {}
    for name, cap in caps.items():
        value, valid = _normalise_estimated_cost(cap)
        if valid and value > 0 and name.strip():
            key = _canon_provider(name)
            result[key] = min(result.get(key, value), value)
    return result


def _as_datetime(value: Any) -> datetime:
    parsed = (
        value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    )
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed


def _time_sql(conn: Any, value: str) -> str:
    # SQLite timestamps can contain T or space; lexical comparison is incorrect.
    return f"julianday({value})" if _connection_dialect(conn) == "sqlite" else value


def _aggregate_provider_key(provider: str | None) -> str:
    """Normalise a Python ``provider`` value for the aggregate's primary key.

    ``None`` becomes the ``'*'`` sentinel (see
    ``_AGGREGATE_ALL_PROVIDERS_SENTINEL``); any other value is
    lowercased + stripped, with empty strings collapsed to the
    sentinel as well so callers cannot accidentally create two
    different "unfiltered" rows.
    """
    if provider is None:
        return _AGGREGATE_ALL_PROVIDERS_SENTINEL
    stripped = provider.strip()
    if not stripped:
        return _AGGREGATE_ALL_PROVIDERS_SENTINEL
    return _canon_provider(provider)


def canonical_budget_key(provider: str | None, model: str | None) -> tuple[str, str]:
    """Return the canonical (provider, model) identity for budget keys.

    F05 (PANTHEON) MUST call this helper before persisting budget
    accounting rows or querying them — that way ``claude`` and
    ``anthropic`` collapse onto the same budget identity, and the same
    holds for any future alias additions. Model strings are
    normalised via ``(name or "").strip().lower()`` so case / whitespace
    variants never accidentally shadow each other.

    Returns ``(canon_provider, canon_model)``; either component can be
    the empty string if the caller passed nothing usable.
    """
    canon_provider = _canon_provider(provider)
    canon_model = (model or "").strip().lower()
    return canon_provider, canon_model


# ---------------------------------------------------------------------------
# Numeric / normalisation helpers
# ---------------------------------------------------------------------------


def _normalise_estimated_cost(value: Any) -> tuple[float, bool]:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0, False
    if not math.isfinite(parsed) or parsed < 0:
        return 0.0, False
    return parsed, True


# ---------------------------------------------------------------------------
# Cost-reservation TTL knobs (env-overridable)
# ---------------------------------------------------------------------------


def _cost_reservation_ttl_seconds() -> int:
    """How long a pending reservation lives before a stale cleanup releases it."""
    raw = os.getenv("MNEMOS_KNEMON_COST_RESERVATION_TTL_SECONDS", "")
    try:
        configured = int(raw) if raw.strip() else 300
    except (TypeError, ValueError):
        configured = 300
    return max(1, configured)


def _cost_reservation_retention_seconds() -> int:
    """How long settled / released reservation history is retained."""
    raw = os.getenv("MNEMOS_KNEMON_COST_RESERVATION_RETENTION_SECONDS", "")
    try:
        configured = int(raw) if raw.strip() else 7 * 24 * 60 * 60
    except (TypeError, ValueError):
        configured = 7 * 24 * 60 * 60
    return max(_cost_reservation_ttl_seconds(), configured)


# ---------------------------------------------------------------------------
# spend queries — pending reservation aware
# ---------------------------------------------------------------------------


async def _ensure_and_lock_reservation_lock(
    conn: Any, caller_subsystem: str, now: datetime
) -> None:
    """Acquire a per-caller_subsystem row lock for atomic reservation math.

    Mirrors the pattern in ``_ensure_and_lock_burn_session`` —
    INSERT-or-ignore then ``SELECT ... FOR UPDATE`` (or equivalent on
    non-PostgreSQL dialects). Concurrent ``reserve_cost`` calls from
    the same caller_subsystem serialize on this row, while different
    subsystems remain independent.
    """
    dialect = _connection_dialect(conn)
    params = {"caller_subsystem": caller_subsystem, "now_ts": now}
    if dialect == "postgres":
        insert_sql = """
            INSERT INTO knemon_cost_reservation_locks (caller_subsystem, touched_at)
            VALUES (:caller_subsystem, :now_ts)
            ON CONFLICT (caller_subsystem) DO NOTHING
            """
    elif dialect == "mysql":
        insert_sql = """
            INSERT INTO knemon_cost_reservation_locks (caller_subsystem, touched_at)
            VALUES (:caller_subsystem, :now_ts)
            ON DUPLICATE KEY UPDATE touched_at = knemon_cost_reservation_locks.touched_at
            """
    elif dialect == "sqlite":
        insert_sql = """
            INSERT INTO knemon_cost_reservation_locks (caller_subsystem, touched_at)
            VALUES (:caller_subsystem, :now_ts)
            ON CONFLICT (caller_subsystem) DO NOTHING
            """
    elif dialect == "oracle":
        insert_sql = """
            MERGE INTO knemon_cost_reservation_locks target
            USING (SELECT :caller_subsystem AS caller_subsystem, :now_ts AS touched_at FROM dual) source
            ON (target.caller_subsystem = source.caller_subsystem)
            WHEN NOT MATCHED THEN
              INSERT (caller_subsystem, touched_at) VALUES (source.caller_subsystem, source.touched_at)
            """
    elif dialect == "db2":
        insert_sql = """
            MERGE INTO knemon_cost_reservation_locks AS target
            USING (VALUES (:caller_subsystem, :now_ts)) AS source(caller_subsystem, touched_at)
            ON target.caller_subsystem = source.caller_subsystem
            WHEN NOT MATCHED THEN
              INSERT (caller_subsystem, touched_at) VALUES (source.caller_subsystem, source.touched_at)
            """
    else:
        insert_sql = """
            INSERT INTO knemon_cost_reservation_locks (caller_subsystem, touched_at)
            VALUES (:caller_subsystem, :now_ts)
            """
    try:
        await _tx_execute(conn, insert_sql, params)
    except Exception as exc:
        if dialect not in {"oracle", "db2"} or not _is_unique_violation(exc):
            raise
    lock_suffix = "" if dialect == "sqlite" else " FOR UPDATE"
    await _tx_rows(
        conn,
        "SELECT caller_subsystem FROM knemon_cost_reservation_locks WHERE caller_subsystem = :caller_subsystem"
        + lock_suffix,
        {"caller_subsystem": caller_subsystem},
    )
    await _tx_execute(
        conn,
        "UPDATE knemon_cost_reservation_locks SET touched_at = :now_ts WHERE caller_subsystem = :caller_subsystem",
        params,
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


async def weekly_spend_usd(
    backend: Any,
    *,
    caller_subsystem: str = "pantheon",
    provider: str | None = None,
    now: datetime | None = None,
) -> float:
    """Return rolling-seven-day committed spend from KNEMON's ledger.

    When ``provider`` is given the spend is scoped to that single provider (the
    ledger ``provider`` column is matched case-insensitively); otherwise it is
    the total across all providers for ``caller_subsystem``.

    NOTE (F04): this returns HISTORICAL committed spend only. To enforce
    admission against the live (committed + pending-reserved) budget, call
    ``effective_spend_usd`` instead. ``evaluate_usage_budget`` already uses
    ``effective_spend_usd`` internally, so this function is preserved for
    callers (audit / dashboards) that explicitly want committed-only math.
    """
    since = (now or datetime.now(timezone.utc)) - timedelta(days=7)
    params: dict[str, Any] = {"caller_subsystem": caller_subsystem, "since_ts": since}
    provider_clause = ""
    if provider:
        provider_clause = "\n          AND LOWER(provider) = :provider"
        params["provider"] = provider.strip().lower()
    rows = await _rows(
        backend,
        f"""
        SELECT COALESCE(SUM(est_cost_usd), 0) AS spent_usd
        FROM usage_ledger
        WHERE caller_subsystem = :caller_subsystem
          AND ts >= :since_ts{provider_clause}
        """,
        params,
    )
    return _to_float((rows[0] if rows else {}).get("spent_usd"), 0.0)


async def provider_spend_usd(
    backend: Any,
    *,
    provider: str,
    caller_subsystem: str = "pantheon",
    now: datetime | None = None,
) -> float:
    """Rolling-seven-day committed spend for a single ``provider``.

    NOTE (F04): see ``weekly_spend_usd`` for the committed-vs-effective
    distinction.
    """
    return await weekly_spend_usd(
        backend, caller_subsystem=caller_subsystem, provider=provider, now=now
    )


async def pending_reservations_usd(
    backend: Any,
    *,
    caller_subsystem: str = "pantheon",
    provider: str | None = None,
    now: datetime | None = None,
) -> float:
    """Return the sum of outstanding (unsettled, unexpired) reservations.

    F04: spend checks for admission must include these — otherwise
    concurrent requests can all be admitted against the same remaining
    headroom. Settled reservations do NOT count (they are already
    reflected in the ledger), and released / expired reservations do
    NOT count (their headroom has been returned).
    """
    current = now or datetime.now(timezone.utc)
    params: dict[str, Any] = {"caller_subsystem": caller_subsystem, "now_ts": current}
    provider_clause = ""
    if provider:
        params["provider"] = provider.strip().lower()
        provider_clause = "AND LOWER(provider) = :provider"
    rows = await _rows(
        backend,
        f"""
        SELECT COALESCE(SUM(estimated_cost_usd), 0) AS pending_usd
        FROM knemon_cost_reservations
        WHERE status = 'pending'
          AND caller_subsystem = :caller_subsystem
          AND expires_at > :now_ts
          {provider_clause}
        """,
        params,
    )
    return _to_float((rows[0] if rows else {}).get("pending_usd"), 0.0)


async def effective_spend_usd(
    backend: Any,
    *,
    caller_subsystem: str = "pantheon",
    provider: str | None = None,
    now: datetime | None = None,
) -> float:
    """Rolling-seven-day spend + outstanding pending reservations.

    F04: admission checks must use this (NOT plain ``weekly_spend_usd``)
    so a fleet of processes cannot all admit against the same remaining
    headroom before any of them has actually recorded the spend.
    """
    from mnemos.domain.knemon.migrations import ensure_knemon_schema

    await ensure_knemon_schema(backend)
    async with backend.transactional() as tx:
        return await effective_spend_usd_tx(
            _conn_from_tx(tx),
            caller_subsystem=caller_subsystem,
            provider=provider,
            now=now,
        )


def _evaluate_single_cap(
    label: str,
    *,
    limit_usd: float,
    spent_usd: float,
    estimated_cost_usd: float,
) -> BudgetDecision:
    """Decide ALLOW/DENY for ONE cap given its spend. Pure/synchronous.

    ``label == "budget"`` preserves the original global-cap reason strings for
    backward compatibility; any other label (e.g. ``provider:openai``) is woven
    into the reason so callers can see which cap bit.
    """
    remaining = max(0.0, limit_usd - spent_usd)
    if spent_usd >= limit_usd:
        reason = (
            f"budget exhausted ({spent_usd:.4f}/{limit_usd:.4f})"
            if label == "budget"
            else f"{label} budget exhausted ({spent_usd:.4f}/{limit_usd:.4f})"
        )
        return BudgetDecision(
            BudgetVerdict.DENY,
            remaining,
            reason,
            spent_usd=spent_usd,
            limit_usd=limit_usd,
        )
    if estimated_cost_usd > 0 and (spent_usd + estimated_cost_usd) > limit_usd:
        reason = (
            f"estimated ${estimated_cost_usd:.4f} would exceed remaining ${remaining:.4f}"
            if label == "budget"
            else f"estimated ${estimated_cost_usd:.4f} would exceed {label} remaining ${remaining:.4f}"
        )
        return BudgetDecision(
            BudgetVerdict.DENY,
            remaining,
            reason,
            spent_usd=spent_usd,
            limit_usd=limit_usd,
        )
    within = "within budget" if label == "budget" else f"within {label} budget"
    return BudgetDecision(
        BudgetVerdict.ALLOW, remaining, within, spent_usd=spent_usd, limit_usd=limit_usd
    )


async def evaluate_usage_budget(
    backend: Any,
    *,
    estimated_cost_usd: float = 0.0,
    limit_usd: float | None = None,
    caller_subsystem: str = "pantheon",
    provider: str | None = None,
    provider_caps: dict[str, float] | None = None,
    now: datetime | None = None,
) -> BudgetDecision:
    """Allow iff ledger spend plus this request stays under EVERY applicable cap.

    F04: this decision is ADVISORY ONLY. Callers that want to durably
    reserve the requested headroom MUST also call ``reserve_cost`` (with
    a stable per-attempt id) before the provider call, and either
    ``settle_cost`` (success path) or ``release_cost`` (abort path)
    afterwards. ``evaluate_usage_budget`` includes PENDING reservations
    in its headroom math via ``effective_spend_usd`` so callers get a
    truthful "is this attempt going to fit?" answer without separately
    re-querying.

    KNEMON is the single owner of spend math: callers supply only an
    optional request-cost estimate, the ledger-backed backend, and
    (optionally) the ``provider`` the request will hit. Two caps are
    enforced when configured:

    * the global weekly cap (``limit_usd`` /
      ``MNEMOS_KNEMON_WEEKLY_BUDGET_CAP_USD``) summed across all
      providers, and
    * a per-provider weekly cap (``provider_caps`` /
      ``MNEMOS_KNEMON_PROVIDER_BUDGET_CAPS_USD``) scoped to ``provider``.

    A request is DENIED if EITHER cap would be exceeded; when both
    ALLOW, the decision reports the tightest remaining headroom. If no
    cap is configured the decision is unlimited.

    SECURITY (review #8): once ANY cap is configured, enforcement FAILS
    CLOSED — a missing/unavailable ledger backend or a failed spend
    query DENIES rather than granting unlimited spend. Deployments
    that do not want enforcement leave the caps unset (the unlimited
    ALLOW path below).
    """
    estimated_cost_usd, estimate_valid = _normalise_estimated_cost(estimated_cost_usd)

    if limit_usd is None:
        configured = float(get_settings().knemon.weekly_budget_cap_usd)
        limit_usd = configured if configured > 0 else None

    canon_provider, canon_model = canonical_budget_key(provider, None)
    provider_key = canon_provider  # alias-collapsed

    if provider_caps is None:
        provider_caps = get_settings().knemon.parsed_provider_budget_caps_usd()
    configured_provider_caps = _canonical_caps(provider_caps)
    if configured_provider_caps and not provider_key:
        return BudgetDecision(
            BudgetVerdict.DENY,
            0.0,
            "provider is required when provider budget caps are configured; failing closed",
            limit_usd=limit_usd,
        )
    provider_cap: float | None = None
    if provider_key:
        candidate = configured_provider_caps.get(provider_key)
        if candidate is not None and candidate > 0:
            provider_cap = float(candidate)

    if limit_usd is None and provider_cap is None:
        return BudgetDecision(
            BudgetVerdict.ALLOW, float("inf"), "no limit", limit_usd=None
        )

    if not estimate_valid:
        return BudgetDecision(
            BudgetVerdict.DENY,
            0.0,
            "invalid estimated cost; failing closed under configured cap",
            limit_usd=limit_usd if limit_usd is not None else provider_cap,
        )

    # Any configured cap requires a working ledger; otherwise fail closed.
    if backend is None:
        return BudgetDecision(
            BudgetVerdict.DENY,
            0.0,
            "budget ledger unavailable; failing closed under configured cap",
            limit_usd=limit_usd if limit_usd is not None else provider_cap,
        )

    allowed_decisions: list[BudgetDecision] = []

    if limit_usd is not None:
        try:
            # F04: include pending reservations so admission isn't a
            # read-then-decide race across the fleet.
            spent_usd = await effective_spend_usd(
                backend, caller_subsystem=caller_subsystem, now=now
            )
        except Exception as exc:
            logger.warning(
                "KNEMON budget ledger query failed; denying under configured cap: %s",
                exc,
            )
            return BudgetDecision(
                BudgetVerdict.DENY,
                0.0,
                "budget ledger query failed; failing closed under configured cap",
                limit_usd=limit_usd,
            )
        decision = _evaluate_single_cap(
            "budget",
            limit_usd=limit_usd,
            spent_usd=spent_usd,
            estimated_cost_usd=estimated_cost_usd,
        )
        if not decision.allowed:
            return decision
        allowed_decisions.append(decision)

    if provider_cap is not None:
        label = f"provider:{provider_key}"
        try:
            spent_provider = await effective_spend_usd(
                backend,
                provider=provider_key,
                caller_subsystem=caller_subsystem,
                now=now,
            )
        except Exception as exc:
            logger.warning(
                "KNEMON %s ledger query failed; denying under configured cap: %s",
                label,
                exc,
            )
            return BudgetDecision(
                BudgetVerdict.DENY,
                0.0,
                f"{label} ledger query failed; failing closed under configured cap",
                limit_usd=provider_cap,
            )
        decision = _evaluate_single_cap(
            label,
            limit_usd=provider_cap,
            spent_usd=spent_provider,
            estimated_cost_usd=estimated_cost_usd,
        )
        if not decision.allowed:
            return decision
        allowed_decisions.append(decision)

    # Every configured cap allowed: report the tightest remaining headroom.
    return min(allowed_decisions, key=lambda d: d.remaining_usd)


# ---------------------------------------------------------------------------
# F04: durable cost reservation / settlement / release
# ---------------------------------------------------------------------------


async def reserve_cost(
    backend: Any,
    *,
    provider: str | None,
    model: str | None,
    estimated_cost_usd: float,
    attempt_id: str | None = None,
    caller_subsystem: str = "pantheon",
    ttl_seconds: int | None = None,
    limit_usd: float | None = None,
    provider_caps: dict[str, float] | None = None,
    now: datetime | None = None,
) -> ReservationHandle:
    """Atomically reserve ``estimated_cost_usd`` of budget headroom.

    F04 review contract:

    * ``attempt_id`` MUST be a per-attempt identifier that uniquely
      identifies the call. If not supplied, KNEMON mints a fresh UUID
      (callers are expected to supply their own id for retry-idempotency).
    * The function atomically checks committed spend + outstanding
      pending reservations against the configured caps (global and
      per-provider), and if room exists, durably inserts a pending
      reservation row before returning.
    * Raises ``CostReservationConflict`` if a reservation already
      exists for ``attempt_id`` — callers should re-read via
      ``evaluate_usage_budget`` rather than racing.
    * Raises ``CostReservationSchemaMissing`` if migration 0054 has
      not been applied.
    * Raises ``BudgetVerdict.DENY`` equivalent (``CostReservationConflict``
      with reason) if the cap would be exceeded.
    """
    canon_provider, canon_model = canonical_budget_key(provider, model)
    if not attempt_id:
        attempt_id = str(uuid.uuid4())
    estimated_cost_usd, estimate_valid = _normalise_estimated_cost(estimated_cost_usd)
    if not estimate_valid:
        raise CostReservationConflict(
            f"invalid estimated cost {estimated_cost_usd!r} for attempt_id={attempt_id}"
        )

    current = now or datetime.now(timezone.utc)
    ttl = (
        int(ttl_seconds) if ttl_seconds is not None else _cost_reservation_ttl_seconds()
    )
    expires_at = current + timedelta(seconds=max(1, ttl))

    # Best-effort migration trigger; raises loudly if it can't apply.
    try:
        from mnemos.domain.knemon.migrations import ensure_knemon_schema

        await ensure_knemon_schema(backend)
    except Exception as exc:
        raise CostReservationSchemaMissing(
            f"KNEMON cost-reservation schema migration 0054 not applied: {exc}"
        ) from exc

    # Resolve caps.
    if limit_usd is None:
        configured = float(get_settings().knemon.weekly_budget_cap_usd)
        limit_usd = configured if configured > 0 else None
    if provider_caps is None:
        provider_caps = get_settings().knemon.parsed_provider_budget_caps_usd()
    configured_provider_caps = _canonical_caps(provider_caps)
    provider_cap = configured_provider_caps.get(canon_provider)

    async with backend.transactional() as tx:
        conn = _conn_from_tx(tx)
        # Lock the caller_subsystem row so concurrent reserves serialize.
        await _ensure_and_lock_reservation_lock(conn, caller_subsystem, current)

        # If the attempt_id already exists, treat the call as a no-op
        # idempotent re-entry — return the existing handle. This makes
        # reserve_cost safe for retry-aware callers (PANTHEON F05).
        existing = await _tx_rows(
            conn,
            """
            SELECT attempt_id, provider, model, caller_subsystem, estimated_cost_usd,
                   reserved_at, expires_at, status
            FROM knemon_cost_reservations
            WHERE attempt_id = :attempt_id
            """,
            {"attempt_id": attempt_id},
        )
        if existing:
            row = existing[0]
            # Already-settled / released reservations cannot be "re-reserved" — the
            # original attempt was already finalized. Surface this loudly so the
            # caller doesn't accidentally double-book.
            status = str(row.get("status") or "")
            if status != "pending":
                raise CostReservationConflict(
                    f"attempt_id={attempt_id} already finalized as {status!r}; "
                    f"reserve_cost is not idempotent across terminal states"
                )
            reserved_at = _as_datetime(row["reserved_at"])
            existing_expires = _as_datetime(row["expires_at"])
            if (
                str(row["provider"]),
                str(row["model"]),
                str(row["caller_subsystem"]),
                float(row["estimated_cost_usd"]),
            ) != (
                canon_provider,
                canon_model,
                caller_subsystem,
                estimated_cost_usd,
            ) or existing_expires <= current:
                raise CostReservationConflict(
                    f"attempt_id={attempt_id} payload changed or expired"
                )
            return ReservationHandle(
                attempt_id,
                canon_provider,
                canon_model,
                caller_subsystem,
                estimated_cost_usd,
                reserved_at,
                existing_expires,
            )

        # Sum committed + pending (effective spend) so concurrent reserves
        # for the same caller_subsystem cannot race past the cap.
        if limit_usd is not None or provider_cap is not None:
            effective_global = await effective_spend_usd_tx(
                conn, caller_subsystem=caller_subsystem, now=current
            )
            if (
                limit_usd is not None
                and (effective_global + estimated_cost_usd) > limit_usd
            ):
                raise CostReservationConflict(
                    f"attempt_id={attempt_id} would exceed global cap "
                    f"{limit_usd:.4f} (committed+pending+estimate={effective_global + estimated_cost_usd:.4f})"
                )
            if provider_cap is not None:
                effective_provider = await effective_spend_usd_tx(
                    conn,
                    caller_subsystem=caller_subsystem,
                    provider=canon_provider,
                    now=current,
                )
                if (effective_provider + estimated_cost_usd) > provider_cap:
                    raise CostReservationConflict(
                        f"attempt_id={attempt_id} would exceed provider:{canon_provider} cap "
                        f"{provider_cap:.4f} (committed+pending+estimate={effective_provider + estimated_cost_usd:.4f})"
                    )

        # Persist the reservation row.
        try:
            await _tx_execute(
                conn,
                """
                INSERT INTO knemon_cost_reservations (
                    attempt_id, provider, model, caller_subsystem,
                    estimated_cost_usd, reserved_at, expires_at, status
                ) VALUES (
                    :attempt_id, :provider, :model, :caller_subsystem,
                    :estimated_cost_usd, :reserved_at, :expires_at, 'pending'
                )
                """,
                {
                    "attempt_id": attempt_id,
                    "provider": canon_provider,
                    "model": canon_model,
                    "caller_subsystem": caller_subsystem,
                    "estimated_cost_usd": estimated_cost_usd,
                    "reserved_at": current,
                    "expires_at": expires_at,
                },
            )
        except Exception as exc:
            # Unique-violation on attempt_id PK means another concurrent caller
            # just raced us to insert. Surface as a conflict rather than letting
            # the suppressed-write behavior mask it (the bug F04 is fixing).
            if _is_unique_violation(exc):
                raise CostReservationConflict(
                    f"attempt_id={attempt_id} reservation already exists (concurrent insert race)"
                ) from exc
            raise

        # Maintain the read-path spend aggregate in the SAME
        # transaction as the reservation INSERT so a concurrent
        # ``effective_spend_usd_aggregate`` reader either sees the
        # pre-reservation counter or the post-reservation counter,
        # never a partial state. Both granularities (provider-scoped
        # and unfiltered) are bumped.
        await _upsert_spend_aggregate_row(
            conn,
            caller_subsystem=caller_subsystem,
            provider=canon_provider,
            pending_delta=estimated_cost_usd,
            window_start_ts=_spend_aggregate_window_start(current),
            now=current,
        )
        await _upsert_spend_aggregate_row(
            conn,
            caller_subsystem=caller_subsystem,
            provider=_AGGREGATE_ALL_PROVIDERS_SENTINEL,
            pending_delta=estimated_cost_usd,
            window_start_ts=_spend_aggregate_window_start(current),
            now=current,
        )

    return ReservationHandle(
        attempt_id=attempt_id,
        provider=canon_provider,
        model=canon_model,
        caller_subsystem=caller_subsystem,
        estimated_cost_usd=estimated_cost_usd,
        reserved_at=current,
        expires_at=expires_at,
    )


async def _spend_components_tx(
    conn: Any,
    *,
    caller_subsystem: str,
    provider: str | None,
    now: datetime,
    since: datetime,
) -> tuple[float, float]:
    provider_params: dict[str, Any] = {}
    clause = ""
    if provider and provider != "*":
        canonical = _canon_provider(provider)
        names = sorted(
            {canonical, *(k for k, v in _PROVIDER_ALIASES.items() if v == canonical)}
        )
        binds = []
        for i, name in enumerate(names):
            provider_params[f"provider_{i}"] = name
            binds.append(f":provider_{i}")
        clause = "AND LOWER(provider) IN (" + ", ".join(binds) + ")"
    # Each statement gets exactly the binds its own SQL text references. The
    # three queries below do NOT share a bind set: only the first two are
    # window-scoped (:since_ts), and none of them reference the caller's
    # ``now``. Oracle rejects a supplied bind the statement does not contain
    # (DPY-4008) where every other supported driver ignores it, so a shared
    # dict fails on Oracle alone -- and because the budget gate fails closed,
    # that denied every GRAEAE consultation including free providers.
    windowed_params: dict[str, Any] = {
        "caller_subsystem": caller_subsystem,
        "since_ts": since,
        **provider_params,
    }
    pending_params: dict[str, Any] = {
        "caller_subsystem": caller_subsystem,
        **provider_params,
    }
    ledger = await _tx_rows(
        conn,
        f"""
        SELECT COALESCE(SUM(est_cost_usd), 0) AS amount FROM usage_ledger
        WHERE caller_subsystem = :caller_subsystem AND {_time_sql(conn, "ts")} >= {_time_sql(conn, ":since_ts")} {clause}
        """,
        windowed_params,
    )
    settled = await _tx_rows(
        conn,
        f"""
        SELECT COALESCE(SUM(actual_cost_usd), 0) AS amount FROM knemon_cost_reservations r
        WHERE status = 'settled' AND caller_subsystem = :caller_subsystem
          AND {_time_sql(conn, "settled_at")} >= {_time_sql(conn, ":since_ts")}
          AND (ledger_id IS NULL OR NOT EXISTS (SELECT 1 FROM usage_ledger l WHERE l.id = r.ledger_id)) {clause}
        """,
        windowed_params,
    )
    pending = await _tx_rows(
        conn,
        f"""
        SELECT COALESCE(SUM(estimated_cost_usd), 0) AS amount FROM knemon_cost_reservations
        WHERE status = 'pending' AND caller_subsystem = :caller_subsystem {clause}
        """,
        pending_params,
    )
    # Expiry is not proof an upstream request was free. Recovery settles conservatively.
    return float(ledger[0]["amount"]) + float(settled[0]["amount"]), float(
        pending[0]["amount"]
    )


async def effective_spend_usd_tx(
    conn: Any,
    *,
    caller_subsystem: str,
    provider: str | None = None,
    now: datetime | None = None,
) -> float:
    current = now or datetime.now(timezone.utc)
    committed, pending = await _spend_components_tx(
        conn,
        caller_subsystem=caller_subsystem,
        provider=provider,
        now=current,
        since=current - timedelta(days=7),
    )
    return committed + pending


async def _locked_attempt(conn: Any, attempt_id: str, current: datetime) -> dict | None:
    rows = await _tx_rows(
        conn,
        "SELECT caller_subsystem FROM knemon_cost_reservations WHERE attempt_id = :id",
        {"id": attempt_id},
    )
    if not rows:
        return None
    await _ensure_and_lock_reservation_lock(conn, rows[0]["caller_subsystem"], current)
    rows = await _tx_rows(
        conn,
        "SELECT * FROM knemon_cost_reservations WHERE attempt_id = :id",
        {"id": attempt_id},
    )
    return rows[0] if rows else None


async def _refresh_attempt_scopes(conn: Any, row: dict, now: datetime) -> None:
    for provider in (row["provider"], None):
        await _reconcile_spend_aggregates_tx(
            conn, caller_subsystem=row["caller_subsystem"], provider=provider, now=now
        )


async def settle_cost(
    backend: Any,
    *,
    attempt_id: str,
    actual_cost_usd: float,
    ledger_id: int | None = None,
    now: datetime | None = None,
) -> None:
    """Persist billable outcomes, serializing admission and deduplicating ledger linkage."""
    async with backend.transactional() as tx:
        await settle_cost_in_transaction(
            tx,
            attempt_id=attempt_id,
            actual_cost_usd=actual_cost_usd,
            ledger_id=ledger_id,
            now=now,
        )


async def settle_cost_in_transaction(
    tx: Any,
    *,
    attempt_id: str,
    actual_cost_usd: float,
    ledger_id: int | None = None,
    now: datetime | None = None,
) -> None:
    """Settle in the same transaction as a usage-ledger append (crash atomic)."""
    amount, valid = _normalise_estimated_cost(actual_cost_usd)
    if not valid:
        raise CostReservationConflict("invalid actual_cost_usd")
    current = now or datetime.now(timezone.utc)
    conn = _conn_from_tx(tx)
    row = await _locked_attempt(conn, attempt_id, current)
    if row is None:
        raise CostReservationConflict(f"unknown attempt_id={attempt_id}")
    recovery = await _tx_rows(
        conn,
        "SELECT attempt_id FROM knemon_cost_recovery WHERE attempt_id = :id",
        {"id": attempt_id},
    )
    if row["status"] == "settled" and not recovery:
        if float(row["actual_cost_usd"]) != amount:
            raise CostReservationConflict(
                f"attempt_id={attempt_id} already settled at different cost"
            )
        if ledger_id is None or row["ledger_id"] == ledger_id:
            return
        if row["ledger_id"] is not None:
            raise CostReservationConflict("reservation linked to different ledger row")
    if ledger_id is not None:
        linked = await _tx_rows(
            conn, "SELECT * FROM usage_ledger WHERE id = :id", {"id": ledger_id}
        )
        if (
            not linked
            or linked[0]["caller_subsystem"] != row["caller_subsystem"]
            or _canon_provider(linked[0]["provider"]) != row["provider"]
            or abs(float(linked[0]["est_cost_usd"]) - amount) > 1e-8
        ):
            raise CostReservationConflict(
                "ledger linkage does not match reservation cost and scope"
            )
    # A late bill remains billable even after explicit release raced completion.
    await _tx_execute(
        conn,
        """UPDATE knemon_cost_reservations SET status = 'settled', settled_at = :now_ts, actual_cost_usd = :amount, ledger_id = :ledger_id WHERE attempt_id = :id""",
        {"now_ts": current, "amount": amount, "ledger_id": ledger_id, "id": attempt_id},
    )
    await _tx_execute(
        conn,
        "DELETE FROM knemon_cost_recovery WHERE attempt_id = :id",
        {"id": attempt_id},
    )
    await _refresh_attempt_scopes(conn, row, current)


async def release_cost(
    backend: Any, *, attempt_id: str, now: datetime | None = None
) -> None:
    """Release only attempts known not to have incurred upstream work."""
    current = now or datetime.now(timezone.utc)
    async with backend.transactional() as tx:
        conn = _conn_from_tx(tx)
        row = await _locked_attempt(conn, attempt_id, current)
        if row is None or row["status"] != "pending":
            return
        await _tx_execute(
            conn,
            "UPDATE knemon_cost_reservations SET status = 'released', released_at = :now_ts WHERE attempt_id = :id",
            {"now_ts": current, "id": attempt_id},
        )
        await _refresh_attempt_scopes(conn, row, current)


async def cleanup_cost_reservations(
    backend: Any, *, now: datetime | None = None
) -> None:
    """Charge expired unknown attempts conservatively and retain a full budget window."""
    current = now or datetime.now(timezone.utc)
    cutoff = current - timedelta(
        seconds=max(7 * 86400, _cost_reservation_retention_seconds())
    )
    async with backend.transactional() as tx:
        conn = _conn_from_tx(tx)
        scopes = await _tx_rows(
            conn,
            "SELECT DISTINCT caller_subsystem FROM knemon_cost_reservations ORDER BY caller_subsystem",
        )
        for scope in scopes:
            await _ensure_and_lock_reservation_lock(
                conn, scope["caller_subsystem"], current
            )
            from mnemos.domain.knemon.router import _row_limited_sql

            expired = await _tx_rows(
                conn,
                _row_limited_sql(
                    f"""SELECT attempt_id FROM knemon_cost_reservations
                WHERE caller_subsystem = :scope AND status = 'pending'
                  AND {_time_sql(conn, "expires_at")} <= {_time_sql(conn, ":now_ts")}
                ORDER BY expires_at""",
                    _connection_dialect(conn),
                    500,
                ),
                {"scope": scope["caller_subsystem"], "now_ts": current},
            )
            for expired_row in expired:
                # Same rule as _spend_components_tx: per-statement binds. The
                # INSERT references only :id, so handing it :now_ts as well is
                # a DPY-4008 on Oracle.
                await _tx_execute(
                    conn,
                    "INSERT INTO knemon_cost_recovery(attempt_id) VALUES (:id)",
                    {"id": expired_row["attempt_id"]},
                )
                await _tx_execute(
                    conn,
                    """UPDATE knemon_cost_reservations
                    SET status = 'settled', settled_at = :now_ts, actual_cost_usd = estimated_cost_usd
                    WHERE attempt_id = :id""",
                    {"id": expired_row["attempt_id"], "now_ts": current},
                )
            await _tx_execute(
                conn,
                f"""DELETE FROM knemon_cost_reservations WHERE caller_subsystem = :scope AND status IN ('settled', 'released') AND {_time_sql(conn, "COALESCE(settled_at, released_at, reserved_at)")} < {_time_sql(conn, ":cutoff")}""",
                {"scope": scope["caller_subsystem"], "cutoff": cutoff},
            )
            await _tx_execute(
                conn,
                """DELETE FROM knemon_cost_recovery
                WHERE NOT EXISTS (SELECT 1 FROM knemon_cost_reservations r WHERE r.attempt_id = knemon_cost_recovery.attempt_id)""",
            )
            providers = await _tx_rows(
                conn,
                "SELECT provider FROM knemon_spend_window_aggregates WHERE caller_subsystem = :scope",
                {"scope": scope["caller_subsystem"]},
            )
            for row in providers:
                await _reconcile_spend_aggregates_tx(
                    conn,
                    caller_subsystem=scope["caller_subsystem"],
                    provider=row["provider"],
                    now=current,
                )


# ---------------------------------------------------------------------------
# Reporting snapshots. Mutable/legacy ledger writers and rolling expiry require
# authoritative reconciliation; these rows are not a money-admission shortcut.
# ---------------------------------------------------------------------------


def _spend_aggregate_window_seconds() -> int:
    """How far back the rolling-window aggregate looks.

    Defaults to 7 days — the same window the existing
    ``weekly_spend_usd`` enforces. Tests can override this via the
    ``MNEMOS_KNEMON_SPEND_AGGREGATE_WINDOW_SECONDS`` env var so the
    regression test can simulate window rollover without waiting a
    real week.
    """
    raw = os.getenv("MNEMOS_KNEMON_SPEND_AGGREGATE_WINDOW_SECONDS", "")
    try:
        configured = int(raw) if raw.strip() else 7 * 24 * 60 * 60
    except (TypeError, ValueError):
        configured = 7 * 24 * 60 * 60
    return max(1, configured)


def _spend_aggregate_window_start(now: datetime) -> datetime:
    """Return the lower-bound timestamp for the current rolling window."""
    return now - timedelta(seconds=_spend_aggregate_window_seconds())


async def _upsert_spend_aggregate_row(
    conn: Any,
    *,
    caller_subsystem: str,
    provider: str | None,
    committed_delta: float = 0.0,
    pending_delta: float = 0.0,
    window_start_ts: datetime | None = None,
    reconciled: bool = False,
    now: datetime | None = None,
) -> None:
    """Incrementally update the ``knemon_spend_window_aggregates`` row.

    The aggregation lives at ``(caller_subsystem, provider)`` where
    ``provider IS NULL`` means "all providers". Both deltas are
    allowed to be negative on release / settle-rollback paths, but
    ``committed_usd`` and ``pending_usd`` cannot go below zero
    (enforced by a CHECK constraint on the migration; we floor at 0
    here for the same effect when running on backends that ignore
    CHECK).
    """
    current = now or datetime.now(timezone.utc)
    window_start = window_start_ts or _spend_aggregate_window_start(current)
    # PostgreSQL / SQLite: an UPSERT that adds the deltas. We avoid
    # UPSERTs entirely on dialects where they are awkward (Oracle /
    # Db2) by using a SELECT-FOR-UPDATE-then-INSERT-or-UPDATE pattern;
    # see the caller's wrapping ``async with backend.transactional()``
    # which holds the row lock for the duration of the work.
    dialect = _connection_dialect(conn)
    params: dict[str, Any] = {
        "caller_subsystem": caller_subsystem,
        "provider": provider,
        "window_start_ts": window_start,
        "committed_delta": committed_delta,
        "pending_delta": pending_delta,
        "last_updated_at": current,
    }
    if dialect in {"postgres", "sqlite"}:
        floor = "GREATEST" if dialect == "postgres" else "MAX"
        timestamp = (
            "CAST(:last_updated_at AS TIMESTAMPTZ)"
            if dialect == "postgres"
            else ":last_updated_at"
        )
        await _tx_execute(
            conn,
            f"""
            INSERT INTO knemon_spend_window_aggregates (
                caller_subsystem, provider, window_start_ts,
                committed_usd, pending_usd,
                last_reconciled_at, last_updated_at
            ) VALUES (
                :caller_subsystem, :provider, :window_start_ts,
                {floor}(0, :committed_delta), {floor}(0, :pending_delta),
                CASE WHEN :reconciled THEN {timestamp} ELSE NULL END,
                :last_updated_at
            )
            ON CONFLICT (caller_subsystem, provider) DO UPDATE
            SET committed_usd = {floor}(0, knemon_spend_window_aggregates.committed_usd + :committed_delta),
                pending_usd   = {floor}(0, knemon_spend_window_aggregates.pending_usd + :pending_delta),
                window_start_ts = :window_start_ts,
                last_reconciled_at = CASE
                    WHEN :reconciled THEN {timestamp}
                    ELSE knemon_spend_window_aggregates.last_reconciled_at
                END,
                last_updated_at = :last_updated_at
            """,
            {**params, "reconciled": reconciled},
        )
        return

    if dialect == "mysql" or dialect == "mariadb":
        await _tx_execute(
            conn,
            """
            INSERT INTO knemon_spend_window_aggregates (
                caller_subsystem, provider, window_start_ts,
                committed_usd, pending_usd,
                last_reconciled_at, last_updated_at
            ) VALUES (
                :caller_subsystem, :provider, :window_start_ts,
                GREATEST(0, :committed_delta), GREATEST(0, :pending_delta),
                CASE WHEN :reconciled THEN :last_updated_at ELSE NULL END,
                :last_updated_at
            )
            ON DUPLICATE KEY UPDATE
                committed_usd = GREATEST(0, knemon_spend_window_aggregates.committed_usd + :committed_delta),
                pending_usd   = GREATEST(0, knemon_spend_window_aggregates.pending_usd + :pending_delta),
                window_start_ts = :window_start_ts,
                last_reconciled_at = CASE
                    WHEN :reconciled THEN :last_updated_at
                    ELSE knemon_spend_window_aggregates.last_reconciled_at
                END,
                last_updated_at = :last_updated_at
            """,
            {**params, "reconciled": reconciled},
        )
        return

    # Oracle / Db2 fallback: select the row, then INSERT-or-UPDATE.
    existing = await _tx_rows(
        conn,
        """
        SELECT committed_usd, pending_usd, window_start_ts, last_reconciled_at
        FROM knemon_spend_window_aggregates
        WHERE caller_subsystem = :caller_subsystem
          AND provider = :provider
        """,
        {"caller_subsystem": caller_subsystem, "provider": provider},
    )
    if existing:
        row = existing[0]
        new_committed = max(
            0.0,
            _to_float(row.get("committed_usd"), 0.0) + committed_delta,
        )
        new_pending = max(0.0, _to_float(row.get("pending_usd"), 0.0) + pending_delta)
        reconciled_at_clause = (
            "last_reconciled_at = :last_updated_at"
            if reconciled
            else "last_reconciled_at = last_reconciled_at"
        )
        await _tx_execute(
            conn,
            f"""
            UPDATE knemon_spend_window_aggregates
            SET committed_usd = :committed_usd,
                pending_usd = :pending_usd,
                window_start_ts = :window_start_ts,
                {reconciled_at_clause},
                last_updated_at = :last_updated_at
            WHERE caller_subsystem = :caller_subsystem
              AND provider = :provider
            """,
            {
                "committed_usd": new_committed,
                "pending_usd": new_pending,
                "window_start_ts": window_start,
                "last_updated_at": current,
                "caller_subsystem": caller_subsystem,
                "provider": provider,
            },
        )
        return

    # No existing row — insert with the absolute delta (we know it's
    # an empty counter because reconcile would have created the row
    # first). ``committed_delta`` and ``pending_delta`` may be
    # negative on caller-side corrections; floor at 0.
    await _tx_execute(
        conn,
        """
        INSERT INTO knemon_spend_window_aggregates (
            caller_subsystem, provider, window_start_ts,
            committed_usd, pending_usd,
            last_reconciled_at, last_updated_at
        ) VALUES (
            :caller_subsystem, :provider, :window_start_ts,
            GREATEST(0, :committed_delta), GREATEST(0, :pending_delta),
            :last_reconciled_at_value, :last_updated_at
        )
        """,
        {
            "caller_subsystem": caller_subsystem,
            "provider": provider,
            "window_start_ts": window_start,
            "committed_delta": committed_delta,
            "pending_delta": pending_delta,
            "last_reconciled_at_value": current if reconciled else None,
            "last_updated_at": current,
        },
    )


async def effective_spend_usd_aggregate(
    backend: Any,
    *,
    caller_subsystem: str = "pantheon",
    provider: str | None = None,
    now: datetime | None = None,
    allow_reconcile: bool = True,
) -> float:
    """Read a reporting snapshot, reconciling when its rolling boundary changes.

    This function is not the admission authority. Legacy ledger writes can
    bypass aggregate maintenance; callers needing current enforceable spend
    use effective_spend_usd or the atomic reserve_cost operation.
    """
    current = now or datetime.now(timezone.utc)
    target_window_start = _spend_aggregate_window_start(current)
    provider_key = _aggregate_provider_key(provider)
    async with backend.transactional() as tx:
        conn = _conn_from_tx(tx)
        await _ensure_and_lock_reservation_lock(conn, caller_subsystem, current)
        rows = await _tx_rows(
            conn,
            """
            SELECT committed_usd, pending_usd, window_start_ts, last_reconciled_at
            FROM knemon_spend_window_aggregates
            WHERE caller_subsystem = :caller_subsystem
              AND provider = :provider
            """,
            {"caller_subsystem": caller_subsystem, "provider": provider_key},
        )
        if rows:
            row = rows[0]
            cached_window_start = row.get("window_start_ts")
            # Stale if the row's window_start_ts is older than the
            # lower bound we want for ``now``. NOTE: ``_to_float``
            # doesn't apply to datetimes, so check identity directly.
            if cached_window_start is not None and not _is_aggregate_window_stale(
                cached_window_start, target_window_start
            ):
                return float(
                    _to_float(row.get("committed_usd"), 0.0)
                    + _to_float(row.get("pending_usd"), 0.0)
                )

        if not allow_reconcile:
            raise RuntimeError(
                "spend aggregate missing or stale; authoritative reconciliation required"
            )

        # Stale (or missing): rebuild from scratch, write the row,
        # and return the fresh value.
        await _reconcile_spend_aggregates_tx(
            conn,
            caller_subsystem=caller_subsystem,
            provider=provider_key,
            now=current,
        )
        rows = await _tx_rows(
            conn,
            """
            SELECT committed_usd, pending_usd
            FROM knemon_spend_window_aggregates
            WHERE caller_subsystem = :caller_subsystem
              AND provider = :provider
            """,
            {"caller_subsystem": caller_subsystem, "provider": provider_key},
        )
        if not rows:
            return 0.0
        return float(
            _to_float(rows[0].get("committed_usd"), 0.0)
            + _to_float(rows[0].get("pending_usd"), 0.0)
        )


def _is_aggregate_window_stale(
    cached_window_start: Any, target_window_start: datetime
) -> bool:
    """Return True when the cached aggregate's window lower-bound is older than ``target``.

    The aggregate row is considered stale when its window starts
    BEFORE the target window's lower bound — i.e. it would still be
    counting spend that has rolled out of the live window.
    """
    if cached_window_start is None:
        return True
    if isinstance(cached_window_start, datetime):
        # Both must be timezone-aware for an apples-to-apples compare.
        cached = cached_window_start
        if cached.tzinfo is None:
            cached = cached.replace(tzinfo=timezone.utc)
        target = target_window_start
        if target.tzinfo is None:
            target = target.replace(tzinfo=timezone.utc)
        return cached < target
    # SQLite / Db2 return ISO-formatted strings; parse if we can.
    try:
        cached_dt = datetime.fromisoformat(str(cached_window_start))
    except ValueError:
        return True
    if cached_dt.tzinfo is None:
        cached_dt = cached_dt.replace(tzinfo=timezone.utc)
    target = target_window_start
    if target.tzinfo is None:
        target = target.replace(tzinfo=timezone.utc)
    return cached_dt < target


async def _reconcile_spend_aggregates_tx(
    conn: Any,
    *,
    caller_subsystem: str,
    provider: str | None,
    now: datetime,
) -> None:
    """Rebuild exactly the requested scope under its caller-subsystem lock."""
    window_start = _spend_aggregate_window_start(now)
    committed, pending = await _spend_components_tx(
        conn,
        caller_subsystem=caller_subsystem,
        provider=provider,
        now=now,
        since=window_start,
    )
    await _absorb_aggregate_absolute(
        conn,
        caller_subsystem=caller_subsystem,
        provider=_aggregate_provider_key(provider),
        committed_usd=committed,
        pending_usd=pending,
        window_start_ts=window_start,
        now=now,
    )


async def _absorb_aggregate_absolute(
    conn: Any,
    *,
    caller_subsystem: str,
    provider: str | None,
    committed_usd: float,
    pending_usd: float,
    window_start_ts: datetime,
    now: datetime,
) -> None:
    """Write the absolute counter value, leaving drift detection to the caller."""
    dialect = _connection_dialect(conn)
    if dialect in {"postgres", "sqlite"}:
        await _tx_execute(
            conn,
            """
            INSERT INTO knemon_spend_window_aggregates (
                caller_subsystem, provider, window_start_ts,
                committed_usd, pending_usd,
                last_reconciled_at, last_updated_at
            ) VALUES (
                :caller_subsystem, :provider, :window_start_ts,
                :committed_usd, :pending_usd,
                :now_ts, :now_ts
            )
            ON CONFLICT (caller_subsystem, provider) DO UPDATE
            SET committed_usd = :committed_usd,
                pending_usd = :pending_usd,
                window_start_ts = :window_start_ts,
                last_reconciled_at = :now_ts,
                last_updated_at = :now_ts
            """,
            {
                "caller_subsystem": caller_subsystem,
                "provider": provider,
                "window_start_ts": window_start_ts,
                "committed_usd": committed_usd,
                "pending_usd": pending_usd,
                "now_ts": now,
            },
        )
        return
    if dialect in {"mysql", "mariadb"}:
        await _tx_execute(
            conn,
            """
            INSERT INTO knemon_spend_window_aggregates (
                caller_subsystem, provider, window_start_ts,
                committed_usd, pending_usd,
                last_reconciled_at, last_updated_at
            ) VALUES (
                :caller_subsystem, :provider, :window_start_ts,
                :committed_usd, :pending_usd,
                :now_ts, :now_ts
            )
            ON DUPLICATE KEY UPDATE
                committed_usd = :committed_usd,
                pending_usd = :pending_usd,
                window_start_ts = :window_start_ts,
                last_reconciled_at = :now_ts,
                last_updated_at = :now_ts
            """,
            {
                "caller_subsystem": caller_subsystem,
                "provider": provider,
                "window_start_ts": window_start_ts,
                "committed_usd": committed_usd,
                "pending_usd": pending_usd,
                "now_ts": now,
            },
        )
        return
    # Oracle / Db2 fallback: delete-then-insert inside the caller's
    # transaction.
    await _tx_execute(
        conn,
        """
        DELETE FROM knemon_spend_window_aggregates
        WHERE caller_subsystem = :caller_subsystem
          AND provider = :provider
        """,
        {"caller_subsystem": caller_subsystem, "provider": provider},
    )
    await _tx_execute(
        conn,
        """
        INSERT INTO knemon_spend_window_aggregates (
            caller_subsystem, provider, window_start_ts,
            committed_usd, pending_usd,
            last_reconciled_at, last_updated_at
        ) VALUES (
            :caller_subsystem, :provider, :window_start_ts,
            :committed_usd, :pending_usd,
            :now_ts, :now_ts
        )
        """,
        {
            "caller_subsystem": caller_subsystem,
            "provider": provider,
            "window_start_ts": window_start_ts,
            "committed_usd": committed_usd,
            "pending_usd": pending_usd,
            "now_ts": now,
        },
    )


@dataclass(frozen=True)
class AggregateDriftReport:
    """Result of ``reconcile_spend_aggregates``.

    ``committed_drift_usd`` is the difference between the maintained
    counter and the fresh full-scan SUM for ``committed_usd``. A
    non-zero value means incremental updates have drifted (e.g. a
    reservation row was deleted out from under us, or the caller
    passed a write that didn't propagate to the counter). The
    reconciliation rebuilds the counter so the read path is correct
    going forward; this report surfaces what was wrong so an
    operator can investigate.
    """

    caller_subsystem: str
    provider: str | None
    committed_drift_usd: float
    pending_drift_usd: float
    committed_maintained_usd: float
    pending_maintained_usd: float
    committed_fresh_usd: float
    pending_fresh_usd: float
    window_start_ts: datetime

    @property
    def has_drift(self) -> bool:
        return (
            abs(self.committed_drift_usd) > 1e-9 or abs(self.pending_drift_usd) > 1e-9
        )


async def reconcile_spend_aggregates(
    backend: Any,
    *,
    caller_subsystem: str = "pantheon",
    provider: str | None = None,
    now: datetime | None = None,
    tolerance_usd: float = 1e-9,
) -> list[AggregateDriftReport]:
    """Rebuild the aggregate counter and surface any drift.

    The reconciliation pass is the safety net the F04 review
    demanded: a counter that silently drifts past the budget cap is
    worse than the original full-scan SUM because the operator has
    no signal anything went wrong. We rebuild the counter from a
    fresh full-scan SUM, compare to the previously maintained
    values, and report the drift so the operator can investigate.

    Parameters
    ----------
    backend
        Persistence backend. The reconciliation opens its own
        transaction so concurrent ``settle_cost`` / ``reserve_cost``
        calls cannot race the rebuild.
    caller_subsystem
        Caller identity to reconcile. The F04 review surfaced this as
        the natural granularity for per-caller budget caps.
    provider
        Optional provider filter. ``None`` reconciles the
        "all providers" row; a specific value reconciles the
        provider-scoped row only.
    now
        "Now" timestamp for the reconciliation window. ``None`` uses
        ``datetime.now(timezone.utc)``. Tests advance this to
        simulate window rollover.
    tolerance_usd
        Allowed rounding mismatch (default 1e-9 USD). Differences
        strictly greater than this surface as drift.

    Returns
    -------
    list[AggregateDriftReport]
        One entry per ``(caller_subsystem, provider)`` row that was
        rebuilt. ``report.has_drift`` is True when the maintained
        counter disagreed with the fresh SUM by more than the
        tolerance.
    """
    current = now or datetime.now(timezone.utc)
    window_start = _spend_aggregate_window_start(current)
    # The aggregate's primary key uses the ``'*'`` sentinel (see
    # ``_aggregate_provider_key``) for "all providers", NOT SQL NULL —
    # SQLite does not enforce UNIQUE on NULLs, and the rest of the
    # code (``effective_spend_usd_aggregate``, ``_upsert_spend_aggregate_row``)
    # is consistent on that sentinel. Using ``provider.strip().lower()``
    # here would silently miss the unfiltered row and report the
    # maintained counter as zero even when ``reserve_cost`` /
    # ``settle_cost`` had been incrementing it correctly.
    #
    # NOTE: ``provider_key`` is ONLY for the aggregate table. The fresh
    # SUM against ``usage_ledger`` / ``knemon_cost_reservations`` is
    # filtered by the original ``provider`` argument (None = sum
    # everything for the caller) — NOT by ``provider_key``, which would
    # compare against the literal sentinel string ``'*'`` and silently
    # match zero rows.
    provider_key = _aggregate_provider_key(provider)
    async with backend.transactional() as tx:
        conn = _conn_from_tx(tx)
        await _ensure_and_lock_reservation_lock(conn, caller_subsystem, current)

        # Snapshot what the maintained counter says BEFORE we rebuild,
        # so the drift report has a meaningful before/after pair.
        snapshot_rows = await _tx_rows(
            conn,
            """
            SELECT committed_usd, pending_usd, window_start_ts, last_reconciled_at
            FROM knemon_spend_window_aggregates
            WHERE caller_subsystem = :caller_subsystem
              AND provider = :provider
            """,
            {"caller_subsystem": caller_subsystem, "provider": provider_key},
        )
        if snapshot_rows:
            snapshot = snapshot_rows[0]
            maintained_committed = _to_float(snapshot.get("committed_usd"), 0.0)
            maintained_pending = _to_float(snapshot.get("pending_usd"), 0.0)
        else:
            maintained_committed = 0.0
            maintained_pending = 0.0

        fresh_committed, fresh_pending = await _spend_components_tx(
            conn,
            caller_subsystem=caller_subsystem,
            provider=provider,
            now=current,
            since=window_start,
        )
        await _absorb_aggregate_absolute(
            conn,
            caller_subsystem=caller_subsystem,
            provider=provider_key,
            committed_usd=fresh_committed,
            pending_usd=fresh_pending,
            window_start_ts=window_start,
            now=current,
        )

        drift = AggregateDriftReport(
            caller_subsystem=caller_subsystem,
            provider=provider,
            committed_drift_usd=fresh_committed - maintained_committed,
            pending_drift_usd=fresh_pending - maintained_pending,
            committed_maintained_usd=maintained_committed,
            pending_maintained_usd=maintained_pending,
            committed_fresh_usd=fresh_committed,
            pending_fresh_usd=fresh_pending,
            window_start_ts=window_start,
        )

    if drift.has_drift and tolerance_usd >= 0:
        if abs(drift.committed_drift_usd) > tolerance_usd:
            logger.warning(
                "knemon spend-aggregate drift: caller=%s provider=%s committed_drift=%s",
                caller_subsystem,
                provider,
                drift.committed_drift_usd,
            )
        if abs(drift.pending_drift_usd) > tolerance_usd:
            logger.warning(
                "knemon spend-aggregate drift: caller=%s provider=%s pending_drift=%s",
                caller_subsystem,
                provider,
                drift.pending_drift_usd,
            )

    return [drift]


__all__ = [
    "AggregateDriftReport",
    "BudgetDecision",
    "BudgetVerdict",
    "CostReservationConflict",
    "CostReservationSchemaMissing",
    "ReservationHandle",
    "canonical_budget_key",
    "cleanup_cost_reservations",
    "effective_spend_usd",
    "effective_spend_usd_aggregate",
    "evaluate_usage_budget",
    "pending_reservations_usd",
    "provider_spend_usd",
    "reconcile_spend_aggregates",
    "release_cost",
    "reserve_cost",
    "settle_cost",
    "weekly_spend_usd",
]
