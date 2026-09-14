"""KNEMON-owned usage budget decisions backed by ``usage_ledger``.

F04: provider budgets do not reserve money atomically
---------------------------------------------------

A COMPANION fix (F05) lands in PANTHEON at the same time, but KNEMON
is the authority for cost-reservation. This module adds:

* ``canonical_budget_key(provider, model)`` — the single canonical
  identity helper that PANTHEON's companion fix should call instead of
  reimplementing the alias table inline. KNEMON's existing provider
  alias table (``claude`` → ``anthropic``) lives here; PANTHEON is
  expected to consume this so a budget cap set under one name applies
  under any alias.
* ``reserve_cost(backend, *, provider, model, estimated_cost_usd,
  attempt_id, caller_subsystem, ttl_seconds)`` — durably records a
  pending reservation keyed by the caller's per-attempt id, atomically
  checking that committed spend PLUS outstanding pending reservations
  stay under the budget cap. The reservation row survives process /
  host restart, so the fleet-wide admission decision is no longer a
  read-then-decide race.
* ``settle_cost(backend, *, attempt_id, actual_cost_usd)`` —
  transitions a pending reservation to ``settled`` with the REAL
  post-call cost. Idempotent: settling the same ``attempt_id`` twice
  does not double-count.
* ``release_cost(backend, *, attempt_id)`` — returns the headroom for
  a pending reservation when the call is aborted before completion.
  Idempotent and safe to a no-op when called on an already-settled or
  already-released attempt_id.
* ``effective_spend_usd(backend, *, ...)`` — like ``weekly_spend_usd``
  but also includes pending reservations in the headroom math, so
  admission checks serialize across processes without a separate
  reservation step.

The ``evaluate_usage_budget`` decision path still raises loudly
rather than swallowing write failures (the prior behavior suppressed
ledger-write errors, which masked stale read-then-decide accounting
across multiple hosts).
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
    crashed caller's reservation is auto-released by the cleanup path.
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
_PROVIDER_ALIASES: dict[str, str] = {"claude": "anthropic"}


def _canon_provider(name: str | None) -> str:
    """Lowercase + strip + alias-collapse a provider label.

    Empty / None input returns the empty string — callers downstream treat
    that as "no provider key" (legacy ``evaluate_usage_budget`` semantics).
    """
    canon = (name or "").strip().lower()
    return _PROVIDER_ALIASES.get(canon, canon)


def canonical_budget_key(
    provider: str | None, model: str | None
) -> tuple[str, str]:
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


async def _ensure_and_lock_reservation_lock(conn: Any, caller_subsystem: str, now: datetime) -> None:
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
        "SELECT caller_subsystem FROM knemon_cost_reservation_locks WHERE caller_subsystem = :caller_subsystem" + lock_suffix,
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
    committed = await weekly_spend_usd(
        backend, caller_subsystem=caller_subsystem, provider=provider, now=now
    )
    pending = await pending_reservations_usd(
        backend, caller_subsystem=caller_subsystem, provider=provider, now=now
    )
    return float(committed) + float(pending)


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
            BudgetVerdict.DENY, remaining, reason, spent_usd=spent_usd, limit_usd=limit_usd
        )
    if estimated_cost_usd > 0 and (spent_usd + estimated_cost_usd) > limit_usd:
        reason = (
            f"estimated ${estimated_cost_usd:.4f} would exceed remaining ${remaining:.4f}"
            if label == "budget"
            else f"estimated ${estimated_cost_usd:.4f} would exceed {label} remaining ${remaining:.4f}"
        )
        return BudgetDecision(
            BudgetVerdict.DENY, remaining, reason, spent_usd=spent_usd, limit_usd=limit_usd
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
    configured_provider_caps = {
        str(name).strip().lower(): float(cap)
        for name, cap in provider_caps.items()
        if str(name).strip() and cap is not None and float(cap) > 0
    }
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
        return BudgetDecision(BudgetVerdict.ALLOW, float("inf"), "no limit", limit_usd=None)

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
                "KNEMON budget ledger query failed; denying under configured cap: %s", exc
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
                backend, provider=provider_key, caller_subsystem=caller_subsystem, now=now
            )
        except Exception as exc:
            logger.warning(
                "KNEMON %s ledger query failed; denying under configured cap: %s", label, exc
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
    ttl = int(ttl_seconds) if ttl_seconds is not None else _cost_reservation_ttl_seconds()
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
    configured_provider_caps = {
        str(name).strip().lower(): float(cap)
        for name, cap in provider_caps.items()
        if str(name).strip() and cap is not None and float(cap) > 0
    }
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
            reserved_at = row.get("reserved_at")
            existing_expires = row.get("expires_at")
            if hasattr(reserved_at, "isoformat"):
                reserved_at = reserved_at
            if hasattr(existing_expires, "isoformat"):
                existing_expires = existing_expires
            return ReservationHandle(
                attempt_id=str(row.get("attempt_id")),
                provider=str(row.get("provider") or canon_provider),
                model=str(row.get("model") or canon_model),
                caller_subsystem=str(row.get("caller_subsystem") or caller_subsystem),
                estimated_cost_usd=float(row.get("estimated_cost_usd") or 0.0),
                reserved_at=reserved_at if isinstance(reserved_at, datetime) else current,
                expires_at=existing_expires if isinstance(existing_expires, datetime) else expires_at,
            )

        # Sum committed + pending (effective spend) so concurrent reserves
        # for the same caller_subsystem cannot race past the cap.
        if limit_usd is not None or provider_cap is not None:
            effective_global = await effective_spend_usd_tx(
                conn, caller_subsystem=caller_subsystem, now=current
            )
            if limit_usd is not None and (effective_global + estimated_cost_usd) > limit_usd:
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

    return ReservationHandle(
        attempt_id=attempt_id,
        provider=canon_provider,
        model=canon_model,
        caller_subsystem=caller_subsystem,
        estimated_cost_usd=estimated_cost_usd,
        reserved_at=current,
        expires_at=expires_at,
    )


async def effective_spend_usd_tx(
    conn: Any,
    *,
    caller_subsystem: str,
    provider: str | None = None,
    now: datetime | None = None,
) -> float:
    """Effective spend computed on an already-open transaction connection.

    Same semantics as ``effective_spend_usd`` but joins an existing
    transaction (avoiding opening a new one). Used by
    ``reserve_cost`` so the per-caller_subsystem lock and the
    effective-spend SUM happen in the same atomic transaction.
    """
    current = now or datetime.now(timezone.utc)
    since = current - timedelta(days=7)
    committed_params: dict[str, Any] = {
        "caller_subsystem": caller_subsystem,
        "since_ts": since,
    }
    provider_clause = ""
    if provider:
        committed_params["provider"] = provider.strip().lower()
        provider_clause = "AND LOWER(provider) = :provider"
    committed_rows = await _tx_rows(
        conn,
        f"""
        SELECT COALESCE(SUM(est_cost_usd), 0) AS spent_usd
        FROM usage_ledger
        WHERE caller_subsystem = :caller_subsystem
          AND ts >= :since_ts
          {provider_clause}
        """,
        committed_params,
    )
    committed = _to_float((committed_rows[0] if committed_rows else {}).get("spent_usd"), 0.0)

    pending_params: dict[str, Any] = {
        "caller_subsystem": caller_subsystem,
        "now_ts": current,
    }
    pending_provider_clause = ""
    if provider:
        pending_params["provider"] = provider.strip().lower()
        pending_provider_clause = "AND LOWER(provider) = :provider"
    pending_rows = await _tx_rows(
        conn,
        f"""
        SELECT COALESCE(SUM(estimated_cost_usd), 0) AS pending_usd
        FROM knemon_cost_reservations
        WHERE status = 'pending'
          AND caller_subsystem = :caller_subsystem
          AND expires_at > :now_ts
          {pending_provider_clause}
        """,
        pending_params,
    )
    pending = _to_float((pending_rows[0] if pending_rows else {}).get("pending_usd"), 0.0)
    return float(committed) + float(pending)


async def settle_cost(
    backend: Any,
    *,
    attempt_id: str,
    actual_cost_usd: float,
    ledger_id: int | None = None,
    now: datetime | None = None,
) -> None:
    """Finalize a pending reservation to its ACTUAL post-call cost.

    F04 contract:

    * Idempotent: settling the same ``attempt_id`` twice does NOT
      double-count. The second call is a no-op (returns ``None``).
    * Raises ``CostReservationConflict`` only if the attempt_id is in
      a non-pending state but with a DIFFERENT ``actual_cost_usd``
      recorded (a real double-settle). Already-released attempts are
      treated as "the caller's view changed", not as a conflict, so
      callers that release-then-settle in a race don't fail loudly.
    """
    actual_cost_usd, valid = _normalise_estimated_cost(actual_cost_usd)
    if not valid:
        raise CostReservationConflict(
            f"invalid actual_cost_usd {actual_cost_usd!r} for attempt_id={attempt_id}"
        )

    current = now or datetime.now(timezone.utc)
    params: dict[str, Any] = {
        "attempt_id": attempt_id,
        "now_ts": current,
        "actual_cost_usd": actual_cost_usd,
    }
    if ledger_id is not None:
        params["ledger_id"] = ledger_id

    async with backend.transactional() as tx:
        conn = _conn_from_tx(tx)
        rows = await _tx_rows(
            conn,
            """
            SELECT attempt_id, status, actual_cost_usd
            FROM knemon_cost_reservations
            WHERE attempt_id = :attempt_id
            """,
            {"attempt_id": attempt_id},
        )
        if not rows:
            # No reservation — silently no-op (idempotent for callers that
            # never reserved, e.g. older PANTHEON versions).
            return
        row = rows[0]
        status = str(row.get("status") or "")
        existing_actual = row.get("actual_cost_usd")
        if status == "settled":
            # Idempotent: same actual cost is a no-op. Different actual cost
            # is a real double-settle and we raise.
            if existing_actual is not None and _to_float(existing_actual, -1.0) != actual_cost_usd:
                raise CostReservationConflict(
                    f"attempt_id={attempt_id} already settled with actual_cost_usd={existing_actual!r}, "
                    f"refusing to overwrite with {actual_cost_usd!r}"
                )
            return
        if status == "released":
            # The caller's view changed mid-flight. Don't pretend to
            # re-settle a released reservation; log + silently no-op so
            # cleanup paths don't fail loudly.
            logger.info(
                "KNEMON settle_cost called on released attempt_id=%s; ignoring", attempt_id
            )
            return
        # status == "pending" — promote to settled.
        update_sql = """
            UPDATE knemon_cost_reservations
            SET status = 'settled',
                settled_at = :now_ts,
                actual_cost_usd = :actual_cost_usd
            WHERE attempt_id = :attempt_id AND status = 'pending'
        """
        params_for_update = {
            "attempt_id": attempt_id,
            "now_ts": current,
            "actual_cost_usd": actual_cost_usd,
        }
        if ledger_id is not None:
            update_sql += ", ledger_id = :ledger_id"
            params_for_update["ledger_id"] = ledger_id
        await _tx_execute(conn, update_sql, params_for_update)


async def release_cost(
    backend: Any,
    *,
    attempt_id: str,
    now: datetime | None = None,
) -> None:
    """Return the headroom for an aborted reservation.

    F04 contract:

    * Idempotent and safe to call on already-settled or
      already-released ``attempt_id`` (no-op, not an error). Cleanup
      code may invoke this defensively without tracking exact state.
    * Raises only if the persistence backend itself fails — the prior
      "swallow on failure" behavior was part of the F04 bug.
    """
    current = now or datetime.now(timezone.utc)
    async with backend.transactional() as tx:
        conn = _conn_from_tx(tx)
        rows = await _tx_rows(
            conn,
            """
            SELECT attempt_id, status
            FROM knemon_cost_reservations
            WHERE attempt_id = :attempt_id
            """,
            {"attempt_id": attempt_id},
        )
        if not rows:
            # No reservation — idempotent no-op for callers that release
            # defensively without a prior reserve.
            return
        status = str(rows[0].get("status") or "")
        if status != "pending":
            # Already-settled / released: no-op, do not raise.
            return
        await _tx_execute(
            conn,
            """
            UPDATE knemon_cost_reservations
            SET status = 'released', released_at = :now_ts
            WHERE attempt_id = :attempt_id AND status = 'pending'
            """,
            {"attempt_id": attempt_id, "now_ts": current},
        )


async def cleanup_cost_reservations(
    backend: Any,
    *,
    now: datetime | None = None,
) -> None:
    """Bound reservation history and expire stale pending reservations.

    Drop settled / released rows older than the configured retention.
    Move stale pending reservations (``expires_at < now``) to
    ``released`` so a crashed caller doesn't permanently burn
    headroom. Cleanup is idempotent; multiple processes may call it.
    """
    current = now or datetime.now(timezone.utc)
    cutoff = current - timedelta(seconds=_cost_reservation_retention_seconds())
    async with backend.transactional() as tx:
        conn = _conn_from_tx(tx)
        await _tx_execute(
            conn,
            """
            UPDATE knemon_cost_reservations
            SET status = 'released', released_at = :now_ts
            WHERE status = 'pending' AND expires_at < :now_ts
            """,
            {"now_ts": current},
        )
        await _tx_execute(
            conn,
            """
            DELETE FROM knemon_cost_reservations
            WHERE status IN ('settled', 'released')
              AND COALESCE(settled_at, released_at, reserved_at) < :cutoff_ts
            """,
            {"cutoff_ts": cutoff},
        )


__all__ = [
    "BudgetDecision",
    "BudgetVerdict",
    "CostReservationConflict",
    "CostReservationSchemaMissing",
    "ReservationHandle",
    "canonical_budget_key",
    "cleanup_cost_reservations",
    "effective_spend_usd",
    "evaluate_usage_budget",
    "pending_reservations_usd",
    "provider_spend_usd",
    "release_cost",
    "reserve_cost",
    "settle_cost",
    "weekly_spend_usd",
]