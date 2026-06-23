"""KNEMON-owned usage budget decisions backed by ``usage_ledger``."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from mnemos.core.config import get_settings
from mnemos.domain.knemon.router import _rows, _to_float

logger = logging.getLogger(__name__)


class BudgetVerdict(str, Enum):
    ALLOW = "allow"
    DENY = "deny"


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


async def weekly_spend_usd(
    backend: Any,
    *,
    caller_subsystem: str = "pantheon",
    provider: str | None = None,
    now: datetime | None = None,
) -> float:
    """Return rolling-seven-day spend from KNEMON's ledger.

    When ``provider`` is given the spend is scoped to that single provider (the
    ledger ``provider`` column is matched case-insensitively); otherwise it is
    the total across all providers for ``caller_subsystem``.
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
    """Rolling-seven-day spend for a single ``provider`` (convenience wrapper)."""
    return await weekly_spend_usd(
        backend, caller_subsystem=caller_subsystem, provider=provider, now=now
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

    KNEMON is the single owner of spend math: callers supply only an optional
    request-cost estimate, the ledger-backed backend, and (optionally) the
    ``provider`` the request will hit. Two caps are enforced when configured:

    * the global weekly cap (``limit_usd`` / ``MNEMOS_KNEMON_WEEKLY_BUDGET_CAP_USD``)
      summed across all providers, and
    * a per-provider weekly cap (``provider_caps`` /
      ``MNEMOS_KNEMON_PROVIDER_BUDGET_CAPS_USD``) scoped to ``provider``.

    A request is DENIED if EITHER cap would be exceeded; when both ALLOW, the
    decision reports the tightest remaining headroom. If no cap is configured the
    decision is unlimited.

    SECURITY (review #8): once ANY cap is configured, enforcement FAILS CLOSED —
    a missing/unavailable ledger backend or a failed spend query DENIES rather
    than granting unlimited spend. Deployments that do not want enforcement leave
    the caps unset (the unlimited ALLOW path below).
    """
    if limit_usd is None:
        configured = float(get_settings().knemon.weekly_budget_cap_usd)
        limit_usd = configured if configured > 0 else None

    provider_key = (provider or "").strip().lower()
    provider_cap: float | None = None
    if provider_key:
        if provider_caps is None:
            provider_caps = get_settings().knemon.parsed_provider_budget_caps_usd()
        candidate = provider_caps.get(provider_key)
        if candidate is not None and candidate > 0:
            provider_cap = float(candidate)

    if limit_usd is None and provider_cap is None:
        return BudgetDecision(BudgetVerdict.ALLOW, float("inf"), "no limit", limit_usd=None)

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
            spent_usd = await weekly_spend_usd(
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
            spent_provider = await provider_spend_usd(
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


__all__ = [
    "BudgetDecision",
    "BudgetVerdict",
    "evaluate_usage_budget",
    "provider_spend_usd",
    "weekly_spend_usd",
]
