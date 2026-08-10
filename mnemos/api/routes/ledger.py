"""KNEMON usage ledger routes."""

from __future__ import annotations

from decimal import Decimal
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from mnemos.api.dependencies import UserContext, require_root
from mnemos.api.extra_guards import require_extra
from mnemos.api.persistence_helpers import backend_or_503
from mnemos.core.plan_windows import compute_plan_window_id, plan_path_kind
from mnemos.domain.knemon.router import (
    BurnReservationConflict,
    _allow_nonatomic_burn_development,
    _is_burn_reservation_schema_missing,
    _plans_by_provider,
    reconcile_burn_reservation,
)
from mnemos.persistence.base import UsageLedgerRecord

router = APIRouter(prefix="/v1", tags=["ledger"], dependencies=[require_extra("knemon", label="KNEMON")])


class LedgerRecordRequest(BaseModel):
    provider: str = Field(..., min_length=1)
    model: str = Field(..., min_length=1)
    task_kind: str = Field(..., min_length=1)
    tokens_in: int = Field(..., ge=0)
    tokens_out: int = Field(..., ge=0)
    tokens_reasoning: int = Field(default=0, ge=0)
    latency_ms: int = Field(..., ge=0)
    outcome: Literal["ok", "err", "timeout"]
    caller_subsystem: str = Field(..., min_length=1)
    tier: str = Field(..., min_length=1)
    # Match /v1/knemon/route so every accepted route session can reconcile.
    session_id: str | None = Field(default=None, max_length=128)
    request_count: int = Field(default=1, ge=1)
    path_kind: str | None = Field(default=None, max_length=64)
    reservation_id: str | None = Field(default=None, min_length=36, max_length=36)


class LedgerRecordResponse(BaseModel):
    id: int
    est_cost_usd: Decimal
    reconciled_reservation_ids: list[str] = Field(default_factory=list)


def _missing_plan_schema(exc: Exception) -> bool:
    sqlstate = str(getattr(exc, "sqlstate", "") or getattr(exc, "pgcode", ""))
    errno = getattr(exc, "errno", None)
    sqlcode = str(getattr(exc, "sqlcode", ""))
    message = str(exc).lower()
    return (
        isinstance(exc, (AttributeError, NotImplementedError))
        or sqlstate in {"42P01", "42704"}
        or errno == 1146
        or sqlcode == "-204"
        or "no such table: subscription_plans" in message
        or "ora-00942" in message
    )


async def _plan_window_options(backend, provider: str, tier: str) -> dict[str, object]:
    """Use the active subscription-plan row as the window authority when present."""
    try:
        plans = await _plans_by_provider(backend)
    except Exception as exc:
        if not _missing_plan_schema(exc):
            raise
        # Compatibility for ledger-only backends without registry query support.
        return {}
    provider_plans = plans.get(provider.strip().lower(), [])
    for plan in provider_plans:
        if str(plan.get("plan_name") or "").strip().lower() != tier.strip().lower():
            continue
        seconds = plan.get("msg_window_seconds") or plan.get("token_window_seconds")
        return {
            "reset_anchor": str(plan.get("reset_anchor") or "monthly"),
            "window_seconds": int(seconds) if seconds else None,
        }
    return {}


@router.post("/ledger", response_model=LedgerRecordResponse)
async def record_ledger_usage(
    payload: LedgerRecordRequest,
    user: UserContext = Depends(require_root),
) -> LedgerRecordResponse:
    backend = backend_or_503()
    recorder = getattr(backend, "record_usage_ledger", None)
    if recorder is None:
        raise HTTPException(status_code=503, detail="usage_ledger requires a ledger-capable backend")

    record_payload = payload.model_dump(exclude={"reservation_id"})
    record_payload["session_id"] = payload.session_id or f"user:{user.user_id}"
    record_payload["path_kind"] = plan_path_kind(payload.provider, payload.tier, payload.path_kind)
    window_options = await _plan_window_options(backend, payload.provider, payload.tier)
    record = UsageLedgerRecord(
        **record_payload,
        plan_window_id=compute_plan_window_id(payload.provider, payload.tier, **window_options),
    )
    try:
        async with backend.transactional() as tx:
            result = await recorder(tx, record)
            reconciled = []
            if not _allow_nonatomic_burn_development():
                reconciled = await reconcile_burn_reservation(
                    tx,
                    reservation_id=payload.reservation_id,
                    session_id=str(record.session_id),
                    ledger_id=result.id,
                    request_count=record.request_count,
                )
    except NotImplementedError:
        raise HTTPException(status_code=503, detail="usage_ledger requires a ledger-capable backend")
    except BurnReservationConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except Exception as exc:
        if _is_burn_reservation_schema_missing(exc):
            raise HTTPException(
                status_code=503,
                detail=(
                    "KNEMON atomic burn reservation schema is missing; apply migration "
                    "0040_knemon_burn_reservations and restart. For development or a single-worker "
                    "deployment only, set MNEMOS_KNEMON_ALLOW_NONATOMIC_BURN_DEVELOPMENT=true."
                ),
            )
        raise

    return LedgerRecordResponse(
        id=result.id,
        est_cost_usd=result.est_cost_usd,
        reconciled_reservation_ids=reconciled,
    )
