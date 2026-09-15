"""One durable accounting owner per upstream attempt, shared by the gateways."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
import weakref
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from mnemos.core.config import get_settings
from mnemos.domain.knemon import budget

logger = logging.getLogger(__name__)
_cleanup_after: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()


def normalized_usage(usage: dict | None) -> dict[str, int]:
    data = dict(usage or {})
    details = data.get("completion_tokens_details")
    if isinstance(details, dict) and "reasoning_tokens" in details:
        data.setdefault("reasoning_tokens", details["reasoning_tokens"])

    def value(*names: str) -> int:
        for key in names:
            if data.get(key) is not None:
                try:
                    return max(0, int(data[key]))
                except (ValueError, TypeError):
                    continue
        return 0

    cached_extra = (
        value("cache_read_input_tokens") + value("cache_creation_input_tokens")
        if "input_tokens" in data and "prompt_tokens" not in data
        else 0
    )
    return {
        "tokens_in": cached_extra
        + value("prompt_tokens", "input_tokens", "tokens_in", "promptTokenCount"),
        "tokens_out": value(
            "completion_tokens", "output_tokens", "tokens_out", "candidatesTokenCount"
        ),
        "tokens_reasoning": value(
            "reasoning_tokens", "tokens_reasoning", "thoughtsTokenCount"
        ),
    }


def finite_cost(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        cost = float(value)
    except (ValueError, TypeError):
        return None
    return cost if math.isfinite(cost) and cost >= 0 else None


def configured_caps() -> bool:
    cfg = get_settings().knemon
    return cfg.weekly_budget_cap_usd > 0 or bool(cfg.parsed_provider_budget_caps_usd())


@dataclass
class Attempt:
    provider: str
    model: str
    subsystem: str
    estimate: float | None
    input_price: float | None = None
    output_price: float | None = None
    task_kind: str = "chat"
    backend: Any = None
    handle: Any = None
    usage: dict | None = None
    cost: float | None = None
    started: float = field(default_factory=time.monotonic)
    finished: bool = False
    _buffer: str = ""
    _finish_task: Any = None

    async def begin(self, timeout: float = 300) -> "Attempt":
        from mnemos.core import lifecycle

        self.backend = lifecycle._persistence_backend
        self.provider, _ = budget.canonical_budget_key(self.provider, self.model)
        self.estimate = finite_cost(self.estimate)
        capped = configured_caps()
        if capped and self.estimate is None:
            raise budget.CostReservationConflict(
                "provider pricing unavailable for budgeted attempt"
            )
        if self.backend is None:
            if capped:
                raise budget.CostReservationSchemaMissing(
                    "budgeted attempt requires persistence"
                )
            return self
        # Unknown unbudgeted calls can still record usage; do not manufacture a
        # priced zero reservation when no rate is known.
        if self.estimate is not None:
            from mnemos.domain.knemon.migrations import ensure_knemon_schema

            await ensure_knemon_schema(self.backend)
            if time.monotonic() >= _cleanup_after.get(self.backend, 0):
                _cleanup_after[self.backend] = time.monotonic() + 30
                try:
                    await budget.cleanup_cost_reservations(self.backend)
                except BaseException:
                    _cleanup_after.pop(self.backend, None)
                    raise
            self.handle = await budget.reserve_cost(
                self.backend,
                provider=self.provider,
                model=self.model,
                estimated_cost_usd=self.estimate,
                caller_subsystem=self.subsystem,
                ttl_seconds=max(300, int(timeout) + 60),
            )
        return self

    def observe(self, response: dict) -> None:
        data = (
            response.get("response")
            if isinstance(response.get("response"), dict)
            else response
        )
        usage = data.get("usage") or data.get("usageMetadata")
        if isinstance(usage, dict):
            self.usage = {**(self.usage or {}), **usage}
        cost = finite_cost(data.get("cost_usd", data.get("cost")))
        if cost is not None:
            self.cost = cost

    def feed(self, chunk: bytes | str | dict) -> None:
        if isinstance(chunk, dict):
            self.observe(chunk)
            return
        self._buffer += (
            chunk.decode("utf-8", "replace") if isinstance(chunk, bytes) else chunk
        )
        lines = self._buffer.split("\n")
        self._buffer = lines.pop()
        # No unbounded server-supplied partial SSE line.
        if len(self._buffer) > 1_048_576:
            self._buffer = ""
        for line in lines:
            if line.startswith("data:"):
                try:
                    value = json.loads(line[5:].strip())
                    if isinstance(value, dict):
                        self.observe(value)
                except (ValueError, TypeError):
                    pass

    async def finish(self, outcome: str = "ok") -> None:
        if self.finished:
            return
        # Shield final accounting from caller cancellation and wait for it to
        # finish before propagating cancellation. A DB failure keeps the pending
        # reservation held and is surfaced, never silently released.
        if self._finish_task is None:
            self._finish_task = asyncio.create_task(self._finish(outcome))
        task = self._finish_task
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise
        self.finished = True

    async def _finish(self, outcome: str) -> None:
        if self.backend is None:
            return
        if outcome == "unused":
            if self.handle is not None:
                await budget.release_cost(
                    self.backend, attempt_id=self.handle.attempt_id
                )
            return
        usage = normalized_usage(self.usage)
        amount = self.cost
        if (
            amount is None
            and self.usage is not None
            and self.input_price is not None
            and self.output_price is not None
        ):
            # Native cache creation can cost up to 2x the base input rate.
            # Reserve conservatively where a provider exposes no cache price.
            cache_creation = int(
                (self.usage or {}).get("cache_creation_input_tokens") or 0
            )
            extra_reasoning = (
                usage["tokens_reasoning"]
                if self.provider == "gemini"
                or "thoughtsTokenCount" in (self.usage or {})
                else 0
            )
            amount = (
                (usage["tokens_in"] + cache_creation) * self.input_price
                + (usage["tokens_out"] + extra_reasoning) * self.output_price
            ) / 1_000_000
        if amount is None:
            amount = self.estimate
        ledger_id = None
        recorder = getattr(self.backend, "record_usage_ledger", None)
        if recorder is not None:
            from mnemos.persistence.base import UsageLedgerRecord

            record = UsageLedgerRecord(
                provider=self.provider,
                model=self.model,
                task_kind=self.task_kind,
                **usage,
                latency_ms=int((time.monotonic() - self.started) * 1000),
                outcome=(outcome if outcome in {"ok", "err", "timeout"} else "err"),
                caller_subsystem=self.subsystem,
                tier="api",
                est_cost_usd=Decimal(str(amount)) if amount is not None else None,
            )
            try:
                async with self.backend.transactional() as tx:
                    result = await recorder(tx, record)
                    amount = float(result.est_cost_usd)
                    if self.handle is not None:
                        await budget.settle_cost_in_transaction(
                            tx,
                            attempt_id=self.handle.attempt_id,
                            actual_cost_usd=amount,
                            ledger_id=result.id,
                        )
                ledger_id = result.id
            except Exception:
                # Durable unlinked settlement retains spend when ledger append
                # fails. Operators get an actionable warning, not lost exposure.
                logger.exception(
                    "usage ledger append failed for %s/%s", self.provider, self.model
                )
                if self.handle is None:
                    raise
        if self.handle is not None and ledger_id is None:
            await budget.settle_cost(
                self.backend,
                attempt_id=self.handle.attempt_id,
                actual_cost_usd=amount
                if amount is not None
                else self.handle.estimated_cost_usd,
                ledger_id=ledger_id,
            )
        self.cost = amount
