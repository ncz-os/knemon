from __future__ import annotations

from types import SimpleNamespace

import pytest

from mnemos.domain.knemon import budget as B
from mnemos.domain.knemon.budget import evaluate_usage_budget


def _settings(weekly: float = 0.0, provider_caps: dict | None = None):
    knemon = SimpleNamespace(
        weekly_budget_cap_usd=weekly,
        parsed_provider_budget_caps_usd=lambda: dict(provider_caps or {}),
    )
    return SimpleNamespace(knemon=knemon)


@pytest.fixture
def patch_settings(monkeypatch):
    def _apply(weekly: float = 0.0, provider_caps: dict | None = None):
        monkeypatch.setattr(B, "get_settings", lambda: _settings(weekly, provider_caps))

    return _apply


def _patch_spends(monkeypatch, *, total: float = 0.0, per_provider: dict | None = None):
    per_provider = per_provider or {}

    async def _weekly(backend, *, caller_subsystem="pantheon", provider=None, now=None):
        if provider:
            return float(per_provider.get(provider.lower(), 0.0))
        return float(total)

    async def _prov(backend, *, provider, caller_subsystem="pantheon", now=None):
        return float(per_provider.get(provider.lower(), 0.0))

    monkeypatch.setattr(B, "weekly_spend_usd", _weekly)
    monkeypatch.setattr(B, "provider_spend_usd", _prov)


@pytest.mark.asyncio
async def test_no_caps_is_unlimited(patch_settings):
    patch_settings(weekly=0.0, provider_caps={})
    decision = await evaluate_usage_budget(object())
    assert decision.allowed
    assert decision.reason == "no limit"


@pytest.mark.asyncio
async def test_global_cap_denies_when_exhausted(patch_settings, monkeypatch):
    patch_settings(weekly=100.0)
    _patch_spends(monkeypatch, total=100.0)
    decision = await evaluate_usage_budget(object())
    assert not decision.allowed
    assert "budget exhausted" in decision.reason


@pytest.mark.asyncio
async def test_provider_cap_denies_even_when_global_has_headroom(patch_settings, monkeypatch):
    patch_settings(weekly=1000.0, provider_caps={"openai": 50.0})
    _patch_spends(monkeypatch, total=200.0, per_provider={"openai": 50.0})
    decision = await evaluate_usage_budget(object(), provider="openai")
    assert not decision.allowed
    assert "provider:openai" in decision.reason


@pytest.mark.asyncio
async def test_provider_under_cap_reports_tightest_remaining(patch_settings, monkeypatch):
    patch_settings(weekly=1000.0, provider_caps={"openai": 50.0})
    _patch_spends(monkeypatch, total=200.0, per_provider={"openai": 10.0})
    decision = await evaluate_usage_budget(object(), provider="openai", estimated_cost_usd=5.0)
    assert decision.allowed
    # global remaining 800, provider remaining 40 -> report the tighter (40).
    assert decision.remaining_usd == pytest.approx(40.0)


@pytest.mark.asyncio
async def test_estimated_cost_over_provider_cap_denies(patch_settings, monkeypatch):
    patch_settings(weekly=1000.0, provider_caps={"openai": 50.0})
    _patch_spends(monkeypatch, total=0.0, per_provider={"openai": 48.0})
    decision = await evaluate_usage_budget(object(), provider="openai", estimated_cost_usd=5.0)
    assert not decision.allowed
    assert "would exceed provider:openai remaining" in decision.reason


@pytest.mark.asyncio
async def test_fail_closed_when_backend_missing_under_provider_cap(patch_settings):
    patch_settings(weekly=0.0, provider_caps={"openai": 50.0})
    decision = await evaluate_usage_budget(None, provider="openai")
    assert not decision.allowed
    assert "failing closed" in decision.reason


@pytest.mark.asyncio
async def test_provider_without_configured_cap_uses_global_only(patch_settings, monkeypatch):
    patch_settings(weekly=100.0, provider_caps={"openai": 50.0})
    # anthropic has no per-provider cap; its large spend must NOT be consulted.
    _patch_spends(monkeypatch, total=10.0, per_provider={"anthropic": 999.0})
    decision = await evaluate_usage_budget(object(), provider="anthropic")
    assert decision.allowed
    assert decision.remaining_usd == pytest.approx(90.0)
