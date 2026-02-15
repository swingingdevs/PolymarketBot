from __future__ import annotations

from config import Settings
from execution.trader import Trader
from metrics import (
    MAX_DAILY_LOSS_PCT_CONFIGURED,
    MAX_DAILY_LOSS_USD_CONFIGURED,
    MAX_TOTAL_OPEN_EXPOSURE_PCT_CONFIGURED,
    MAX_TOTAL_OPEN_EXPOSURE_USD_CONFIGURED,
)


def test_effective_caps_adjust_with_equity(tmp_path):
    settings = Settings(
        dry_run=True,
        risk_state_path=str(tmp_path / "risk_state.json"),
        max_daily_loss_usd=200.0,
        max_daily_loss_pct=0.05,
        max_open_exposure_per_market_usd=300.0,
        max_open_exposure_per_market_pct=0.10,
        max_total_open_exposure_usd=500.0,
        max_total_open_exposure_pct=0.20,
    )
    trader = Trader(settings)

    low_equity_caps = trader._compute_effective_risk_caps(1_000.0)
    assert low_equity_caps.daily_loss_cap_usd == 50.0
    assert low_equity_caps.exposure_per_market_cap_usd == 100.0
    assert low_equity_caps.total_exposure_cap_usd == 200.0

    high_equity_caps = trader._compute_effective_risk_caps(10_000.0)
    assert high_equity_caps.daily_loss_cap_usd == 200.0
    assert high_equity_caps.exposure_per_market_cap_usd == 300.0
    assert high_equity_caps.total_exposure_cap_usd == 500.0


def test_daily_loss_blocks_new_orders(tmp_path):
    settings = Settings(
        dry_run=True,
        risk_state_path=str(tmp_path / "risk_state.json"),
        max_daily_loss_usd=100.0,
        max_daily_loss_pct=0.05,
        equity_usd=1_000.0,
        max_usd_per_trade=100.0,
    )
    trader = Trader(settings)
    trader.risk.daily_realized_pnl = -60.0

    assert trader._check_risk(10.0, token_id="t1", horizon="5m", direction="BUY") is False


def test_exposure_caps_block_per_market_and_total(tmp_path):
    settings = Settings(
        dry_run=True,
        risk_state_path=str(tmp_path / "risk_state.json"),
        max_usd_per_trade=100.0,
        max_open_exposure_per_market_usd=100.0,
        max_open_exposure_per_market_pct=0.50,
        max_total_open_exposure_usd=150.0,
        max_total_open_exposure_pct=0.50,
        equity_usd=1_000.0,
    )
    trader = Trader(settings)

    trader._apply_open_exposure_delta("t1", "5m", "BUY", 90.0)
    assert trader._check_risk(20.0, token_id="t1", horizon="5m", direction="BUY") is False

    trader._apply_open_exposure_delta("t2", "15m", "BUY", 50.0)
    assert trader._check_risk(20.0, token_id="t3", horizon="30m", direction="BUY") is False


def test_configured_risk_limit_metrics_are_exported(tmp_path):
    settings = Settings(
        dry_run=True,
        risk_state_path=str(tmp_path / "risk_state.json"),
        max_daily_loss_usd=250.0,
        max_daily_loss_pct=0.05,
        max_total_open_exposure_usd=500.0,
        max_total_open_exposure_pct=0.15,
    )
    Trader(settings)

    assert MAX_DAILY_LOSS_USD_CONFIGURED._value.get() == 250.0
    assert MAX_DAILY_LOSS_PCT_CONFIGURED._value.get() == 0.05
    assert MAX_TOTAL_OPEN_EXPOSURE_USD_CONFIGURED._value.get() == 500.0
    assert MAX_TOTAL_OPEN_EXPOSURE_PCT_CONFIGURED._value.get() == 0.15


def test_live_mode_fails_closed_when_equity_unavailable(tmp_path):
    settings = Settings(
        dry_run=False,
        risk_state_path=str(tmp_path / "risk_state.json"),
        max_usd_per_trade=100.0,
    )
    trader = Trader(settings)

    assert trader._check_risk(10.0, token_id="t1", horizon="5m", direction="BUY") is False


def test_live_mode_refresh_marks_failure_without_synthetic_fallback(tmp_path):
    settings = Settings(
        dry_run=False,
        risk_state_path=str(tmp_path / "risk_state.json"),
        equity_usd=1_000.0,
    )
    trader = Trader(settings)
    trader.risk.cumulative_realized_pnl = -250.0

    refreshed = trader._refresh_equity_cache(force=True)

    assert refreshed == 1_000.0
    assert trader._equity_refresh_failed is True


def test_successful_refresh_clears_failure_flag(tmp_path, monkeypatch):
    settings = Settings(
        dry_run=False,
        risk_state_path=str(tmp_path / "risk_state.json"),
    )
    trader = Trader(settings)
    monkeypatch.setattr(trader, "_effective_equity_usd", lambda: None)
    trader._refresh_equity_cache(force=True)

    monkeypatch.setattr(trader, "_effective_equity_usd", lambda: 1_500.0)
    refreshed = trader._refresh_equity_cache(force=True)

    assert refreshed == 1_500.0
    assert trader._equity_refresh_failed is False
