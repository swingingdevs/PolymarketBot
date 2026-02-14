from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta

from config import Settings
from execution.trader import Trader


def test_trader_persists_and_reloads_risk_state(tmp_path):
    state_path = tmp_path / "risk_state.json"
    settings = Settings(dry_run=True, risk_state_path=str(state_path))

    trader = Trader(settings)
    trader.risk.daily_realized_pnl = -12.5
    trader.risk.trades_this_hour = 3
    trader._persist_risk_state()

    reloaded = Trader(settings)
    assert reloaded.risk.daily_realized_pnl == -12.5
    assert reloaded.risk.trades_this_hour == 3


def test_trader_resets_daily_pnl_on_utc_rollover(tmp_path):
    state_path = tmp_path / "risk_state.json"
    yesterday = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
    state_path.write_text(
        json.dumps(
            {
                "daily_realized_pnl": -100.0,
                "trades_this_hour": 2,
                "last_trade_hour": 5,
                "last_pnl_reset_date_utc": yesterday,
            }
        ),
        encoding="utf-8",
    )

    settings = Settings(dry_run=True, risk_state_path=str(state_path))
    trader = Trader(settings)

    assert trader.risk.daily_realized_pnl == 0.0
    assert trader.risk.last_pnl_reset_date_utc == datetime.now(timezone.utc).date().isoformat()


def test_extract_realized_pnl_from_nested_response(tmp_path):
    settings = Settings(dry_run=True, risk_state_path=str(tmp_path / "risk_state.json"))
    trader = Trader(settings)

    response = {
        "status": "ok",
        "realized_pnl": "2.5",
        "fills": [{"pnl": -1.0}, {"realizedPnl": "0.75"}],
        "settlements": [{"settlement_pnl": "1.25"}],
    }

    assert trader._extract_realized_pnl(response) == 3.5


def test_rejects_when_market_open_exposure_cap_exceeded(tmp_path):
    settings = Settings(
        dry_run=True,
        quote_size_usd=60.0,
        max_usd_per_trade=100.0,
        max_open_exposure_per_market=100.0,
        max_open_exposure_per_market_pct=1.0,
        max_total_open_exposure=1_000.0,
        max_total_open_exposure_pct=1.0,
        risk_state_path=str(tmp_path / "risk_state.json"),
    )
    trader = Trader(settings)

    first = trader._check_risk(notional_usd=60.0, token_id="t1", horizon="5m", direction="BUY")
    trader._apply_open_exposure_delta("t1", "5m", "BUY", 60.0)
    second = trader._check_risk(notional_usd=60.0, token_id="t1", horizon="5m", direction="BUY")

    assert first is True
    assert second is False


def test_rejects_when_global_open_exposure_cap_exceeded(tmp_path):
    settings = Settings(
        dry_run=True,
        max_open_exposure_per_market=500.0,
        max_open_exposure_per_market_pct=1.0,
        max_usd_per_trade=100.0,
        max_total_open_exposure=100.0,
        max_total_open_exposure_pct=1.0,
        risk_state_path=str(tmp_path / "risk_state.json"),
    )
    trader = Trader(settings)

    trader._apply_open_exposure_delta("t1", "5m", "BUY", 60.0)
    trader._apply_open_exposure_delta("t2", "15m", "BUY", 30.0)

    assert trader._check_risk(notional_usd=15.0, token_id="t3", horizon="30m", direction="BUY") is False


def test_load_risk_state_migrates_legacy_exposure_keys(tmp_path):
    state_path = tmp_path / "risk_state.json"
    state_path.write_text(
        json.dumps(
            {
                "open_exposure_usd_by_market": {
                    "token-a|5m|BUY": 10.0,
                    "token-a|5m|BUY|slug:btc-updown-5m-1700000000": 2.5,
                }
            }
        ),
        encoding="utf-8",
    )

    trader = Trader(Settings(dry_run=True, risk_state_path=str(state_path)))

    assert trader.risk.open_exposure_usd_by_market == {
        "token-a|5m|BUY|legacy": 10.0,
        "token-a|5m|BUY|slug:btc-updown-5m-1700000000": 2.5,
    }


def test_exposure_rollover_cleanup_removes_expired_market_entries(tmp_path):
    state_path = tmp_path / "risk_state.json"
    now = int(datetime.now(timezone.utc).timestamp())
    expired_start = now - 600
    active_start = now - 60
    state_path.write_text(
        json.dumps(
            {
                "open_exposure_usd_by_market": {
                    f"token-expired|5m|BUY|start:{expired_start}": 10.0,
                    f"token-active|15m|BUY|start:{active_start}": 20.0,
                },
                "total_open_notional_usd": 30.0,
            }
        ),
        encoding="utf-8",
    )

    trader = Trader(Settings(dry_run=True, risk_state_path=str(state_path)))

    allowed = trader._check_risk(5.0, token_id="token-new", horizon="5m", direction="BUY")

    assert allowed is True
    assert trader.risk.open_exposure_usd_by_market == {f"token-active|15m|BUY|start:{active_start}": 20.0}
    assert trader.risk.total_open_notional_usd == 20.0


def test_dynamic_quote_size_scales_with_equity(tmp_path):
    settings = Settings(
        dry_run=True,
        risk_state_path=str(tmp_path / "risk_state.json"),
        equity_usd=1_000.0,
        risk_pct_per_trade=0.01,
        kelly_fraction=0.25,
        max_risk_pct_cap=0.02,
        max_usd_per_trade=100.0,
    )
    trader = Trader(settings)

    quote_without_edge = min(
        settings.max_usd_per_trade,
        trader._refresh_equity_cache() * min(settings.max_risk_pct_cap, settings.risk_pct_per_trade),
    )
    assert quote_without_edge == 10.0

    trader.risk.cumulative_realized_pnl = -500.0
    quote_after_loss = min(
        settings.max_usd_per_trade,
        trader._refresh_equity_cache(force=True) * min(settings.max_risk_pct_cap, settings.risk_pct_per_trade),
    )
    assert quote_after_loss == 5.0


def test_kelly_fraction_is_capped_by_hard_risk_limit(tmp_path):
    settings = Settings(
        dry_run=True,
        risk_state_path=str(tmp_path / "risk_state.json"),
        equity_usd=2_000.0,
        risk_pct_per_trade=0.01,
        kelly_fraction=0.25,
        max_risk_pct_cap=0.02,
        max_usd_per_trade=100.0,
    )
    trader = Trader(settings)

    p_hat = 0.95
    effective_cost = 0.40
    kelly_suggested = max(0.0, min(1.0, (p_hat - effective_cost) / (1.0 - effective_cost)))
    dynamic_risk_pct = min(
        settings.max_risk_pct_cap,
        max(settings.risk_pct_per_trade, kelly_suggested * settings.kelly_fraction),
    )

    assert kelly_suggested > 0.8
    assert dynamic_risk_pct == settings.max_risk_pct_cap
    assert min(settings.max_usd_per_trade, trader._refresh_equity_cache() * dynamic_risk_pct) == 40.0


def test_cooldown_blocks_risk_checks_after_consecutive_losses(tmp_path):
    settings = Settings(
        dry_run=True,
        risk_state_path=str(tmp_path / "risk_state.json"),
        cooldown_consecutive_losses=2,
        cooldown_drawdown_pct=0.50,
        cooldown_minutes=10,
        max_usd_per_trade=100.0,
    )
    trader = Trader(settings)

    trader._record_post_trade_updates(
        {"realized_pnl": -1.0, "fills": [{"price": 0.5, "size": 10}]},
        token_id="token-a",
        horizon="5m",
        direction="BUY",
        fallback_notional_usd=5.0,
    )
    assert trader._check_risk(5.0, token_id="token-b", horizon="5m", direction="BUY") is True

    trader._record_post_trade_updates(
        {"realized_pnl": -1.0, "fills": [{"price": 0.5, "size": 10}]},
        token_id="token-a",
        horizon="5m",
        direction="BUY",
        fallback_notional_usd=5.0,
    )

    assert trader.risk.cooldown_until_ts > 0
    assert trader._check_risk(5.0, token_id="token-c", horizon="5m", direction="BUY") is False
