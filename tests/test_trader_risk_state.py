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
        max_total_open_exposure=1_000.0,
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
        max_usd_per_trade=100.0,
        max_total_open_exposure=100.0,
        risk_state_path=str(tmp_path / "risk_state.json"),
    )
    trader = Trader(settings)

    trader._apply_open_exposure_delta("t1", "5m", "BUY", 60.0)
    trader._apply_open_exposure_delta("t2", "15m", "BUY", 30.0)

    assert trader._check_risk(notional_usd=15.0, token_id="t3", horizon="30m", direction="BUY") is False
