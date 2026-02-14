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
