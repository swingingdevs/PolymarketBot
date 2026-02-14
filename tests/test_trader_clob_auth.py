from __future__ import annotations

import asyncio
from types import SimpleNamespace

from execution import trader as trader_module
from execution.trader import Trader


class _FakeClobClient:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.creds = None
        self.derived_creds = {"api_key": "dk", "api_secret": "ds", "api_passphrase": "dp"}
        self.derive_called = False
        self.create_market_order_called = False
        self.post_order_called = False

    def create_or_derive_api_creds(self):
        self.derive_called = True
        return self.derived_creds

    def set_api_creds(self, creds) -> None:
        self.creds = creds

    def create_market_order(self, **kwargs):
        self.create_market_order_called = True
        return kwargs

    def post_order(self, order, time_in_force="FOK"):
        self.post_order_called = True
        return {"ok": True, "order": order, "time_in_force": time_in_force}


def _build_settings(tmp_path, **overrides):
    base = {
        "dry_run": True,
        "risk_state_path": str(tmp_path / "risk_state.json"),
        "clob_host": "https://clob.polymarket.com",
        "chain_id": 137,
        "private_key": "0xabc",
        "signature_type": None,
        "funder": "",
        "api_key": "",
        "api_secret": "",
        "api_passphrase": "",
        "quote_size_usd": 20.0,
        "max_usd_per_trade": 100.0,
        "max_daily_loss": 250.0,
        "max_trades_per_hour": 4,
        "max_open_exposure_per_market": 500.0,
        "max_total_open_exposure": 5000.0,
        "exposure_reconcile_every_n_trades": 10,
        "order_submit_timeout_seconds": 1.0,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_dry_run_does_not_require_l2_creds(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(trader_module, "ClobClient", _FakeClobClient)

    trader = Trader(_build_settings(tmp_path, dry_run=True, api_key="", api_secret="", api_passphrase=""))

    assert trader.client is None
    assert trader._live_auth_ready is False


def test_live_mode_initializes_l2_auth(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(trader_module, "ClobClient", _FakeClobClient)

    trader = Trader(_build_settings(tmp_path, dry_run=False, api_key="k", api_secret="s", api_passphrase="p", signature_type=2, funder="0xfunder"))

    assert isinstance(trader.client, _FakeClobClient)
    assert trader.client.kwargs.get("signature_type") == 2
    assert trader.client.kwargs.get("funder") == "0xfunder"
    assert trader._live_auth_ready is True
    assert trader.client.derive_called is True
    assert trader.client.creds is not None


def test_live_order_blocked_when_l2_creds_missing(monkeypatch, tmp_path) -> None:
    class _NoCredsClobClient(_FakeClobClient):
        def create_or_derive_api_creds(self):
            self.derive_called = True
            return None

    monkeypatch.setattr(trader_module, "ClobClient", _NoCredsClobClient)
    logger_calls = []

    class _Logger:
        def error(self, event, **kwargs):
            logger_calls.append((event, kwargs))

        def info(self, *args, **kwargs):
            return None

        def warning(self, *args, **kwargs):
            return None

    monkeypatch.setattr(trader_module, "logger", _Logger())

    trader = Trader(_build_settings(tmp_path, dry_run=False, api_key="", api_secret="", api_passphrase=""))
    ok = asyncio.run(trader.buy_fok("token-1", ask=0.5, horizon="5m"))

    assert ok is False
    assert any(call[0] == "missing_clob_l2_credentials" for call in logger_calls)
    assert trader.client.post_order_called is False
