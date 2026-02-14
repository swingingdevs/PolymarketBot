from __future__ import annotations

from types import SimpleNamespace

import asyncio

import pytest

from execution.trader import BatchOrderLeg, Trader


class _FakeBatchClient:
    def __init__(self):
        self.post_orders_calls = []
        self.post_order_calls = []

    def create_limit_order(self, **kwargs):
        return kwargs

    def post_orders(self, orders, atomic=True, time_in_force="FOK"):
        self.post_orders_calls.append(
            {
                "orders": orders,
                "atomic": atomic,
                "time_in_force": time_in_force,
            }
        )
        return [{"ok": True, "order": order} for order in orders]

    def post_order(self, order, time_in_force="FOK"):
        self.post_order_calls.append({"order": order, "time_in_force": time_in_force})
        return {"ok": True, "order": order}


class _FakeSequentialClient:
    def __init__(self):
        self.post_order_calls = []

    def create_limit_order(self, **kwargs):
        return kwargs

    def post_order(self, order, time_in_force="FOK"):
        self.post_order_calls.append({"order": order, "time_in_force": time_in_force})
        return {"ok": True, "order": order}


def _settings(tmp_path, **overrides):
    base = {
        "dry_run": False,
        "risk_state_path": str(tmp_path / "risk_state.json"),
        "batch_orders_enabled": True,
        "batch_order_max_size": 3,
        "order_submit_timeout_seconds": 1.0,
        "enable_fee_rate": False,
        "default_fee_rate_bps": 0.0,
        "equity_usd": 1000.0,
        "fee_rate_ttl_seconds": 60.0,
        "clob_host": "https://clob.polymarket.com",
        "max_daily_loss_usd": 250.0,
        "max_daily_loss_pct": 0.05,
        "max_open_exposure_per_market_usd": 100.0,
        "max_open_exposure_per_market_pct": 0.05,
        "max_total_open_exposure_usd": 500.0,
        "max_total_open_exposure_pct": 0.15,
        "settings_profile": "paper",
        "watch_return_threshold": 0.004,
        "hammer_secs": 20,
        "d_min": 4.0,
        "max_entry_price": 0.95,
        "fee_bps": 8.0,
        "divergence_threshold_pct": 0.5,
        "divergence_sustain_seconds": 5.0,
        "chainlink_max_lag_seconds": 2.5,
        "spot_max_lag_seconds": 2.5,
        "spot_quorum_min_sources": 2,
        "rtds_recovery_stabilization_seconds": 10.0,
        "rtds_recovery_min_fresh_updates": 3,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_batch_submission_uses_client_batch_endpoint(tmp_path):
    trader = Trader(_settings(tmp_path))
    trader.client = _FakeBatchClient()
    trader._live_auth_ready = True

    legs = [
        BatchOrderLeg(token_id="1", price=0.45, size=10),
        BatchOrderLeg(token_id="2", price=0.55, size=11),
    ]

    result = asyncio.run(trader.submit_fok_batch(legs, atomic=True))

    assert result.ok is True
    assert result.used_batch_endpoint is True
    assert trader.client.post_orders_calls
    payload = trader.client.post_orders_calls[0]
    assert payload["atomic"] is True
    assert payload["time_in_force"] == "FOK"
    assert len(payload["orders"]) == 2
    assert payload["orders"][0]["token_id"] == "1"
    assert payload["orders"][1]["token_id"] == "2"


def test_batch_submission_enforces_max_size(tmp_path):
    trader = Trader(_settings(tmp_path, batch_order_max_size=2))
    trader.client = _FakeBatchClient()
    trader._live_auth_ready = True

    legs = [
        BatchOrderLeg(token_id="1", price=0.45, size=10),
        BatchOrderLeg(token_id="2", price=0.55, size=11),
        BatchOrderLeg(token_id="3", price=0.65, size=12),
    ]

    with pytest.raises(ValueError, match="batch_order_max_size_exceeded"):
        asyncio.run(trader.submit_fok_batch(legs, atomic=True))


def test_batch_submission_falls_back_to_sequential_when_supported(tmp_path):
    trader = Trader(_settings(tmp_path))
    trader.client = _FakeSequentialClient()
    trader._live_auth_ready = True

    legs = [
        BatchOrderLeg(token_id="1", price=0.45, size=10),
        BatchOrderLeg(token_id="2", price=0.55, size=11),
    ]

    result = asyncio.run(trader.submit_fok_batch(legs, atomic=False))

    assert result.ok is True
    assert result.used_batch_endpoint is False
    assert len(trader.client.post_order_calls) == 2
