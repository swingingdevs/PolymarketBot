from __future__ import annotations

import json

from execution import order_builder
from markets.gamma_cache import UpDownMarket
from markets.token_metadata_cache import TokenMetadata, TokenMetadataCache
from strategy.state_machine import StrategyStateMachine


class _FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class _FakeClient:
    def __init__(self) -> None:
        self.orders: list[dict[str, object]] = []

    def create_limit_order(self, **kwargs):
        if "feeRateBps" not in kwargs:
            raise RuntimeError("rejected_missing_feeRateBps")
        self.orders.append(kwargs)
        return kwargs


def _seed(sm: StrategyStateMachine, t0: int) -> None:
    for i in range(61):
        sm.on_price(t0 + i, 50_000 + i * 2)
    sm.start_prices[300] = 49_900


def test_fetch_fee_rate_uses_cache(monkeypatch) -> None:
    calls = []

    def _fake_urlopen(url: str, timeout: float):
        del timeout
        calls.append(url)
        return _FakeResponse({"feeRateBps": 55})

    order_builder._FEE_RATE_CACHE.clear()
    order_builder.configure_fee_rate_fetcher(base_url="https://clob.polymarket.com", ttl_seconds=60)
    monkeypatch.setattr(order_builder, "urlopen", _fake_urlopen)

    first = order_builder.fetch_fee_rate("token-1")
    second = order_builder.fetch_fee_rate("token-1")

    assert first == 55
    assert second == 55
    assert len(calls) == 1


def test_order_builder_includes_fee_rate_in_payload() -> None:
    fake_client = _FakeClient()
    builder = order_builder.OrderBuilder(fake_client, enable_fee_rate=False, default_fee_rate_bps=33)

    payload, used_fallback, fee_rate_bps = builder.build_signed_order(token_id="token-1", price=0.5, size=10)

    assert used_fallback is False
    assert fee_rate_bps == 33
    assert payload["feeRateBps"] == 33


def test_ev_after_fees_blocks_trade_when_negative() -> None:
    cache = TokenMetadataCache(ttl_seconds=300)
    cache.put("u5", TokenMetadata(fee_rate_bps=1500))

    sm = StrategyStateMachine(
        0.005,
        hammer_secs=15,
        d_min=1.0,
        max_entry_price=0.99,
        fee_bps=0,
        token_metadata_cache=cache,
        expected_notional_usd=20.0,
        enable_fee_rate=True,
        default_fee_rate_bps=1500,
    )

    t0 = 1_710_000_000
    _seed(sm, t0)
    market = UpDownMarket("m5", t0 - 285, t0 + 15, "u5", "d5", 5)
    sm.on_book("u5", 0.3, 0.4, fill_prob=1.0, ask_size=60.0, asks_levels=[(0.4, 60.0)], ts=t0 + 1)

    best = sm.pick_best(t0, [market], {})

    assert best is not None
    assert best.fee_cost > 0
    assert best.ev <= 0
