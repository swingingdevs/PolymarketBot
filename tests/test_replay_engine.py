from markets.gamma_cache import UpDownMarket
from strategy.replay_engine import ReplayEngine


def _params() -> dict[str, float | int]:
    return {
        "watch_return_threshold": 0.001,
        "hammer_secs": 60,
        "d_min": 1.0,
        "max_entry_price": 0.99,
        "fee_bps": 10.0,
    }


def test_replay_engine_latency_impacts_fill_outcome() -> None:
    t0 = 1_710_000_000
    markets = [UpDownMarket("m5", t0, t0 + 120, "u", "d", 5)]
    events = [
        {"type": "rtds_price", "ts": t0, "price": 50_000, "payload_ts": t0},
        {"type": "rtds_price", "ts": t0 + 120, "price": 50_100, "payload_ts": t0 + 120},
        {
            "type": "clob_book",
            "ts": t0 + 10,
            "token_id": "u",
            "bids_levels": [{"price": 0.44, "size": 100}],
            "asks_levels": [{"price": 0.45, "size": 100}],
        },
        {
            "type": "clob_book",
            "ts": t0 + 11,
            "token_id": "u",
            "bids_levels": [{"price": 0.44, "size": 0.1}],
            "asks_levels": [{"price": 0.45, "size": 0.1}],
        },
        {"type": "decision", "ts": t0 + 10, "token_id": "u", "ask": 0.45, "fee_cost": 0.001, "notional": 20.0},
    ]

    zero_latency = ReplayEngine(markets, order_latency_ms=0)
    slow_latency = ReplayEngine(markets, order_latency_ms=1000)

    trades_fast, summary_fast = zero_latency.run(events, _params())
    trades_slow, summary_slow = slow_latency.run(events, _params())

    assert len(trades_fast) == 1
    assert trades_fast[0].ok is True
    assert summary_fast.fok_fail_pct == 0.0

    assert len(trades_slow) == 1
    assert trades_slow[0].ok is False
    assert summary_slow.fok_fail_pct == 1.0
