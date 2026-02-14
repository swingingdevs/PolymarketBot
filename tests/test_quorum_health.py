from __future__ import annotations

from strategy.quorum_health import QuorumHealth


def test_quorum_blocks_without_two_spot_sources() -> None:
    q = QuorumHealth(chainlink_max_lag_seconds=5.0, spot_max_lag_seconds=5.0, min_spot_sources=2)
    q.update_chainlink(price=50000, payload_ts=100.0, received_ts=100.0)
    q.update_spot(feed="binance", price=50010, payload_ts=100.0, received_ts=100.0)

    decision = q.evaluate(now=101.0)
    assert decision.trading_allowed is False
    assert "SPOT_QUORUM_UNAVAILABLE" in decision.reason_codes


def test_quorum_divergence_requires_persistence() -> None:
    q = QuorumHealth(
        chainlink_max_lag_seconds=30.0,
        spot_max_lag_seconds=30.0,
        divergence_threshold_pct=0.5,
        divergence_sustain_seconds=5.0,
    )
    q.update_chainlink(price=50000, payload_ts=100.0, received_ts=100.0)
    q.update_spot(feed="binance", price=49000, payload_ts=100.0, received_ts=100.0)
    q.update_spot(feed="coinbase", price=49000, payload_ts=100.0, received_ts=100.0)

    early = q.evaluate(now=102.0)
    assert early.trading_allowed is True

    sustained = q.evaluate(now=108.0)
    assert sustained.trading_allowed is False
    assert "SPOT_DIVERGENCE_SUSTAINED" in sustained.reason_codes


def test_quorum_recovers_when_feeds_realign() -> None:
    q = QuorumHealth(
        chainlink_max_lag_seconds=30.0,
        spot_max_lag_seconds=30.0,
        divergence_threshold_pct=0.5,
        divergence_sustain_seconds=2.0,
    )
    q.update_chainlink(price=50000, payload_ts=100.0, received_ts=100.0)
    q.update_spot(feed="binance", price=49000, payload_ts=100.0, received_ts=100.0)
    q.update_spot(feed="coinbase", price=49000, payload_ts=100.0, received_ts=100.0)
    q.evaluate(now=101.0)
    blocked = q.evaluate(now=104.0)
    assert blocked.trading_allowed is False

    q.update_spot(feed="binance", price=50010, payload_ts=104.5, received_ts=104.5)
    q.update_spot(feed="coinbase", price=50020, payload_ts=104.5, received_ts=104.5)
    recovered = q.evaluate(now=105.0)
    assert recovered.trading_allowed is True
    assert recovered.reason_codes == []
