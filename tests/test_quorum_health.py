from __future__ import annotations

from strategy.quorum_health import QuorumHealth


def _build_quorum() -> QuorumHealth:
    q = QuorumHealth(
        chainlink_max_lag_seconds=30.0,
        spot_max_lag_seconds=30.0,
        divergence_threshold_pct=0.5,
        divergence_sustain_seconds=5.0,
        min_spot_sources=2,
    )
    q.update_chainlink(price=50000, payload_ts=100.0, received_ts=100.0)
    q.update_spot(feed="binance", price=50000, payload_ts=100.0, received_ts=100.0)
    q.update_spot(feed="coinbase", price=50000, payload_ts=100.0, received_ts=100.0)
    return q


def test_divergence_blocks_until_recovery_window() -> None:
    q = _build_quorum()
    q.update_spot(feed="binance", price=49000, payload_ts=101.0, received_ts=101.0)
    q.update_spot(feed="coinbase", price=49000, payload_ts=101.0, received_ts=101.0)

    early = q.evaluate(now=104.0)
    assert early.trading_allowed is True
    assert "SPOT_DIVERGENCE_SUSTAINED" not in early.reason_codes

    blocked = q.evaluate(now=109.1)
    assert blocked.trading_allowed is False
    assert "SPOT_DIVERGENCE_SUSTAINED" in blocked.reason_codes
    assert blocked.divergence_pct is not None

    q.update_spot(feed="binance", price=50010, payload_ts=108.0, received_ts=108.0)
    q.update_spot(feed="coinbase", price=50020, payload_ts=108.0, received_ts=108.0)

    still_blocked = q.evaluate(now=112.0)
    assert still_blocked.trading_allowed is False
    assert "SPOT_DIVERGENCE_SUSTAINED" in still_blocked.reason_codes

    recovered = q.evaluate(now=122.1)
    assert recovered.trading_allowed is True
    assert "SPOT_DIVERGENCE_SUSTAINED" not in recovered.reason_codes


def test_quorum_drop_blocks_trading() -> None:
    q = _build_quorum()
    q.spot_samples.pop("coinbase")

    decision = q.evaluate(now=102.0)

    assert decision.trading_allowed is False
    assert "SPOT_QUORUM_UNAVAILABLE" in decision.reason_codes


def test_chainlink_staleness_immediately_blocks() -> None:
    q = QuorumHealth(chainlink_max_lag_seconds=2.5, spot_max_lag_seconds=30.0, min_spot_sources=2)
    q.update_chainlink(price=50000, payload_ts=100.0, received_ts=100.0)
    q.update_spot(feed="binance", price=50000, payload_ts=100.0, received_ts=100.0)
    q.update_spot(feed="coinbase", price=50000, payload_ts=100.0, received_ts=100.0)

    decision = q.evaluate(now=102.6)

    assert decision.trading_allowed is False
    assert "CHAINLINK_STALE" in decision.reason_codes


def test_no_flapping_near_divergence_threshold() -> None:
    q = QuorumHealth(
        chainlink_max_lag_seconds=30.0,
        spot_max_lag_seconds=30.0,
        divergence_threshold_pct=0.5,
        divergence_sustain_seconds=2.0,
        min_spot_sources=2,
    )
    q.update_chainlink(price=50000, payload_ts=100.0, received_ts=100.0)

    # oscillate around threshold without sustaining above it
    q.update_spot(feed="binance", price=49760, payload_ts=100.0, received_ts=100.0)
    q.update_spot(feed="coinbase", price=49760, payload_ts=100.0, received_ts=100.0)
    assert q.evaluate(now=101.0).trading_allowed is True

    q.update_spot(feed="binance", price=49740, payload_ts=101.1, received_ts=101.1)
    q.update_spot(feed="coinbase", price=49740, payload_ts=101.1, received_ts=101.1)
    assert q.evaluate(now=102.0).trading_allowed is True

    q.update_spot(feed="binance", price=49760, payload_ts=102.1, received_ts=102.1)
    q.update_spot(feed="coinbase", price=49760, payload_ts=102.1, received_ts=102.1)
    assert q.evaluate(now=103.0).trading_allowed is True

    # sustained high divergence should eventually block
    q.update_spot(feed="binance", price=49000, payload_ts=103.1, received_ts=103.1)
    q.update_spot(feed="coinbase", price=49000, payload_ts=103.1, received_ts=103.1)
    assert q.evaluate(now=104.0).trading_allowed is True

    blocked = q.evaluate(now=106.2)
    assert blocked.trading_allowed is False
    assert "SPOT_DIVERGENCE_SUSTAINED" in blocked.reason_codes
