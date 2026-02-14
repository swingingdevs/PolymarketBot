from strategy.state_machine import StrategyStateMachine


def test_hammer_window_boundaries() -> None:
    sm = StrategyStateMachine(0.005, hammer_secs=15, d_min=5, max_entry_price=0.97, fee_bps=10)
    assert sm.in_hammer_window(100, 110)
    assert sm.in_hammer_window(100, 115)
    assert not sm.in_hammer_window(100, 116)
    assert not sm.in_hammer_window(120, 110)


def test_watch_mode_triggers_from_rolling_return_before_minute_boundary() -> None:
    sm = StrategyStateMachine(
        0.005,
        hammer_secs=15,
        d_min=5,
        max_entry_price=0.97,
        fee_bps=10,
        rolling_window_seconds=60,
        watch_mode_expiry_seconds=120,
    )

    t0 = 1_710_000_000
    sm.on_price(t0, 100.0)
    sm.on_price(t0 + 10, 100.6)

    assert sm.watch_mode
    assert sm.watch_mode_started_at == t0 + 10


def test_watch_mode_expires_after_timeout_without_candidate() -> None:
    sm = StrategyStateMachine(
        0.005,
        hammer_secs=15,
        d_min=5,
        max_entry_price=0.97,
        fee_bps=10,
        rolling_window_seconds=60,
        watch_mode_expiry_seconds=5,
    )

    t0 = 1_710_000_000
    sm.on_price(t0, 100.0)
    sm.on_price(t0 + 1, 100.7)
    assert sm.watch_mode

    sm.on_price(t0 + 7, 100.71)

    assert not sm.watch_mode
    assert sm.watch_mode_started_at is None


def test_watch_mode_triggers_from_zscore_without_large_window_return() -> None:
    sm = StrategyStateMachine(
        0.10,
        hammer_secs=15,
        d_min=5,
        max_entry_price=0.97,
        fee_bps=10,
        rolling_window_seconds=60,
        watch_zscore_threshold=2.0,
        watch_mode_expiry_seconds=60,
    )

    t0 = 1_710_000_000
    prices = [100.0, 100.1, 100.2, 100.3, 100.4, 100.5, 99.9]
    for idx, px in enumerate(prices):
        sm.on_price(t0 + idx, px)

    assert sm.watch_mode
    assert sm.watch_mode_started_at == t0 + len(prices) - 1
