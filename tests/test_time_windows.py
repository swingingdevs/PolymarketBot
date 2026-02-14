import pytest

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


def _build_state_machine_for_start_price_tests() -> StrategyStateMachine:
    StrategyStateMachine.rolling_window_seconds = 60
    StrategyStateMachine.watch_mode_expiry_seconds = 60
    StrategyStateMachine.watch_zscore_threshold = 0.0
    return StrategyStateMachine(0.5, hammer_secs=15, d_min=5, max_entry_price=0.97, fee_bps=10)


def test_start_price_updates_on_first_tick_after_5m_boundary() -> None:
    sm = _build_state_machine_for_start_price_tests()

    sm.on_price(299.2, 100.0, metadata={"source": "chainlink_rtds", "timestamp": 299.2})
    assert sm.start_prices[300] == 100.0
    assert sm.start_price_metadata[300] == {
        "price": 100.0,
        "timestamp": 299.2,
        "source": "chainlink_rtds",
    }

    sm.on_price(301.7, 101.5, metadata={"source": "chainlink_rtds", "timestamp": 301.7})

    assert sm.start_prices[300] == 101.5
    assert sm.start_price_metadata[300] == {
        "price": 101.5,
        "timestamp": 301.7,
        "source": "chainlink_rtds",
    }


def test_start_price_updates_on_first_tick_after_15m_boundary() -> None:
    sm = _build_state_machine_for_start_price_tests()

    sm.on_price(899.1, 200.0, metadata={"source": "chainlink_rtds", "timestamp": 899.1})
    assert sm.start_prices[900] == 200.0
    assert sm.start_price_metadata[900] == {
        "price": 200.0,
        "timestamp": 899.1,
        "source": "chainlink_rtds",
    }

    sm.on_price(901.4, 199.25, metadata={"source": "chainlink_rtds", "timestamp": 901.4})

    assert sm.start_prices[900] == 199.25
    assert sm.start_price_metadata[900] == {
        "price": 199.25,
        "timestamp": 901.4,
        "source": "chainlink_rtds",
    }


def test_on_price_uses_configured_price_stale_after_seconds(monkeypatch: pytest.MonkeyPatch) -> None:
    seen_thresholds: list[float] = []

    def fake_is_price_stale(timestamp: float, stale_after_seconds: float = 2.0) -> bool:
        seen_thresholds.append(stale_after_seconds)
        return False

    monkeypatch.setattr("strategy.state_machine.is_price_stale", fake_is_price_stale)

    sm = StrategyStateMachine(
        0.005,
        hammer_secs=15,
        d_min=5,
        max_entry_price=0.97,
        fee_bps=10,
        price_stale_after_seconds=7.5,
    )

    sm.on_price(100.0, 50000.0, metadata={"source": "chainlink_rtds", "timestamp": 100.0})

    assert seen_thresholds == [7.5]


def _legacy_rolling_returns(rows: list[tuple[int, float]]) -> list[float]:
    rets: list[float] = []
    for idx in range(1, len(rows)):
        prev_price = rows[idx - 1][1]
        curr_price = rows[idx][1]
        if prev_price <= 0:
            continue
        rets.append((curr_price / prev_price) - 1)
    return rets


def _legacy_sigma1(rows: list[tuple[int, float]]) -> float:
    if len(rows) < 61:
        return 0.0
    last_rows = rows[-61:]
    rets: list[float] = []
    for idx in range(1, len(last_rows)):
        prev_price = last_rows[idx - 1][1]
        curr_price = last_rows[idx][1]
        if prev_price > 0:
            rets.append((curr_price / prev_price) - 1)
    if not rets:
        return 0.0
    mean = sum(rets) / len(rets)
    var = sum((x - mean) ** 2 for x in rets) / max(1, len(rets) - 1)
    return (max(var, 1e-12)) ** 0.5


def test_rolling_aggregates_match_legacy_deterministically() -> None:
    sm = StrategyStateMachine(
        0.5,
        hammer_secs=15,
        d_min=5,
        max_entry_price=0.97,
        fee_bps=10,
        rolling_window_seconds=8,
        watch_zscore_threshold=1.0,
        watch_mode_expiry_seconds=999,
    )

    t0 = 1_710_000_000
    prices = [
        100.0,
        100.2,
        99.8,
        100.4,
        99.9,
        100.1,
        101.0,
        0.0,
        100.0,
        100.3,
        100.7,
        100.6,
    ]

    for idx, price in enumerate(prices):
        ts = t0 + idx
        sm.on_price(ts, price)

        rows = list(sm.prices_1s)
        legacy_rets = _legacy_rolling_returns(rows)
        assert sm._rolling_returns() == pytest.approx(legacy_rets, abs=1e-15)

        latest_ret = sm.rolling_returns[-1] if sm.rolling_returns else None
        if len(legacy_rets) >= 2 and latest_ret is not None:
            legacy_mean = sum(legacy_rets) / len(legacy_rets)
            legacy_var = sum((x - legacy_mean) ** 2 for x in legacy_rets) / max(1, len(legacy_rets) - 1)
            legacy_std = legacy_var**0.5
            if legacy_std > 0:
                legacy_z = abs((legacy_rets[-1] - legacy_mean) / legacy_std)
                agg_z = abs((latest_ret - sm.rolling_return_stats.mean) / sm.rolling_return_stats.stddev())
                assert agg_z == pytest.approx(legacy_z, rel=1e-12, abs=1e-12)

        assert sm._sigma1() == pytest.approx(_legacy_sigma1(rows), rel=1e-12, abs=1e-12)
