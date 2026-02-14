from __future__ import annotations

from main import enter_monitor_only_mode, recovery_window_satisfied, update_quorum_metrics
from metrics import KILL_SWITCH_ACTIVE, TRADING_ALLOWED
from strategy.execution_mode import ExecutionMode, get_execution_mode, set_execution_mode
from strategy.quorum_health import QuorumHealth
from strategy.state_machine import StrategyStateMachine


def test_fallback_transition_enters_monitor_only_and_disables_watch_mode() -> None:
    strategy = StrategyStateMachine(
        threshold=0.001,
        hammer_secs=15,
        d_min=1.0,
        max_entry_price=0.97,
        fee_bps=10.0,
    )
    strategy.watch_mode = True
    feed_state: dict[str, object] = {"mode": "rtds"}
    set_execution_mode(ExecutionMode.LIVE_TRADING)

    enter_monitor_only_mode(strategy, feed_state, now=100.0)

    assert get_execution_mode() == ExecutionMode.MONITOR_ONLY
    assert feed_state["mode"] == "fallback"
    assert strategy.watch_mode is False


def test_missing_divergence_data_keeps_kill_switch_active() -> None:
    q = QuorumHealth(chainlink_max_lag_seconds=30.0, spot_max_lag_seconds=30.0, min_spot_sources=2)
    q.update_chainlink(price=50000, payload_ts=100.0, received_ts=100.0)

    decision = q.evaluate(now=101.0)
    update_quorum_metrics(decision)

    assert decision.trading_allowed is False
    assert decision.divergence_data_available is False
    assert "DIVERGENCE_DATA_MISSING" in decision.reason_codes
    assert TRADING_ALLOWED._value.get() == 0
    assert KILL_SWITCH_ACTIVE._value.get() == 1


def test_recovery_requires_stabilization_window_and_min_updates() -> None:
    assert not recovery_window_satisfied(
        now=105.0,
        recovery_started_at=100.0,
        fresh_rtds_updates=5,
        stabilization_window_seconds=10.0,
        min_fresh_updates=3,
    )
    assert not recovery_window_satisfied(
        now=112.0,
        recovery_started_at=100.0,
        fresh_rtds_updates=2,
        stabilization_window_seconds=10.0,
        min_fresh_updates=3,
    )
    assert recovery_window_satisfied(
        now=112.0,
        recovery_started_at=100.0,
        fresh_rtds_updates=3,
        stabilization_window_seconds=10.0,
        min_fresh_updates=3,
    )
