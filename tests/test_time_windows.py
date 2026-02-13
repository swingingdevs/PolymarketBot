from strategy.state_machine import StrategyStateMachine


def test_hammer_window_boundaries() -> None:
    sm = StrategyStateMachine(0.005, hammer_secs=15, d_min=5, max_entry_price=0.97, fee_bps=10)
    assert sm.in_hammer_window(100, 110)
    assert sm.in_hammer_window(100, 115)
    assert not sm.in_hammer_window(100, 116)
    assert not sm.in_hammer_window(120, 110)
