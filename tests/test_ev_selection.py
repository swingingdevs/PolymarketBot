from markets.gamma_cache import UpDownMarket
from strategy.state_machine import StrategyStateMachine


def _seed_state(sm: StrategyStateMachine, t0: int, base: float = 50_000.0) -> None:
    for i in range(61):
        sm.on_price(t0 + i, base + i * 2)
    sm.start_prices[300] = base - 100
    sm.start_prices[900] = base - 100


def test_best_ev_selection_prefers_highest_positive() -> None:
    sm = StrategyStateMachine(0.005, hammer_secs=15, d_min=1.0, max_entry_price=0.99, fee_bps=0)

    t0 = 1_710_000_000
    _seed_state(sm, t0)

    m5 = UpDownMarket("m5", t0 - 285, t0 + 15, "u5", "d5", 5)
    m15 = UpDownMarket("m15", t0 - 885, t0 + 15, "u15", "d15", 15)

    sm.on_book("u5", 0.3, 0.40, fill_prob=0.95, ask_size=3.0, ts=t0 + 1)
    sm.on_book("d5", 0.3, 0.60, fill_prob=0.95, ask_size=3.0, ts=t0 + 1)
    sm.on_book("u15", 0.3, 0.45, fill_prob=0.95, ask_size=3.0, ts=t0 + 1)
    sm.on_book("d15", 0.3, 0.70, fill_prob=0.95, ask_size=3.0, ts=t0 + 1)

    best = sm.pick_best(t0, [m5, m15], {})
    assert best is not None
    assert best.direction == "UP"
    assert best.ask in {0.40, 0.45}
    assert best.ev > 0
    assert best.ev_exec > 0


def test_fill_probability_can_change_candidate_ranking() -> None:
    sm = StrategyStateMachine(0.005, hammer_secs=15, d_min=1.0, max_entry_price=0.99, fee_bps=0)

    t0 = 1_710_000_000
    _seed_state(sm, t0)

    m5 = UpDownMarket("m5", t0 - 285, t0 + 15, "u5", "d5", 5)
    m15 = UpDownMarket("m15", t0 - 885, t0 + 15, "u15", "d15", 15)

    # Better raw edge on u5, but poor fill probability should push it below u15.
    sm.on_book("u5", 0.39, 0.40, fill_prob=0.15, ask_size=3.0, ts=t0 + 1)
    sm.on_book("u15", 0.44, 0.45, fill_prob=0.90, ask_size=3.0, ts=t0 + 1)

    best = sm.pick_best(t0, [m5, m15], {})
    assert best is not None
    assert best.token_id == "u15"
    assert best.fill_prob == 0.90


def test_slippage_penalty_can_change_candidate_ranking() -> None:
    sm = StrategyStateMachine(0.005, hammer_secs=15, d_min=1.0, max_entry_price=0.99, fee_bps=0)

    t0 = 1_710_000_000
    _seed_state(sm, t0)

    m5 = UpDownMarket("m5", t0 - 285, t0 + 15, "u5", "d5", 5)
    m15 = UpDownMarket("m15", t0 - 885, t0 + 15, "u15", "d15", 15)

    # u5 has slightly better ask but much wider spread and thin depth.
    sm.on_book("u5", 0.30, 0.40, fill_prob=0.95, ask_size=0.1, ts=t0 + 1)
    sm.on_book("u15", 0.44, 0.45, fill_prob=0.95, ask_size=3.0, ts=t0 + 1)

    best = sm.pick_best(t0, [m5, m15], {})
    assert best is not None
    assert best.token_id == "u15"
    assert best.slippage_cost > 0
