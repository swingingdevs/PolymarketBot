from markets.gamma_cache import UpDownMarket
from strategy.state_machine import StrategyStateMachine


def test_best_ev_selection_prefers_highest_positive() -> None:
    sm = StrategyStateMachine(0.005, hammer_secs=15, d_min=1.0, max_entry_price=0.99, fee_bps=0)

    # Seed prices for sigma estimate and current price.
    t0 = 1_710_000_000
    base = 50_000.0
    for i in range(61):
        sm.on_price(t0 + i, base + i * 2)

    sm.start_prices[300] = base - 100
    sm.start_prices[900] = base - 100

    m5 = UpDownMarket("m5", t0 - 285, t0 + 15, "u5", "d5", 5)
    m15 = UpDownMarket("m15", t0 - 885, t0 + 15, "u15", "d15", 15)

    sm.on_book("u5", 0.3, 0.40)
    sm.on_book("d5", 0.3, 0.60)
    sm.on_book("u15", 0.3, 0.45)
    sm.on_book("d15", 0.3, 0.70)

    best = sm.pick_best(t0, [m5, m15], {})
    assert best is not None
    assert best.direction == "UP"
    assert best.ask in {0.40, 0.45}
    assert best.ev > 0
