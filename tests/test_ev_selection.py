import pytest
from markets.gamma_cache import UpDownMarket
from markets.token_metadata_cache import TokenMetadata, TokenMetadataCache
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


def test_depth_penalty_scales_with_displayed_size_shortfall() -> None:
    sm = StrategyStateMachine(
        0.005,
        hammer_secs=15,
        d_min=1.0,
        max_entry_price=0.99,
        fee_bps=0,
        expected_notional_usd=20.0,
        depth_penalty_coeff=2.0,
    )

    t0 = 1_710_000_000
    _seed_state(sm, t0)
    m5 = UpDownMarket("m5", t0 - 285, t0 + 15, "u5", "d5", 5)

    # Same spread in both cases, but lower displayed size should incur a bigger depth penalty.
    high_depth = sm._candidate_ev(m5, "UP", ask=0.40, bid=0.35, ask_size=60.0, fill_prob=1.0)
    low_depth = sm._candidate_ev(m5, "UP", ask=0.40, bid=0.35, ask_size=5.0, fill_prob=1.0)

    assert high_depth is not None
    assert low_depth is not None
    assert low_depth.slippage_cost > high_depth.slippage_cost
    assert low_depth.ev_exec < high_depth.ev_exec


def test_candidate_ev_has_sufficient_depth_keeps_fill_probability() -> None:
    sm = StrategyStateMachine(
        0.005, hammer_secs=15, d_min=1.0, max_entry_price=0.99, fee_bps=0, expected_notional_usd=20.0
    )

    t0 = 1_710_000_000
    _seed_state(sm, t0)
    m5 = UpDownMarket("m5", t0 - 285, t0 + 15, "u5", "d5", 5)

    candidate = sm._candidate_ev(m5, "UP", ask=0.40, bid=0.35, ask_size=60.0, fill_prob=0.9)

    assert candidate is not None
    assert candidate.fill_prob == 0.9


def test_candidate_ev_forces_fill_probability_to_zero_when_depth_is_insufficient() -> None:
    sm = StrategyStateMachine(
        0.005, hammer_secs=15, d_min=1.0, max_entry_price=0.99, fee_bps=0, expected_notional_usd=20.0
    )

    t0 = 1_710_000_000
    _seed_state(sm, t0)
    m5 = UpDownMarket("m5", t0 - 285, t0 + 15, "u5", "d5", 5)

    candidate = sm._candidate_ev(m5, "UP", ask=0.40, bid=0.35, ask_size=49.0, fill_prob=0.9)

    assert candidate is not None
    assert candidate.fill_prob == 0.0
    assert candidate.ev == 0.0


def test_candidate_ev_depth_boundary_uses_equality_as_sufficient_depth() -> None:
    sm = StrategyStateMachine(
        0.005, hammer_secs=15, d_min=1.0, max_entry_price=0.99, fee_bps=0, expected_notional_usd=20.0
    )

    t0 = 1_710_000_000
    _seed_state(sm, t0)
    m5 = UpDownMarket("m5", t0 - 285, t0 + 15, "u5", "d5", 5)

    # required_shares = quote_size_usd / ask = 20 / 0.4 = 50
    candidate = sm._candidate_ev(m5, "UP", ask=0.40, bid=0.35, ask_size=50.0, fill_prob=0.9)

    assert candidate is not None
    assert candidate.fill_prob == 0.9


def test_candidate_ev_uses_token_fee_rate_with_global_fallback() -> None:
    cache = TokenMetadataCache(ttl_seconds=300)
    cache.put("u5", TokenMetadata(tick_size=0.01, fee_rate_bps=40))

    sm = StrategyStateMachine(
        0.005,
        hammer_secs=15,
        d_min=1.0,
        max_entry_price=0.99,
        fee_bps=10,
        token_metadata_cache=cache,
    )

    t0 = 1_710_000_000
    _seed_state(sm, t0)
    m5 = UpDownMarket("m5", t0 - 285, t0 + 15, "u5", "d5", 5)

    with_token_fee = sm._candidate_ev(m5, "UP", ask=0.4, bid=0.39, ask_size=3.0, fill_prob=1.0, token_id="u5")
    with_fallback_fee = sm._candidate_ev(m5, "UP", ask=0.4, bid=0.39, ask_size=3.0, fill_prob=1.0, token_id="missing")

    assert with_token_fee is not None
    assert with_fallback_fee is not None
    assert with_token_fee.fee_cost == 0.0016
    assert with_fallback_fee.fee_cost == 0.001


def test_candidate_ev_fee_is_price_dependent_for_fee_enabled_markets() -> None:
    cache = TokenMetadataCache(ttl_seconds=300)
    cache.put("u5", TokenMetadata(fee_rate_bps=100))

    sm = StrategyStateMachine(
        0.005,
        hammer_secs=15,
        d_min=1.0,
        max_entry_price=0.99,
        fee_bps=10,
        token_metadata_cache=cache,
    )

    t0 = 1_710_000_000
    _seed_state(sm, t0)
    m5 = UpDownMarket("m5", t0 - 285, t0 + 15, "u5", "d5", 5)

    low_price = sm._candidate_ev(m5, "UP", ask=0.1, bid=0.09, ask_size=3.0, fill_prob=1.0, token_id="u5")
    mid_price = sm._candidate_ev(m5, "UP", ask=0.5, bid=0.49, ask_size=3.0, fill_prob=1.0, token_id="u5")

    assert low_price is not None
    assert mid_price is not None
    assert low_price.fee_cost == 0.001
    assert mid_price.fee_cost == 0.005
    assert mid_price.fee_cost > low_price.fee_cost


def test_candidate_ev_fee_is_symmetric_at_complementary_prices() -> None:
    cache = TokenMetadataCache(ttl_seconds=300)
    cache.put_many({"u5": TokenMetadata(fee_rate_bps=100), "d5": TokenMetadata(fee_rate_bps=100)})

    sm = StrategyStateMachine(
        0.005,
        hammer_secs=15,
        d_min=1.0,
        max_entry_price=0.99,
        fee_bps=10,
        token_metadata_cache=cache,
    )

    t0 = 1_710_000_000
    _seed_state(sm, t0)
    m5 = UpDownMarket("m5", t0 - 285, t0 + 15, "u5", "d5", 5)

    up_candidate = sm._candidate_ev(m5, "UP", ask=0.2, bid=0.19, ask_size=3.0, fill_prob=1.0, token_id="u5")
    down_candidate = sm._candidate_ev(m5, "DOWN", ask=0.8, bid=0.79, ask_size=3.0, fill_prob=1.0, token_id="d5")

    assert up_candidate is not None
    assert down_candidate is not None
    assert up_candidate.fee_cost == pytest.approx(down_candidate.fee_cost)
