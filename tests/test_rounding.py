import pytest

from utils.rounding import round_price_to_tick, round_size_to_step


def test_round_price_to_tick_floor() -> None:
    assert round_price_to_tick(0.4567, 0.01) == 0.45


def test_round_size_to_step_floor() -> None:
    assert round_size_to_step(12.987, 0.1) == 12.9


def test_invalid_rounding_inputs() -> None:
    with pytest.raises(ValueError):
        round_price_to_tick(1.0, 0)
    with pytest.raises(ValueError):
        round_size_to_step(1.0, -1)
