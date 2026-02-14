from pathlib import Path

from markets.gamma_cache import UpDownMarket
from strategy.calibration import IdentityCalibrator, IsotonicCalibrator, load_probability_calibrator
from strategy.state_machine import StrategyStateMachine


class ConstantCalibrator:
    def calibrate(self, value: float) -> float:
        return 0.2


def test_isotonic_output_is_monotonic() -> None:
    calibrator = IsotonicCalibrator(x=[0.0, 0.5, 1.0], y=[0.1, 0.4, 0.9])
    outputs = [calibrator.calibrate(v) for v in [0.1, 0.2, 0.6, 0.8]]
    assert outputs == sorted(outputs)


def test_missing_isotonic_params_falls_back_to_identity(tmp_path: Path) -> None:
    missing_path = tmp_path / "missing.json"
    calibrator = load_probability_calibrator(
        method="isotonic",
        params_path=str(missing_path),
        logistic_coef=1.0,
        logistic_intercept=0.0,
    )
    assert isinstance(calibrator, IdentityCalibrator)
    assert calibrator.calibrate(1.5) == 1.0


def test_candidate_ev_uses_calibrated_probability() -> None:
    sm = StrategyStateMachine(
        0.005,
        hammer_secs=15,
        d_min=1.0,
        max_entry_price=0.99,
        fee_bps=0,
        probability_calibrator=ConstantCalibrator(),
    )
    t0 = 1_710_000_000
    base = 50_000.0
    for i in range(61):
        sm.on_price(t0 + i, base + i * 2)

    sm.start_prices[300] = base - 100
    market = UpDownMarket("m5", t0 - 285, t0 + 15, "u5", "d5", 5)
    candidate = sm._candidate_ev(market, "UP", ask=0.1, asks_levels=[(0.1, 100.0)])

    assert candidate is not None
    assert candidate.p_hat == 0.2
    assert candidate.ev == 0.1
