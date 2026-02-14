from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import structlog

logger = structlog.get_logger(__name__)

CalibrationInput = Literal["p_hat", "z_score"]
CalibrationMethod = Literal["none", "logistic", "isotonic"]


class ProbabilityCalibrator:
    def calibrate(self, value: float) -> float:
        raise NotImplementedError


@dataclass(slots=True)
class IdentityCalibrator(ProbabilityCalibrator):
    def calibrate(self, value: float) -> float:
        return min(1.0, max(0.0, value))


@dataclass(slots=True)
class LogisticCalibrator(ProbabilityCalibrator):
    coef: float
    intercept: float

    def calibrate(self, value: float) -> float:
        logit = (self.coef * value) + self.intercept
        return 1.0 / (1.0 + math.exp(-logit))


@dataclass(slots=True)
class IsotonicCalibrator(ProbabilityCalibrator):
    x: list[float]
    y: list[float]

    def __post_init__(self) -> None:
        if len(self.x) != len(self.y) or len(self.x) < 2:
            raise ValueError("isotonic calibrator requires >=2 x/y points")

        pairs = sorted(zip(self.x, self.y), key=lambda row: row[0])
        self.x = [p[0] for p in pairs]

        monotonic_y: list[float] = []
        running = 0.0
        for _, yi in pairs:
            running = max(running, yi)
            monotonic_y.append(min(1.0, max(0.0, running)))
        self.y = monotonic_y

    def calibrate(self, value: float) -> float:
        if value <= self.x[0]:
            return self.y[0]
        if value >= self.x[-1]:
            return self.y[-1]

        for i in range(1, len(self.x)):
            if value <= self.x[i]:
                x0, x1 = self.x[i - 1], self.x[i]
                y0, y1 = self.y[i - 1], self.y[i]
                span = x1 - x0
                if span <= 0:
                    return y1
                w = (value - x0) / span
                return y0 + (w * (y1 - y0))

        return self.y[-1]


def _read_params(params_path: str | None) -> dict[str, object] | None:
    if not params_path:
        return None

    path = Path(params_path)
    if not path.exists():
        logger.warning("calibration_params_file_missing", params_path=params_path)
        return None

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        logger.exception("failed_to_read_calibration_params", params_path=params_path)
        return None


def load_probability_calibrator(
    *,
    method: CalibrationMethod,
    params_path: str | None,
    logistic_coef: float,
    logistic_intercept: float,
) -> ProbabilityCalibrator:
    if method == "none":
        return IdentityCalibrator()

    params = _read_params(params_path)

    if method == "logistic":
        if params:
            coef = float(params.get("coef", logistic_coef))
            intercept = float(params.get("intercept", logistic_intercept))
            return LogisticCalibrator(coef=coef, intercept=intercept)
        return LogisticCalibrator(coef=logistic_coef, intercept=logistic_intercept)

    if method == "isotonic":
        if not params:
            logger.warning("isotonic_calibration_unavailable_fallback_identity")
            return IdentityCalibrator()
        try:
            x = [float(v) for v in list(params["x"])]
            y = [float(v) for v in list(params["y"])]
            return IsotonicCalibrator(x=x, y=y)
        except (KeyError, TypeError, ValueError):
            logger.exception("invalid_isotonic_params_fallback_identity")
            return IdentityCalibrator()

    logger.warning("unknown_calibration_method_fallback_identity", method=method)
    return IdentityCalibrator()
