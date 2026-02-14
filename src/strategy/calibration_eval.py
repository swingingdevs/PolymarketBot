from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass


@dataclass(slots=True)
class CalibrationBin:
    bin_start: float
    bin_end: float
    count: int
    mean_predicted: float
    empirical_rate: float


def brier_score(probabilities: list[float], outcomes: list[int]) -> float:
    if len(probabilities) != len(outcomes) or not probabilities:
        raise ValueError("probabilities and outcomes must be same non-zero length")
    return sum((p - y) ** 2 for p, y in zip(probabilities, outcomes)) / len(probabilities)


def calibration_curve(probabilities: list[float], outcomes: list[int], bins: int = 10) -> list[CalibrationBin]:
    if bins <= 0:
        raise ValueError("bins must be positive")
    if len(probabilities) != len(outcomes):
        raise ValueError("probabilities and outcomes must be same length")

    bucket_preds: list[list[float]] = [[] for _ in range(bins)]
    bucket_obs: list[list[int]] = [[] for _ in range(bins)]

    for p, y in zip(probabilities, outcomes):
        clamped = min(1.0, max(0.0, p))
        idx = min(bins - 1, int(clamped * bins))
        bucket_preds[idx].append(clamped)
        bucket_obs[idx].append(y)

    rows: list[CalibrationBin] = []
    for i in range(bins):
        start = i / bins
        end = (i + 1) / bins
        if not bucket_preds[i]:
            rows.append(CalibrationBin(start, end, 0, 0.0, 0.0))
            continue

        mean_pred = sum(bucket_preds[i]) / len(bucket_preds[i])
        empirical = sum(bucket_obs[i]) / len(bucket_obs[i])
        rows.append(CalibrationBin(start, end, len(bucket_preds[i]), mean_pred, empirical))

    return rows


def _load_csv(path: str) -> tuple[list[float], list[int]]:
    probs: list[float] = []
    outcomes: list[int] = []
    with open(path, encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            probs.append(float(row["probability"]))
            outcomes.append(int(row["outcome"]))
    return probs, outcomes


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate calibrated probabilities on labeled outcomes")
    parser.add_argument("csv_path", help="CSV path with columns: probability,outcome")
    parser.add_argument("--bins", type=int, default=10)
    args = parser.parse_args()

    probabilities, outcomes = _load_csv(args.csv_path)
    print(f"brier_score={brier_score(probabilities, outcomes):.6f}")
    print("bin_start,bin_end,count,mean_predicted,empirical_rate")
    for row in calibration_curve(probabilities, outcomes, bins=args.bins):
        print(
            f"{row.bin_start:.3f},{row.bin_end:.3f},{row.count},{row.mean_predicted:.6f},{row.empirical_rate:.6f}"
        )


if __name__ == "__main__":
    main()
