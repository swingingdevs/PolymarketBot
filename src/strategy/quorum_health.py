from __future__ import annotations

import time
from dataclasses import dataclass
from statistics import median


@dataclass(slots=True)
class FeedSample:
    price: float
    payload_ts: float
    received_ts: float


@dataclass(slots=True)
class QuorumDecision:
    trading_allowed: bool
    reason_codes: list[str]
    chainlink_stale: bool
    spot_quorum_divergence_pct: float | None
    feed_lag_seconds: dict[str, float]


class QuorumHealth:
    def __init__(
        self,
        *,
        chainlink_max_lag_seconds: float = 5.0,
        spot_max_lag_seconds: float = 5.0,
        divergence_threshold_pct: float = 0.5,
        divergence_sustain_seconds: float = 5.0,
        min_spot_sources: int = 2,
    ) -> None:
        self.chainlink_max_lag_seconds = chainlink_max_lag_seconds
        self.spot_max_lag_seconds = spot_max_lag_seconds
        self.divergence_threshold_pct = divergence_threshold_pct
        self.divergence_sustain_seconds = divergence_sustain_seconds
        self.min_spot_sources = min_spot_sources

        self.chainlink_sample: FeedSample | None = None
        self.spot_samples: dict[str, FeedSample] = {}
        self._divergence_started_at: float | None = None

    def update_chainlink(self, *, price: float, payload_ts: float, received_ts: float | None = None) -> None:
        self.chainlink_sample = FeedSample(price=price, payload_ts=payload_ts, received_ts=received_ts or time.time())

    def update_spot(self, *, feed: str, price: float, payload_ts: float, received_ts: float | None = None) -> None:
        self.spot_samples[feed] = FeedSample(price=price, payload_ts=payload_ts, received_ts=received_ts or time.time())

    def evaluate(self, now: float | None = None) -> QuorumDecision:
        current = now or time.time()
        reasons: list[str] = []
        lag_seconds: dict[str, float] = {}

        if self.chainlink_sample is None:
            reasons.append("CHAINLINK_MISSING")
            return QuorumDecision(False, reasons, True, None, lag_seconds)

        chainlink_lag = max(0.0, current - self.chainlink_sample.payload_ts)
        lag_seconds["chainlink"] = chainlink_lag
        chainlink_stale = chainlink_lag > self.chainlink_max_lag_seconds
        if chainlink_stale:
            reasons.append("CHAINLINK_STALE")

        fresh_spot_prices: list[float] = []
        for feed, sample in self.spot_samples.items():
            feed_lag = max(0.0, current - sample.payload_ts)
            lag_seconds[feed] = feed_lag
            if feed_lag <= self.spot_max_lag_seconds:
                fresh_spot_prices.append(sample.price)

        if len(fresh_spot_prices) < self.min_spot_sources:
            reasons.append("SPOT_QUORUM_UNAVAILABLE")
            self._divergence_started_at = None
            return QuorumDecision(False, reasons, chainlink_stale, None, lag_seconds)

        spot_median = float(median(fresh_spot_prices))
        divergence_pct = abs((self.chainlink_sample.price - spot_median) / self.chainlink_sample.price) * 100.0

        if divergence_pct >= self.divergence_threshold_pct:
            if self._divergence_started_at is None:
                self._divergence_started_at = current
            elif (current - self._divergence_started_at) >= self.divergence_sustain_seconds:
                reasons.append("SPOT_DIVERGENCE_SUSTAINED")
        else:
            self._divergence_started_at = None

        trading_allowed = len(reasons) == 0
        return QuorumDecision(trading_allowed, reasons, chainlink_stale, divergence_pct, lag_seconds)
