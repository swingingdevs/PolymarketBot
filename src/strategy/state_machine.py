from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field

import structlog

from metrics import CURRENT_EV, WATCH_EVENTS
from markets.gamma_cache import UpDownMarket
from strategy.calibration import CalibrationInput, IdentityCalibrator, ProbabilityCalibrator
from utils.price_validation import is_price_stale, validate_price_source

logger = structlog.get_logger(__name__)


@dataclass(slots=True)
class Candidate:
    market: UpDownMarket
    direction: str
    token_id: str
    ask: float
    ev: float
    p_hat: float
    fill_prob: float
    fee_cost: float
    slippage_cost: float
    ev_exec: float
    d: float


@dataclass(slots=True)
class BookSnapshot:
    bid: float | None = None
    ask: float | None = None
    bid_size: float | None = None
    ask_size: float | None = None
    fill_prob: float | None = None


@dataclass(slots=True)
class FillProbStats:
    samples: deque[tuple[float | None, float]] = field(default_factory=lambda: deque(maxlen=50))


class StrategyStateMachine:
    def __init__(
        self,
        threshold: float,
        hammer_secs: int,
        d_min: float,
        max_entry_price: float,
        fee_bps: float,
        probability_calibrator: ProbabilityCalibrator | None = None,
        calibration_input: CalibrationInput = "p_hat",
    ) -> None:
        self.threshold = threshold
        self.hammer_secs = hammer_secs
        self.d_min = d_min
        self.max_entry_price = max_entry_price
        self.fee_bps = fee_bps
        self.probability_calibrator = probability_calibrator or IdentityCalibrator()
        self.calibration_input = calibration_input

        self.last_price: float | None = None
        self.curr_minute_start: int | None = None
        self.minute_open: float | None = None
        self.watch_mode = False
        self.watch_mode_started_at: int | None = None

        self.start_prices: dict[int, float] = {}
        self.start_price_metadata: dict[int, dict[str, object]] = {}
        self.last_5m_bucket: int | None = None
        self.last_15m_bucket: int | None = None
        self.prices_1s: deque[tuple[int, float]] = deque(maxlen=max(120, self.rolling_window_seconds * 2))
        self.books: dict[str, BookSnapshot] = {}
        self.fill_stats: dict[str, FillProbStats] = {}

    def _estimate_fill_prob(self, token_id: str, ask: float | None, ts: float | None) -> float | None:
        if ask is None:
            return None
        stats = self.fill_stats.setdefault(token_id, FillProbStats())
        stats.samples.append((ask, ts if ts is not None else float(len(stats.samples))))
        if len(stats.samples) < 2:
            return 0.5

        same_time = 0.0
        total_time = 0.0
        rows = list(stats.samples)
        for idx in range(1, len(rows)):
            prev_ask, prev_ts = rows[idx - 1]
            curr_ask, curr_ts = rows[idx]
            if prev_ts is None or curr_ts is None:
                dt = 1.0
            else:
                dt = max(0.0, curr_ts - prev_ts)
            total_time += dt
            if prev_ask == curr_ask:
                same_time += dt

        if total_time <= 0:
            stability = sum(1 for i in range(1, len(rows)) if rows[i][0] == rows[i - 1][0]) / (len(rows) - 1)
        else:
            stability = same_time / total_time
        return min(0.95, max(0.05, stability))

    def on_book(
        self,
        token_id: str,
        bid: float | None,
        ask: float | None,
        bid_size: float | None = None,
        ask_size: float | None = None,
        fill_prob: float | None = None,
        ts: float | None = None,
    ) -> None:
        snap = self.books.get(token_id, BookSnapshot())
        if bid is not None:
            snap.bid = bid
        if ask is not None:
            snap.ask = ask
        if bid_size is not None:
            snap.bid_size = bid_size
        if ask_size is not None:
            snap.ask_size = ask_size

        inferred_fill_prob = self._estimate_fill_prob(token_id, snap.ask, ts)
        if fill_prob is not None:
            snap.fill_prob = min(1.0, max(0.0, fill_prob))
        elif inferred_fill_prob is not None:
            snap.fill_prob = inferred_fill_prob

        self.books[token_id] = snap

    def on_price(self, ts: float, price: float, metadata: dict[str, object] | None = None) -> None:
        metadata = metadata or {"source": "chainlink_rtds", "timestamp": ts}

        if not validate_price_source(metadata):
            logger.warning("invalid_price_source", metadata=metadata)
            return
        if is_price_stale(float(metadata.get("timestamp", ts)), stale_after_seconds=2.0):
            logger.warning("stale_price_update", timestamp=metadata.get("timestamp", ts))

        sec = int(ts)
        self.prices_1s.append((sec, price))
        cutoff = sec - self.rolling_window_seconds
        while self.prices_1s and self.prices_1s[0][0] < cutoff:
            self.prices_1s.popleft()

        self.last_price = price

        bucket_5m = sec // 300
        if bucket_5m != self.last_5m_bucket:
            self.last_5m_bucket = bucket_5m
            self.start_prices[300] = price
            self.start_price_metadata[300] = {
                "price": price,
                "timestamp": float(metadata.get("timestamp", ts)),
                "source": metadata.get("source", "unknown"),
            }

        bucket_15m = sec // 900
        if bucket_15m != self.last_15m_bucket:
            self.last_15m_bucket = bucket_15m
            self.start_prices[900] = price
            self.start_price_metadata[900] = {
                "price": price,
                "timestamp": float(metadata.get("timestamp", ts)),
                "source": metadata.get("source", "unknown"),
            }

        if self.watch_mode and self.watch_mode_started_at is not None:
            if sec - self.watch_mode_started_at >= self.watch_mode_expiry_seconds:
                self._set_watch_mode(False, sec)
                self.prices_1s = deque([(sec, price)], maxlen=max(120, self.rolling_window_seconds * 2))
                return

        if len(self.prices_1s) < 2:
            return

        first_price = self.prices_1s[0][1]
        rolling_abs_ret = abs((price / first_price) - 1) if first_price > 0 else 0.0
        trigger_by_return = rolling_abs_ret >= self.threshold

        trigger_by_zscore = False
        if self.watch_zscore_threshold > 0:
            rets = self._rolling_returns()
            if len(rets) >= 2:
                mean = sum(rets) / len(rets)
                var = sum((x - mean) ** 2 for x in rets) / max(1, len(rets) - 1)
                stddev = math.sqrt(var)
                if stddev > 0:
                    latest_ret = rets[-1]
                    z_score = abs((latest_ret - mean) / stddev)
                    trigger_by_zscore = z_score >= self.watch_zscore_threshold

        if trigger_by_return or trigger_by_zscore:
            self._set_watch_mode(True, sec)

    def in_hammer_window(self, now_ts: int, end_epoch: int) -> bool:
        return 0 <= (end_epoch - now_ts) <= self.hammer_secs

    def _sigma1(self) -> float:
        if len(self.prices_1s) < 61:
            return 0.0
        rows = list(self.prices_1s)[-61:]
        rets = []
        for i in range(1, len(rows)):
            prev, curr = rows[i - 1][1], rows[i][1]
            if prev > 0:
                rets.append((curr / prev) - 1)
        if not rets:
            return 0.0
        mean = sum(rets) / len(rets)
        var = sum((x - mean) ** 2 for x in rets) / max(1, len(rets) - 1)
        return math.sqrt(max(var, 1e-12))

    @staticmethod
    def _normal_cdf(x: float) -> float:
        return 0.5 * (1 + math.erf(x / math.sqrt(2)))

    def _candidate_ev(
        self,
        market: UpDownMarket,
        direction: str,
        ask: float,
        bid: float | None,
        ask_size: float | None,
        fill_prob: float | None,
    ) -> Candidate | None:
        curr = self.last_price
        if curr is None or ask <= 0:
            return None

        horizon_key = market.horizon_minutes * 60
        start = self.start_prices.get(horizon_key)
        if start is None:
            return None

        d = abs(curr - start)
        if d <= self.d_min or ask > self.max_entry_price:
            return None

        sigma1 = self._sigma1()
        secs = max(1, market.end_epoch - int(self.prices_1s[-1][0]))
        sigma_t = sigma1 * math.sqrt(secs)
        if sigma_t <= 0:
            return None

        z_up = (start - curr) / (curr * sigma_t)
        p_up = 1 - self._normal_cdf(z_up)
        raw_p_hat = p_up if direction == "UP" else 1 - p_up
        z_directional = -z_up if direction == "UP" else z_up

        if self.calibration_input == "z_score":
            p_hat = self.probability_calibrator.calibrate(z_directional)
        else:
            p_hat = self.probability_calibrator.calibrate(raw_p_hat)

        fee_cost = self.fee_bps / 10000.0
        spread = max(0.0, ask - bid) if bid is not None else 0.0
        spread_penalty = 0.5 * spread
        depth_penalty = 0.0
        if ask_size is not None and ask_size > 0:
            depth_penalty = max(0.0, 1.0 - ask_size) * spread
        slippage_cost = spread_penalty + depth_penalty

        effective_fill_prob = 0.5 if fill_prob is None else min(1.0, max(0.0, fill_prob))
        ev_exec = p_hat - ask - fee_cost - slippage_cost
        ev = ev_exec * effective_fill_prob
        return Candidate(
            market=market,
            direction=direction,
            token_id="",
            ask=ask,
            ev=ev,
            p_hat=p_hat,
            fill_prob=effective_fill_prob,
            fee_cost=fee_cost,
            slippage_cost=slippage_cost,
            ev_exec=ev_exec,
            d=d,
        )

    def pick_best(
        self,
        now_ts: int,
        markets: list[UpDownMarket],
        token_map: dict[str, str],
    ) -> Candidate | None:
        candidates: list[Candidate] = []
        for market in markets:
            if not self.in_hammer_window(now_ts, market.end_epoch):
                continue
            for direction, tid in (("UP", market.up_token_id), ("DOWN", market.down_token_id)):
                book = self.books.get(tid, BookSnapshot())
                ask = book.ask
                if ask is None:
                    continue
                cand = self._candidate_ev(
                    market,
                    direction,
                    ask,
                    bid=book.bid,
                    ask_size=book.ask_size,
                    fill_prob=book.fill_prob,
                )
                if cand:
                    cand.token_id = tid
                    candidates.append(cand)

        if not candidates:
            return None

        best = max(candidates, key=lambda c: c.ev)
        logger.info(
            "best_candidate_selected",
            token_id=best.token_id,
            direction=best.direction,
            ask=best.ask,
            p_hat=best.p_hat,
            fill_prob=best.fill_prob,
            slippage_cost=best.slippage_cost,
            ev_exec=best.ev_exec,
            ev=best.ev,
        )
        CURRENT_EV.set(best.ev)
        return best
