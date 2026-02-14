from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass, field

import structlog

from markets.gamma_cache import UpDownMarket
from markets.token_metadata_cache import TokenMetadataCache
from metrics import CURRENT_EV, FEED_BLOCKED_STALE_PRICE, REJECTED_MAX_ENTRY_PRICE, STALE_FEED, WATCH_EVENTS, WATCH_TRIGGERED
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
    bids_levels: list[tuple[float, float]] = field(default_factory=list)
    asks_levels: list[tuple[float, float]] = field(default_factory=list)


@dataclass(slots=True)
class FillProbStats:
    samples: deque[tuple[float | None, float]] = field(default_factory=lambda: deque(maxlen=50))


@dataclass(slots=True)
class RollingStats:
    count: int = 0
    mean: float = 0.0
    m2: float = 0.0

    def add(self, value: float) -> None:
        self.count += 1
        delta = value - self.mean
        self.mean += delta / self.count
        delta2 = value - self.mean
        self.m2 += delta * delta2

    def remove(self, value: float) -> None:
        if self.count <= 0:
            return
        if self.count == 1:
            self.count = 0
            self.mean = 0.0
            self.m2 = 0.0
            return
        next_count = self.count - 1
        delta = value - self.mean
        next_mean = self.mean - (delta / next_count)
        self.m2 -= delta * (value - next_mean)
        self.count = next_count
        self.mean = next_mean
        if self.m2 < 0:
            self.m2 = 0.0

    def stddev(self) -> float:
        if self.count <= 0:
            return 0.0
        return math.sqrt(self.m2 / max(1, self.count - 1))


class StrategyStateMachine:
    def __init__(
        self,
        threshold: float,
        hammer_secs: int,
        d_min: float,
        max_entry_price: float,
        fee_bps: float,
        fee_formula_exponent: float = 1.0,
        expected_notional_usd: float = 1.0,
        depth_penalty_coeff: float = 1.0,
        price_stale_after_seconds: float = 2.0,
        probability_calibrator: ProbabilityCalibrator | None = None,
        calibration_input: CalibrationInput = "p_hat",
        token_metadata_cache: TokenMetadataCache | None = None,
        rolling_window_seconds: int = 60,
        watch_zscore_threshold: float = 0.0,
        watch_mode_expiry_seconds: int = 60,
    ) -> None:
        self.threshold = threshold
        self.hammer_secs = hammer_secs
        self.d_min = d_min
        self.max_entry_price = max_entry_price
        self.fee_bps = fee_bps
        self.fee_formula_exponent = fee_formula_exponent
        self.expected_notional_usd = max(0.0, expected_notional_usd)
        self.depth_penalty_coeff = max(0.0, depth_penalty_coeff)
        self.price_stale_after_seconds = price_stale_after_seconds
        self.probability_calibrator = probability_calibrator or IdentityCalibrator()
        self.calibration_input = calibration_input
        self.token_metadata_cache = token_metadata_cache
        self.rolling_window_seconds = max(2, rolling_window_seconds)
        self.watch_zscore_threshold = watch_zscore_threshold
        self.watch_mode_expiry_seconds = max(1, watch_mode_expiry_seconds)

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
        self.rolling_returns: deque[float | None] = deque()
        self.rolling_return_stats = RollingStats()
        self.sigma1_window_returns: deque[float | None] = deque(maxlen=60)
        self.sigma1_stats = RollingStats()
        self.books: dict[str, BookSnapshot] = {}
        self.fill_stats: dict[str, FillProbStats] = {}
        self.price_is_stale = False
        FEED_BLOCKED_STALE_PRICE.set(0)

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
        bids_levels: list[tuple[float, float]] | None = None,
        asks_levels: list[tuple[float, float]] | None = None,
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
        if bids_levels is not None:
            snap.bids_levels = bids_levels
        elif snap.bid is not None and snap.bid_size is not None:
            snap.bids_levels = [(snap.bid, snap.bid_size)]
        if asks_levels is not None:
            snap.asks_levels = asks_levels
        elif snap.ask is not None and snap.ask_size is not None:
            snap.asks_levels = [(snap.ask, snap.ask_size)]

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
        metadata_ts = float(metadata.get("timestamp", ts))
        historical_replay = abs(time.time() - float(ts)) > (self.price_stale_after_seconds * 10)
        stale_by_event_clock = (float(ts) - metadata_ts) > self.price_stale_after_seconds
        stale_by_wall_clock_eval = is_price_stale(metadata_ts, stale_after_seconds=self.price_stale_after_seconds)
        stale_by_wall_clock = False if historical_replay else stale_by_wall_clock_eval
        if stale_by_event_clock or stale_by_wall_clock:
            logger.warning("stale_price_update", timestamp=metadata_ts)
            STALE_FEED.inc()
            self.price_is_stale = True
            FEED_BLOCKED_STALE_PRICE.set(1)
            return
        self.price_is_stale = False
        FEED_BLOCKED_STALE_PRICE.set(0)

        sec = int(ts)

        if self.prices_1s:
            prev_price = self.prices_1s[-1][1]
            latest_ret = ((price / prev_price) - 1.0) if prev_price > 0 else None
            self.rolling_returns.append(latest_ret)
            if latest_ret is not None:
                self.rolling_return_stats.add(latest_ret)

            if len(self.sigma1_window_returns) == self.sigma1_window_returns.maxlen:
                expired_sigma_ret = self.sigma1_window_returns.popleft()
                if expired_sigma_ret is not None:
                    self.sigma1_stats.remove(expired_sigma_ret)
            self.sigma1_window_returns.append(latest_ret)
            if latest_ret is not None:
                self.sigma1_stats.add(latest_ret)

        self.prices_1s.append((sec, price))
        cutoff = sec - self.rolling_window_seconds
        while self.prices_1s and self.prices_1s[0][0] < cutoff:
            self.prices_1s.popleft()
            if self.rolling_returns:
                expired_ret = self.rolling_returns.popleft()
                if expired_ret is not None:
                    self.rolling_return_stats.remove(expired_ret)

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
                self.rolling_returns = deque()
                self.rolling_return_stats = RollingStats()
                self.sigma1_window_returns = deque(maxlen=60)
                self.sigma1_stats = RollingStats()
                return

        if len(self.prices_1s) < 2:
            return

        first_price = self.prices_1s[0][1]
        rolling_abs_ret = abs((price / first_price) - 1) if first_price > 0 else 0.0
        trigger_by_return = rolling_abs_ret >= self.threshold

        trigger_by_zscore = False
        if self.watch_zscore_threshold > 0:
            latest_ret = self.rolling_returns[-1] if self.rolling_returns else None
            if self.rolling_return_stats.count >= 2 and latest_ret is not None:
                stddev = self.rolling_return_stats.stddev()
                if stddev > 0:
                    z_score = abs((latest_ret - self.rolling_return_stats.mean) / stddev)
                    trigger_by_zscore = z_score >= self.watch_zscore_threshold

        if trigger_by_return or trigger_by_zscore:
            if self.price_is_stale:
                return
            self._set_watch_mode(True, sec)


    def _rolling_returns(self) -> list[float]:
        return [ret for ret in self.rolling_returns if ret is not None]

    def _set_watch_mode(self, enabled: bool, ts: int) -> None:
        if enabled == self.watch_mode:
            return
        self.watch_mode = enabled
        self.watch_mode_started_at = ts if enabled else None
        WATCH_EVENTS.inc()
        if enabled:
            WATCH_TRIGGERED.inc()

    def disable_watch_mode(self, ts: int) -> None:
        self._set_watch_mode(False, ts)

    def in_hammer_window(self, now_ts: int, end_epoch: int) -> bool:
        return 0 <= (end_epoch - now_ts) <= self.hammer_secs

    def _sigma1(self) -> float:
        if len(self.prices_1s) < 61:
            return 0.0
        if self.sigma1_stats.count <= 0:
            return 0.0
        return math.sqrt(max((self.sigma1_stats.m2 / max(1, self.sigma1_stats.count - 1)), 1e-12))

    @staticmethod
    def _normal_cdf(x: float) -> float:
        return 0.5 * (1 + math.erf(x / math.sqrt(2)))


    @staticmethod
    def vwap_to_fill(size: float, asks_levels: list[tuple[float, float]]) -> float | None:
        if size <= 0:
            return None
        if not asks_levels:
            return None

        remaining = size
        notional = 0.0
        for price, level_size in asks_levels:
            if price <= 0 or level_size <= 0:
                continue
            take = min(remaining, level_size)
            notional += take * price
            remaining -= take
            if remaining <= 1e-12:
                return notional / size
        return None

    def _buy_fee_cost_per_share(self, *, ask: float, fee_rate_bps: float) -> float:
        fee_rate = fee_rate_bps / 10_000.0
        p = min(max(ask, 1e-9), 1 - 1e-9)
        return p * fee_rate * ((p * (1 - p)) ** self.fee_formula_exponent)

    def _candidate_ev(
        self,
        market: UpDownMarket,
        direction: str,
        ask: float,
        bid: float | None = None,
        ask_size: float | None = None,
        fill_prob: float | None = None,
        token_id: str | None = None,
        expected_size: float | None = None,
        asks_levels: list[tuple[float, float]] | None = None,
    ) -> Candidate | None:
        curr = self.last_price
        if curr is None or ask <= 0:
            return None

        horizon_key = market.horizon_minutes * 60
        start = self.start_prices.get(horizon_key)
        if start is None:
            return None

        d = abs(curr - start)
        if ask > self.max_entry_price:
            REJECTED_MAX_ENTRY_PRICE.inc()
            return None
        if d <= self.d_min:
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

        fee_bps: float | None = None
        if token_id and self.token_metadata_cache is not None:
            metadata = self.token_metadata_cache.get(token_id, allow_stale=True)
            if metadata and metadata.fee_rate_bps is not None and metadata.fee_rate_bps >= 0:
                fee_bps = metadata.fee_rate_bps
        if fee_bps is None and token_id:
            market_metadata = market.token_metadata_by_id.get(token_id)
            if market_metadata and market_metadata.fee_rate_bps is not None and market_metadata.fee_rate_bps >= 0:
                fee_bps = market_metadata.fee_rate_bps

        if fee_bps is None:
            fee_cost = self.fee_bps / 10000.0
        else:
            fee_cost = self._buy_fee_cost_per_share(ask=ask, fee_rate_bps=fee_bps)

        required_shares = expected_size if expected_size is not None else (self.expected_notional_usd / ask)
        required_shares = max(0.0, required_shares)
        vwap_price = self.vwap_to_fill(required_shares, asks_levels or [])
        if vwap_price is None:
            slippage_cost = 0.0
            can_fill = False
        else:
            slippage_cost = max(0.0, vwap_price - ask)
            can_fill = True

        effective_fill_prob = 1.0 if fill_prob is None else min(1.0, max(0.0, fill_prob))
        if not can_fill:
            effective_fill_prob = 0.0
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
        if self.price_is_stale:
            return None
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
                    token_id=tid,
                    asks_levels=book.asks_levels,
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
