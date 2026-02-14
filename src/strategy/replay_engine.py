from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import orjson

from markets.gamma_cache import UpDownMarket
from strategy.state_machine import StrategyStateMachine


@dataclass(slots=True)
class ReplayTrade:
    decision_ts: float
    token_id: str
    direction: str
    horizon_minutes: int
    requested_price: float
    requested_size: float
    fill_price: float | None
    fee_cost: float
    slippage_cost: float
    ok: bool
    failure_type: str | None
    start_price: float
    end_price: float

    @property
    def won(self) -> bool:
        if self.direction == "UP":
            return self.end_price > self.start_price
        return self.end_price <= self.start_price

    @property
    def pnl(self) -> float:
        if not self.ok:
            return 0.0
        payout = 1.0 if self.won else 0.0
        if self.fill_price is None:
            return 0.0
        return payout - self.fill_price - self.fee_cost


@dataclass(slots=True)
class BookDepth:
    ts: float
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]


@dataclass(slots=True)
class ReplaySummary:
    pnl: float
    max_drawdown: float
    trade_count: int
    win_rate: float
    avg_fee: float
    avg_slippage: float
    fok_fail_pct: float


class ReplayEngine:
    def __init__(self, markets: list[UpDownMarket], *, order_latency_ms: int = 150) -> None:
        self.markets = markets
        self.order_latency_ms = max(0, order_latency_ms)
        self._token_index: dict[str, tuple[UpDownMarket, str]] = {}
        for market in markets:
            self._token_index[market.up_token_id] = (market, "UP")
            self._token_index[market.down_token_id] = (market, "DOWN")

    def run(
        self,
        events: list[dict[str, Any]],
        params: dict[str, float | int],
    ) -> tuple[list[ReplayTrade], ReplaySummary]:
        sm = StrategyStateMachine(
            threshold=float(params["watch_return_threshold"]),
            hammer_secs=int(params["hammer_secs"]),
            d_min=float(params["d_min"]),
            max_entry_price=float(params["max_entry_price"]),
            fee_bps=float(params["fee_bps"]),
        )

        depth_by_token: dict[str, list[BookDepth]] = {}
        prices: list[tuple[int, float]] = []
        decisions: list[dict[str, Any]] = []

        sorted_events = sorted(events, key=lambda e: (float(e.get("ts", 0.0)), str(e.get("type", ""))))
        for event in sorted_events:
            event_type = str(event.get("type", "")).strip().lower()
            ts = float(event.get("ts", 0.0))
            if event_type == "rtds_price":
                px = float(event["price"])
                sm.on_price(ts, px, {"source": "chainlink_rtds", "timestamp": event.get("payload_ts", ts)})
                prices.append((int(ts), px))
            elif event_type in {"clob_book", "clob_price_change"}:
                token_id = str(event.get("token_id", ""))
                if not token_id:
                    continue
                bids = _normalize_levels(event.get("bids_levels"), fallback_px=event.get("best_bid"))
                asks = _normalize_levels(event.get("asks_levels"), fallback_px=event.get("best_ask"))
                depth_by_token.setdefault(token_id, []).append(BookDepth(ts=ts, bids=bids, asks=asks))
                best_bid = bids[0][0] if bids else None
                best_ask = asks[0][0] if asks else None
                bid_size = bids[0][1] if bids else None
                ask_size = asks[0][1] if asks else None
                sm.on_book(token_id, best_bid, best_ask, bid_size=bid_size, ask_size=ask_size, ts=ts)
            elif event_type == "decision":
                decisions.append(event)

        if not decisions:
            return [], ReplaySummary(0.0, 0.0, 0, 0.0, 0.0, 0.0, 0.0)

        price_ts = [t for t, _ in prices]
        trades: list[ReplayTrade] = []

        for dec in decisions:
            token_id = str(dec.get("token_id", ""))
            token_row = self._token_index.get(token_id)
            if token_row is None:
                continue
            market, direction = token_row
            decision_ts = float(dec.get("ts", 0.0))
            ask = float(dec.get("ask", 0.0))
            if ask <= 0:
                continue
            notional = float(dec.get("notional", 20.0))
            requested_size = notional / ask
            fee_cost = float(dec.get("fee_cost", params["fee_bps"]) )
            if fee_cost > 1.0:
                fee_cost = float(params["fee_bps"]) / 10_000.0

            end_idx = bisect_left(price_ts, market.end_epoch)
            start_idx = bisect_left(price_ts, market.start_epoch)
            if end_idx >= len(prices) or start_idx >= len(prices):
                continue
            start_price = prices[start_idx][1]
            end_price = prices[end_idx][1]

            fill_price, slip_cost, failure_type = self._simulate_fill(
                token_id=token_id,
                decision_ts=decision_ts,
                requested_size=requested_size,
                baseline_ask=ask,
                depth_by_token=depth_by_token,
            )
            trades.append(
                ReplayTrade(
                    decision_ts=decision_ts,
                    token_id=token_id,
                    direction=direction,
                    horizon_minutes=market.horizon_minutes,
                    requested_price=ask,
                    requested_size=requested_size,
                    fill_price=fill_price,
                    fee_cost=fee_cost,
                    slippage_cost=slip_cost,
                    ok=fill_price is not None,
                    failure_type=failure_type,
                    start_price=start_price,
                    end_price=end_price,
                )
            )

        return trades, _summarize_trades(trades)

    def _simulate_fill(
        self,
        *,
        token_id: str,
        decision_ts: float,
        requested_size: float,
        baseline_ask: float,
        depth_by_token: dict[str, list[BookDepth]],
    ) -> tuple[float | None, float, str | None]:
        target_ts = decision_ts + (self.order_latency_ms / 1000.0)
        ladder = _closest_depth(depth_by_token.get(token_id, []), target_ts)
        if ladder is None or not ladder.asks:
            return None, 0.0, "missing_book"

        remaining = requested_size
        cost = 0.0
        for px, size in ladder.asks:
            take = min(remaining, size)
            cost += take * px
            remaining -= take
            if remaining <= 1e-9:
                break

        if remaining > 1e-9:
            return None, 0.0, "fok_insufficient_depth"

        fill_price = cost / requested_size
        return fill_price, max(0.0, fill_price - baseline_ask), None


def _normalize_levels(raw_levels: Any, *, fallback_px: Any = None) -> list[tuple[float, float]]:
    levels: list[tuple[float, float]] = []
    if isinstance(raw_levels, list):
        for row in raw_levels:
            if isinstance(row, dict):
                px = row.get("price")
                size = row.get("size", 0.0)
            elif isinstance(row, (list, tuple)) and len(row) >= 2:
                px, size = row[0], row[1]
            else:
                continue
            try:
                px_f = float(px)
                size_f = float(size)
            except (TypeError, ValueError):
                continue
            if size_f <= 0:
                continue
            levels.append((px_f, size_f))
    if not levels and fallback_px is not None:
        try:
            px_fallback = float(fallback_px)
            levels = [(px_fallback, 1.0)]
        except (TypeError, ValueError):
            return []
    levels.sort(key=lambda x: x[0])
    return levels


def _closest_depth(rows: list[BookDepth], target_ts: float) -> BookDepth | None:
    if not rows:
        return None
    return min(rows, key=lambda r: (abs(r.ts - target_ts), r.ts))


def _max_drawdown(curve: list[float]) -> float:
    peak = float("-inf")
    max_dd = 0.0
    for val in curve:
        peak = max(peak, val)
        max_dd = min(max_dd, val - peak)
    return abs(max_dd)


def _summarize_trades(trades: list[ReplayTrade]) -> ReplaySummary:
    if not trades:
        return ReplaySummary(0.0, 0.0, 0, 0.0, 0.0, 0.0, 0.0)
    pnl = 0.0
    curve: list[float] = []
    ok_trades = [t for t in trades if t.ok]
    fok_fails = sum(1 for t in trades if t.failure_type == "fok_insufficient_depth")

    for trade in trades:
        pnl += trade.pnl
        curve.append(pnl)

    win_rate = (sum(1 for t in ok_trades if t.won) / len(ok_trades)) if ok_trades else 0.0
    avg_fee = (sum(t.fee_cost for t in ok_trades) / len(ok_trades)) if ok_trades else 0.0
    avg_slippage = (sum(t.slippage_cost for t in ok_trades) / len(ok_trades)) if ok_trades else 0.0
    return ReplaySummary(
        pnl=pnl,
        max_drawdown=_max_drawdown(curve),
        trade_count=len(trades),
        win_rate=win_rate,
        avg_fee=avg_fee,
        avg_slippage=avg_slippage,
        fok_fail_pct=fok_fails / len(trades),
    )


def load_recorded_events(path: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    with path.open("rb") as f:
        for line in f:
            if not line.strip():
                continue
            payload = orjson.loads(line)
            if isinstance(payload, dict):
                events.append(payload)
    return events

