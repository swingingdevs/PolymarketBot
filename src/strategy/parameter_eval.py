from __future__ import annotations

import argparse
import csv
from bisect import bisect_left
from dataclasses import dataclass
from itertools import product
from pathlib import Path

import orjson
from markets.gamma_cache import UpDownMarket
from strategy.replay_engine import ReplayEngine, load_recorded_events
from strategy.state_machine import StrategyStateMachine


@dataclass(slots=True)
class ReplayRow:
    ts: float
    event: str
    price: float | None = None
    token_id: str | None = None
    bid: float | None = None
    ask: float | None = None


@dataclass(slots=True)
class FilledTrade:
    ts: int
    horizon_minutes: int
    direction: str
    ask: float
    fee_cost: float
    ev: float
    start_price: float
    end_price: float

    @property
    def won(self) -> bool:
        if self.direction == "UP":
            return self.end_price > self.start_price
        return self.end_price <= self.start_price

    @property
    def pnl(self) -> float:
        payout = 1.0 if self.won else 0.0
        return payout - self.ask - self.fee_cost

    @property
    def ev_error(self) -> float:
        return self.pnl - self.ev


def _parse_float(raw: str | None) -> float | None:
    if raw is None or raw == "":
        return None
    return float(raw)


def load_replay_rows(path: Path) -> list[ReplayRow]:
    rows: list[ReplayRow] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(
                ReplayRow(
                    ts=float(row["ts"]),
                    event=row["event"].strip().lower(),
                    price=_parse_float(row.get("price")),
                    token_id=row.get("token_id") or None,
                    bid=_parse_float(row.get("bid")),
                    ask=_parse_float(row.get("ask")),
                )
            )
    return sorted(rows, key=lambda r: r.ts)


def load_markets(path: Path) -> list[UpDownMarket]:
    payload = orjson.loads(path.read_bytes())
    return [UpDownMarket(**row) for row in payload]


def _max_drawdown(equity_curve: list[float]) -> float:
    peak = float("-inf")
    max_dd = 0.0
    for value in equity_curve:
        peak = max(peak, value)
        max_dd = min(max_dd, value - peak)
    return abs(max_dd)


def replay_with_params(rows: list[ReplayRow], markets: list[UpDownMarket], params: dict[str, float | int]) -> dict[str, float | int]:
    sm = StrategyStateMachine(
        threshold=float(params["watch_return_threshold"]),
        hammer_secs=int(params["hammer_secs"]),
        d_min=float(params["d_min"]),
        max_entry_price=float(params["max_entry_price"]),
        fee_bps=float(params["fee_bps"]),
    )

    fee_cost = float(params["fee_bps"]) / 10_000.0
    all_markets = list(markets)
    closed_markets = [m for m in all_markets if m.end_epoch <= int(rows[-1].ts)] if rows else []

    price_rows = [r for r in rows if r.event == "price" and r.price is not None]
    price_ts = [int(r.ts) for r in price_rows]

    trades: list[FilledTrade] = []
    trade_keys: set[tuple[str, int]] = set()

    for row in rows:
        if row.event == "book" and row.token_id:
            asks_levels = [(row.ask, 1_000_000.0)] if row.ask is not None else None
            bids_levels = [(row.bid, 1_000_000.0)] if row.bid is not None else None
            sm.on_book(row.token_id, row.bid, row.ask, ask_size=1_000_000.0 if row.ask is not None else None, bid_size=1_000_000.0 if row.bid is not None else None, asks_levels=asks_levels, bids_levels=bids_levels)
            continue
        if row.event != "price" or row.price is None:
            continue

        sm.on_price(row.ts, row.price, {"source": "chainlink_rtds", "timestamp": row.ts})
        now = int(row.ts)

        for market in all_markets:
            key = market.horizon_minutes * 60
            if key in sm.start_prices:
                continue
            if now >= market.start_epoch:
                sm.start_prices[key] = float(row.price)

        if not sm.watch_mode:
            continue

        best = sm.pick_best(now, all_markets, {})
        if best is None or best.ev <= 0:
            continue

        trade_key = (best.market.slug, now)
        if trade_key in trade_keys:
            continue
        trade_keys.add(trade_key)

        start_price = sm.start_prices.get(best.market.horizon_minutes * 60)
        if start_price is None:
            continue

        end_idx = bisect_left(price_ts, best.market.end_epoch)
        if end_idx >= len(price_rows):
            continue

        end_price = float(price_rows[end_idx].price)
        trades.append(
            FilledTrade(
                ts=now,
                horizon_minutes=best.market.horizon_minutes,
                direction=best.direction,
                ask=best.ask,
                fee_cost=fee_cost,
                ev=best.ev,
                start_price=float(start_price),
                end_price=end_price,
            )
        )

    if not trades:
        return {
            **params,
            "closed_markets": len(closed_markets),
            "trades": 0,
            "win_rate": 0.0,
            "avg_ev_error": 0.0,
            "max_drawdown": 0.0,
            "trade_frequency_per_hour": 0.0,
            "total_pnl": 0.0,
        }

    wins = sum(1 for t in trades if t.won)
    total_pnl = 0.0
    curve: list[float] = []
    ev_errors: list[float] = []
    for trade in trades:
        total_pnl += trade.pnl
        curve.append(total_pnl)
        ev_errors.append(abs(trade.ev_error))

    span_secs = max(1.0, rows[-1].ts - rows[0].ts)
    trades_per_hour = len(trades) / (span_secs / 3600.0)

    return {
        **params,
        "closed_markets": len(closed_markets),
        "trades": len(trades),
        "win_rate": wins / len(trades),
        "avg_ev_error": sum(ev_errors) / len(ev_errors),
        "max_drawdown": _max_drawdown(curve),
        "trade_frequency_per_hour": trades_per_hour,
        "total_pnl": total_pnl,
    }


def _parse_grid(grid_raw: str) -> dict[str, list[float]]:
    payload = orjson.loads(grid_raw)
    parsed: dict[str, list[float]] = {}
    for key, values in payload.items():
        parsed[key] = [float(v) for v in values]
    return parsed


def sweep_parameter_grid(
    rows: list[ReplayRow] | None,
    markets: list[UpDownMarket],
    grid: dict[str, list[float]],
    *,
    replay_events: list[dict[str, object]] | None = None,
    order_latency_ms: int = 150,
) -> tuple[list[dict[str, float | int]], dict[str, dict[str, float]]]:
    keys = ["watch_return_threshold", "hammer_secs", "d_min", "max_entry_price", "fee_bps"]
    combos = product(*(grid[k] for k in keys))

    report: list[dict[str, float | int]] = []
    for combo in combos:
        params: dict[str, float | int] = {
            "watch_return_threshold": combo[0],
            "hammer_secs": int(combo[1]),
            "d_min": combo[2],
            "max_entry_price": combo[3],
            "fee_bps": combo[4],
        }
        if replay_events is None:
            assert rows is not None
            result = replay_with_params(rows, markets, params)
        else:
            engine = ReplayEngine(markets, order_latency_ms=order_latency_ms)
            _trades, summary = engine.run(replay_events, params)
            result = {
                **params,
                "closed_markets": len(markets),
                "trades": summary.trade_count,
                "win_rate": summary.win_rate,
                "avg_ev_error": 0.0,
                "max_drawdown": summary.max_drawdown,
                "trade_frequency_per_hour": 0.0,
                "total_pnl": summary.pnl,
                "avg_fee": summary.avg_fee,
                "avg_slippage": summary.avg_slippage,
                "fok_fail_pct": summary.fok_fail_pct,
            }
        report.append(result)

    ranked = sorted(
        report,
        key=lambda r: (float(r["total_pnl"]), float(r["win_rate"]), -float(r["avg_ev_error"])),
        reverse=True,
    )
    top_n = max(1, len(ranked) // 5)
    robust = ranked[:top_n]

    robust_ranges: dict[str, dict[str, float]] = {}
    for key in keys:
        values = [float(row[key]) for row in robust]
        robust_ranges[key] = {"min": min(values), "max": max(values)}

    return ranked, robust_ranges


def export_report(report: list[dict[str, float | int]], robust_ranges: dict[str, dict[str, float]], output_prefix: Path) -> None:
    output_prefix.parent.mkdir(parents=True, exist_ok=True)

    json_path = output_prefix.with_suffix(".json")
    csv_path = output_prefix.with_suffix(".csv")

    json_path.write_bytes(orjson.dumps({"runs": report, "robust_ranges": robust_ranges}, option=orjson.OPT_INDENT_2))

    if report:
        fieldnames = list(report[0].keys())
        with csv_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(report)


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay/backtest + parameter sweep for StrategyStateMachine")
    parser.add_argument("--replay-csv", type=Path, default=None)
    parser.add_argument("--markets-json", required=True, type=Path)
    parser.add_argument("--grid-json", required=True, help="JSON object with parameter arrays")
    parser.add_argument("--output-prefix", required=True, type=Path)
    parser.add_argument("--recorded-session-jsonl", type=Path, default=None)
    parser.add_argument("--order-latency-ms", type=int, default=150)
    args = parser.parse_args()

    if args.recorded_session_jsonl is None and args.replay_csv is None:
        raise SystemExit("--replay-csv is required unless --recorded-session-jsonl is provided")

    markets = load_markets(args.markets_json)
    grid = _parse_grid(args.grid_json)
    if args.recorded_session_jsonl is not None:
        events = load_recorded_events(args.recorded_session_jsonl)
        report, robust_ranges = sweep_parameter_grid(
            None,
            markets,
            grid,
            replay_events=events,
            order_latency_ms=args.order_latency_ms,
        )
    else:
        rows = load_replay_rows(args.replay_csv)
        report, robust_ranges = sweep_parameter_grid(rows, markets, grid)
    export_report(report, robust_ranges, args.output_prefix)


if __name__ == "__main__":
    main()
