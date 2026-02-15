from __future__ import annotations

import asyncio
import math
import sys
import time
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from config import Settings
from execution.trader import Trader
from feeds.clob_ws import CLOBWebSocket
from feeds.rtds import RTDSFeed
from markets.gamma_cache import GammaCache, UpDownMarket
from markets.token_metadata_cache import TokenMetadataCache
from strategy.state_machine import StrategyStateMachine


@dataclass(slots=True)
class SmokeState:
    rtds_events: int = 0
    clob_events: int = 0
    snapshots_printed: int = 0
    candidate_found: bool = False
    order_attempted: bool = False
    order_result: bool | None = None


def floor_to_boundary(ts: int, seconds: int) -> int:
    return ts - (ts % seconds)


def candidate_start_epochs(now: int, seconds: int) -> list[int]:
    current = floor_to_boundary(now, seconds)
    return [current, current - seconds, current + seconds]


async def resolve_market_with_fallback(gamma: GammaCache, horizon_minutes: int, now: int) -> UpDownMarket:
    interval_seconds = horizon_minutes * 60
    candidates = candidate_start_epochs(now, interval_seconds)
    last_exc: Exception | None = None

    for candidate_start in candidates:
        try:
            return await gamma.get_market(horizon_minutes, candidate_start)
        except Exception as exc:  # best-effort fallback across boundary candidates
            last_exc = exc

    assert last_exc is not None
    candidate_text = ", ".join(str(candidate) for candidate in candidates)
    raise RuntimeError(
        f"Failed to resolve {horizon_minutes}m market. attempted_epochs=[{candidate_text}] "
        f"last_error={last_exc}"
    )


async def resolve_markets(gamma: GammaCache) -> list[UpDownMarket]:
    now = int(time.time())
    market_5m, market_15m = await asyncio.gather(
        resolve_market_with_fallback(gamma, 5, now),
        resolve_market_with_fallback(gamma, 15, now),
    )
    return [market_5m, market_15m]


def seed_strategy_prices(strategy: StrategyStateMachine, base_price: float, now_ts: int) -> None:
    for i in range(61):
        ts = now_ts - (60 - i)
        px = base_price + math.sin(i / 3.0) * 7.5
        strategy.on_price(float(ts), px, {"source": "chainlink_rtds", "timestamp": float(ts)})


async def main() -> None:
    settings = Settings(settings_profile="paper", dry_run=True)
    gamma = GammaCache(str(settings.gamma_api_url))
    try:
        markets = await resolve_markets(gamma)
        token_ids = sorted({m.up_token_id for m in markets} | {m.down_token_id for m in markets})

        print(f"[SMOKE] SETTINGS_PROFILE={settings.settings_profile} DRY_RUN={settings.dry_run}")
        print("[SMOKE] RESOLVED_MARKETS")
        for market in markets:
            print(
                "[SMOKE] MARKET",
                f"slug={market.slug}",
                f"up={market.up_token_id}",
                f"down={market.down_token_id}",
            )

        token_metadata_cache = TokenMetadataCache(ttl_seconds=settings.token_metadata_ttl_seconds)
        metadata_updates = {}
        for market in markets:
            metadata_updates.update(market.token_metadata_by_id)
        token_metadata_cache.put_many(metadata_updates)

        strategy = StrategyStateMachine(
            threshold=settings.watch_return_threshold,
            hammer_secs=settings.hammer_secs,
            d_min=min(settings.d_min, 0.1),
            max_entry_price=settings.max_entry_price,
            fee_bps=settings.fee_bps,
            token_metadata_cache=token_metadata_cache,
            rolling_window_seconds=settings.watch_rolling_window_seconds,
            watch_zscore_threshold=settings.watch_zscore_threshold,
            watch_mode_expiry_seconds=settings.watch_mode_expiry_seconds,
        )
        trader = Trader(settings, token_metadata_cache=token_metadata_cache)

        rtds = RTDSFeed(
            settings.rtds_ws_url,
            settings.symbol,
            topic=settings.rtds_topic,
            ping_interval=settings.rtds_ping_interval,
            pong_timeout=settings.rtds_pong_timeout,
            reconnect_delay_min=settings.rtds_reconnect_delay_min,
            reconnect_delay_max=settings.rtds_reconnect_delay_max,
            price_staleness_threshold=settings.price_staleness_threshold,
            log_price_comparison=False,
        )
        clob = CLOBWebSocket(
            settings.clob_ws_url,
            ping_interval=settings.rtds_ping_interval,
            pong_timeout=settings.rtds_pong_timeout,
            reconnect_delay_min=settings.rtds_reconnect_delay_min,
            reconnect_delay_max=settings.rtds_reconnect_delay_max,
            book_staleness_threshold=settings.clob_book_staleness_threshold,
        )

        smoke = SmokeState()

        async def consume_rtds() -> None:
            async for ts, px, metadata in rtds.stream_prices():
                smoke.rtds_events += 1
                strategy.on_price(ts, px, metadata)
                if smoke.rtds_events == 1:
                    seed_strategy_prices(strategy, px, int(ts))

        async def consume_clob() -> None:
            async for top in clob.stream_books(token_ids):
                smoke.clob_events += 1
                strategy.on_book(
                    top.token_id,
                    top.best_bid,
                    top.best_ask,
                    bid_size=top.best_bid_size,
                    ask_size=top.best_ask_size,
                    fill_prob=top.fill_prob,
                    ts=top.ts,
                )
                constraints = clob.get_token_constraints(top.token_id)
                if constraints is not None:
                    trader.update_token_constraints(
                        top.token_id,
                        min_order_size=constraints.min_order_size,
                        tick_size=constraints.tick_size,
                    )

        tasks = [asyncio.create_task(consume_rtds()), asyncio.create_task(consume_clob())]
        started = time.time()
        next_snapshot_at = started + 10

        try:
            while (time.time() - started) < 120:
                await asyncio.sleep(1)

                if time.time() >= next_snapshot_at:
                    next_snapshot_at += 10
                    smoke.snapshots_printed += 1
                    print("[SMOKE] BOOK_SNAPSHOT")
                    for market in markets:
                        for label, token_id in (("UP", market.up_token_id), ("DOWN", market.down_token_id)):
                            snap = strategy.books.get(token_id)
                            if snap is None:
                                print(f"[SMOKE] SNAPSHOT slug={market.slug} side={label} token={token_id} bid=None ask=None")
                            else:
                                print(
                                    f"[SMOKE] SNAPSHOT slug={market.slug} side={label} token={token_id} "
                                    f"bid={snap.bid} ask={snap.ask}"
                                )

                if smoke.rtds_events == 0 or smoke.clob_events == 0:
                    continue

                if not smoke.candidate_found:
                    forced_eval_ts = min(m.end_epoch for m in markets) - 1
                    best = strategy.pick_best(forced_eval_ts, markets, {})
                    if best is not None:
                        smoke.candidate_found = True
                        print(
                            "[SMOKE] CANDIDATE",
                            f"token={best.token_id}",
                            f"direction={best.direction}",
                            f"horizon={best.market.horizon_minutes}m",
                            f"ask={best.ask:.4f}",
                            f"p_hat={best.p_hat:.4f}",
                            f"fill_prob={best.fill_prob:.4f}",
                            f"fee_cost={best.fee_cost:.6f}",
                            f"slippage_cost={best.slippage_cost:.6f}",
                            f"ev_exec={best.ev_exec:.6f}",
                            f"ev={best.ev:.6f}",
                        )

                if smoke.candidate_found and not smoke.order_attempted:
                    forced_eval_ts = min(m.end_epoch for m in markets) - 1
                    best = strategy.pick_best(forced_eval_ts, markets, {})
                    if best is not None:
                        smoke.order_attempted = True
                        smoke.order_result = await trader.buy_fok(best.token_id, best.ask, str(best.market.horizon_minutes))
                        print(
                            "[SMOKE] PAPER_ORDER",
                            f"token={best.token_id}",
                            f"ask={best.ask:.4f}",
                            f"result={smoke.order_result}",
                        )

                if smoke.order_attempted:
                    break
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        print(
            "[SMOKE] SUMMARY",
            f"rtds_events={smoke.rtds_events}",
            f"clob_events={smoke.clob_events}",
            f"snapshots={smoke.snapshots_printed}",
            f"candidate_found={smoke.candidate_found}",
            f"order_result={smoke.order_result}",
        )
    finally:
        await gamma.close()


if __name__ == "__main__":
    asyncio.run(main())
