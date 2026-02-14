from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable

import structlog

from config import Settings
from execution.trader import Trader
from feeds.chainlink_direct import ChainlinkDirectFeed
from feeds.clob_ws import BookTop, CLOBWebSocket
from feeds.rtds import RTDSFeed
from logging_utils import configure_logging
from markets.gamma_cache import GammaCache, UpDownMarket
from metrics import start_metrics_server
from strategy.state_machine import StrategyStateMachine

logger = structlog.get_logger(__name__)


def floor_to_boundary(ts: int, seconds: int) -> int:
    return ts - (ts % seconds)


async def stream_prices_with_fallback(
    rtds: RTDSFeed,
    fallback: ChainlinkDirectFeed,
    *,
    use_fallback_feed: bool,
    price_staleness_threshold: float,
) -> AsyncIterator[tuple[float, float, dict[str, object]]]:
    """Yield RTDS prices and temporarily fall back when RTDS stalls."""
    rtds_iter = rtds.stream_prices().__aiter__()
    fallback_iter = fallback.stream_prices().__aiter__()
    rtds_task = asyncio.create_task(rtds_iter.__anext__())
    fallback_task: asyncio.Task[tuple[float, float, dict[str, object]]] | None = None
    using_fallback = False

    while True:
        if not using_fallback:
            done, _pending = await asyncio.wait({rtds_task}, timeout=price_staleness_threshold)
            if rtds_task in done:
                item = rtds_task.result()
                yield item
                rtds_task = asyncio.create_task(rtds_iter.__anext__())
            else:
                if not use_fallback_feed:
                    continue
                logger.warning("switching_to_chainlink_direct_fallback")
                using_fallback = True
                if fallback_task is None:
                    fallback_task = asyncio.create_task(fallback_iter.__anext__())
            continue

        if fallback_task is None:
            await asyncio.wait({rtds_task}, return_when=asyncio.ALL_COMPLETED)
            item = rtds_task.result()
            logger.info("switching_back_to_rtds")
            using_fallback = False
            yield item
            rtds_task = asyncio.create_task(rtds_iter.__anext__())
            continue

        done, _pending = await asyncio.wait({rtds_task, fallback_task}, return_when=asyncio.FIRST_COMPLETED)

        if rtds_task in done:
            item = rtds_task.result()
            logger.info("switching_back_to_rtds")
            using_fallback = False
            yield item
            rtds_task = asyncio.create_task(rtds_iter.__anext__())

            if fallback_task and not fallback_task.done():
                fallback_task.cancel()
            fallback_task = None
            continue

        try:
            item = fallback_task.result()
        except StopAsyncIteration:
            fallback_task = None
            await asyncio.sleep(0.01)
            continue

        yield item
        fallback_task = asyncio.create_task(fallback_iter.__anext__())


async def stream_clob_with_resubscribe(
    clob: CLOBWebSocket,
    get_token_ids: Callable[[], set[str]],
    *,
    idle_sleep_seconds: float = 1.0,
) -> AsyncIterator[BookTop]:
    """Yield book tops and re-subscribe when token ids roll to a new epoch."""
    while True:
        snapshot = tuple(sorted(get_token_ids()))
        if not snapshot:
            await asyncio.sleep(idle_sleep_seconds)
            continue

        async for top in clob.stream_books(list(snapshot)):
            yield top
            current = tuple(sorted(get_token_ids()))
            if current != snapshot:
                logger.info("clob_token_set_changed_resubscribing", previous=snapshot, current=current)
                break


async def orchestrate() -> None:
    settings = Settings()
    configure_logging()
    start_metrics_server(settings.metrics_host, settings.metrics_port)

    gamma = GammaCache(str(settings.gamma_api_url))
    rtds = RTDSFeed(
        settings.rtds_ws_url,
        settings.symbol,
        topic=settings.rtds_topic,
        ping_interval=settings.rtds_ping_interval,
        pong_timeout=settings.rtds_pong_timeout,
        reconnect_delay_min=settings.rtds_reconnect_delay_min,
        reconnect_delay_max=settings.rtds_reconnect_delay_max,
        price_staleness_threshold=settings.price_staleness_threshold,
        log_price_comparison=settings.log_price_comparison,
    )
    fallback = ChainlinkDirectFeed(settings.chainlink_direct_api_url)
    trader = Trader(settings)
    strategy = StrategyStateMachine(
        threshold=settings.watch_return_threshold,
        hammer_secs=settings.hammer_secs,
        d_min=settings.d_min,
        max_entry_price=settings.max_entry_price,
        fee_bps=settings.fee_bps,
    )

    market_state: dict[str, UpDownMarket] = {}
    token_ids: set[str] = set()

    async def refresh_markets(now_ts: int) -> None:
        nonlocal market_state, token_ids
        s5 = floor_to_boundary(now_ts, 300)
        s15 = floor_to_boundary(now_ts, 900)
        m5 = await gamma.get_market(5, s5)
        m15 = await gamma.get_market(15, s15)
        pairs = [m5, m15]
        market_state = {m.slug: m for m in pairs}
        token_ids = {m.up_token_id for m in pairs} | {m.down_token_id for m in pairs}

    await refresh_markets(int(time.time()))
    clob = CLOBWebSocket(
        settings.clob_ws_url,
        ping_interval=settings.rtds_ping_interval,
        pong_timeout=settings.rtds_pong_timeout,
        reconnect_delay_min=settings.rtds_reconnect_delay_min,
        reconnect_delay_max=settings.rtds_reconnect_delay_max,
    )

    async def process_price(ts: float, px: float, metadata: dict[str, object]) -> None:
        strategy.on_price(ts, px, metadata)
        now = int(ts)
        if now % 60 == 0:
            await refresh_markets(now)

        if not strategy.watch_mode:
            return

        best = strategy.pick_best(now, list(market_state.values()), {})
        if best and best.ev > 0:
            logger.info(
                "trade_candidate",
                ev=best.ev,
                ask=best.ask,
                p_hat=best.p_hat,
                direction=best.direction,
                horizon=best.market.horizon_minutes,
            )
            await trader.buy_fok(best.token_id, best.ask, str(best.market.horizon_minutes))

    async def consume_rtds() -> None:
        async for ts, px, metadata in stream_prices_with_fallback(
            rtds,
            fallback,
            use_fallback_feed=settings.use_fallback_feed,
            price_staleness_threshold=settings.price_staleness_threshold,
        ):
            await process_price(ts, px, metadata)

    async def consume_clob() -> None:
        async for top in stream_clob_with_resubscribe(clob, lambda: token_ids):
            strategy.on_book(top.token_id, top.best_bid, top.best_ask)

    await asyncio.gather(consume_rtds(), consume_clob())


if __name__ == "__main__":
    asyncio.run(orchestrate())
