from __future__ import annotations

import asyncio
import time

import structlog

from config import Settings
from execution.trader import Trader
from feeds.chainlink_direct import ChainlinkDirectFeed
from feeds.clob_ws import CLOBWebSocket
from feeds.rtds import RTDSFeed
from logging_utils import configure_logging
from markets.gamma_cache import GammaCache, UpDownMarket
from metrics import start_metrics_server
from strategy.state_machine import StrategyStateMachine

logger = structlog.get_logger(__name__)


def floor_to_boundary(ts: int, seconds: int) -> int:
    return ts - (ts % seconds)


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
        book_staleness_threshold=settings.clob_book_staleness_threshold,
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
        last_rtds_update = time.time()
        async for ts, px, metadata in rtds.stream_prices():
            last_rtds_update = time.time()
            await process_price(ts, px, metadata)

            if settings.use_fallback_feed and (time.time() - last_rtds_update) > settings.price_staleness_threshold:
                logger.warning("switching_to_chainlink_direct_fallback")
                async for fts, fpx, fmeta in fallback.stream_prices():
                    await process_price(fts, fpx, fmeta)
                    if (time.time() - last_rtds_update) <= settings.price_staleness_threshold:
                        logger.info("switching_back_to_rtds")
                        break

    async def consume_clob() -> None:
        while True:
            if not token_ids:
                await asyncio.sleep(1)
                continue
            async for top in clob.stream_books(list(token_ids)):
                strategy.on_book(top.token_id, top.best_bid, top.best_ask)

    await asyncio.gather(consume_rtds(), consume_clob())


if __name__ == "__main__":
    asyncio.run(orchestrate())
