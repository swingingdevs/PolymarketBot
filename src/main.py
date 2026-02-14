from __future__ import annotations

import asyncio
import contextlib
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
    clob_resubscribe_event = asyncio.Event()
    token_change_reason = "initial_bootstrap"
    last_token_change_at = 0.0
    clob_resubscribe_debounce_seconds = 2.0

    async def wait_for_token_set_to_stabilize() -> None:
        nonlocal last_token_change_at
        while True:
            observed_change_at = last_token_change_at
            await asyncio.sleep(clob_resubscribe_debounce_seconds)
            if observed_change_at == last_token_change_at:
                return

    async def refresh_markets(now_ts: int) -> None:
        nonlocal market_state, token_ids, token_change_reason, last_token_change_at
        s5 = floor_to_boundary(now_ts, 300)
        s15 = floor_to_boundary(now_ts, 900)
        m5 = await gamma.get_market(5, s5)
        m15 = await gamma.get_market(15, s15)
        pairs = [m5, m15]
        market_state = {m.slug: m for m in pairs}
        new_token_ids = {m.up_token_id for m in pairs} | {m.down_token_id for m in pairs}
        if new_token_ids != token_ids:
            added_tokens = sorted(new_token_ids - token_ids)
            removed_tokens = sorted(token_ids - new_token_ids)
            token_change_reason = f"refresh_markets_boundary_5m={s5}_15m={s15}"
            last_token_change_at = time.monotonic()
            logger.info(
                "clob_token_set_changed",
                reason=token_change_reason,
                added_tokens=added_tokens,
                removed_tokens=removed_tokens,
                previous_token_count=len(token_ids),
                new_token_count=len(new_token_ids),
                debounce_seconds=clob_resubscribe_debounce_seconds,
            )
            token_ids = new_token_ids
            clob_resubscribe_event.set()

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

    feed_state = {
        "mode": "rtds",
        "last_rtds_update": time.time(),
    }
    fallback_task: asyncio.Task[None] | None = None

    async def consume_rtds() -> None:
        async for ts, px, metadata in rtds.stream_prices():
            feed_state["last_rtds_update"] = time.time()
            if feed_state["mode"] != "rtds":
                continue
            await process_price(ts, px, metadata)

    async def consume_fallback() -> None:
        async for ts, px, metadata in fallback.stream_prices():
            if feed_state["mode"] != "fallback":
                return
            await process_price(ts, px, metadata)

    async def monitor_rtds_staleness() -> None:
        nonlocal fallback_task
        check_interval = max(0.1, min(1.0, settings.price_staleness_threshold / 2))
        while True:
            await asyncio.sleep(check_interval)
            age = time.time() - float(feed_state["last_rtds_update"])
            stale = age > settings.price_staleness_threshold

            if stale and settings.use_fallback_feed:
                if feed_state["mode"] != "fallback":
                    feed_state["mode"] = "fallback"
                    logger.warning("enter_fallback_chainlink_direct", staleness_seconds=age)
                    fallback_task = asyncio.create_task(consume_fallback())
                else:
                    logger.warning("still_degraded_using_fallback", staleness_seconds=age)
                continue

            if feed_state["mode"] == "fallback":
                feed_state["mode"] = "rtds"
                logger.info("recovered_rtds_freshness", staleness_seconds=age)
                if fallback_task and not fallback_task.done():
                    fallback_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await fallback_task
                fallback_task = None

    async def consume_clob() -> None:
        nonlocal token_change_reason
        subscribe_reason = "initial_bootstrap"
        while True:
            if not token_ids:
                await asyncio.sleep(1)
                continue

            subscribed_tokens = sorted(token_ids)
            logger.info(
                "clob_subscribe",
                reason=subscribe_reason,
                token_count=len(subscribed_tokens),
                token_ids=subscribed_tokens,
            )
            stream = clob.stream_books(subscribed_tokens)
            stream_iter = stream.__aiter__()
            while True:
                next_book_task = asyncio.create_task(stream_iter.__anext__())
                resubscribe_task = asyncio.create_task(clob_resubscribe_event.wait())
                done, pending = await asyncio.wait(
                    {next_book_task, resubscribe_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if next_book_task in done:
                    resubscribe_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await resubscribe_task
                    top = next_book_task.result()
                    strategy.on_book(top.token_id, top.best_bid, top.best_ask)
                    continue

                next_book_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await next_book_task
                clob_resubscribe_event.clear()
                await wait_for_token_set_to_stabilize()
                subscribe_reason = token_change_reason
                logger.info(
                    "clob_resubscribe_triggered",
                    reason=subscribe_reason,
                    debounce_seconds=clob_resubscribe_debounce_seconds,
                    token_count=len(token_ids),
                )
                break

    await asyncio.gather(consume_rtds(), consume_clob(), monitor_rtds_staleness())


if __name__ == "__main__":
    asyncio.run(orchestrate())
