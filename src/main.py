from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import AsyncIterator, Awaitable, Callable

import structlog

from config import Settings
from execution.trader import Trader
from feeds.chainlink_direct import ChainlinkDirectFeed
from feeds.clob_ws import BookTop, CLOBWebSocket
from feeds.rtds import RTDSFeed
from logging_utils import configure_logging
from markets.gamma_cache import GammaCache, UpDownMarket
from markets.token_metadata_cache import TokenMetadataCache
from metrics import HAMMER_ATTEMPTED, HAMMER_FILLED, KILL_SWITCH_ACTIVE, STALE_FEED, start_metrics_server
from strategy.calibration import load_probability_calibrator
from strategy.state_machine import StrategyStateMachine

logger = structlog.get_logger(__name__)


def floor_to_boundary(ts: int, seconds: int) -> int:
    return ts - (ts % seconds)


def current_start_epoch(ts: int, interval_seconds: int) -> int:
    end_epoch = ((ts // interval_seconds) * interval_seconds) + interval_seconds
    return end_epoch - interval_seconds


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
    token_metadata_cache = TokenMetadataCache(ttl_seconds=settings.token_metadata_ttl_seconds)
    trader = Trader(settings, token_metadata_cache=token_metadata_cache)
    calibrator = load_probability_calibrator(
        method=settings.calibration_method,
        params_path=settings.calibration_params_path or None,
        logistic_coef=settings.calibration_logistic_coef,
        logistic_intercept=settings.calibration_logistic_intercept,
    )
    strategy = StrategyStateMachine(
        threshold=settings.watch_return_threshold,
        hammer_secs=settings.hammer_secs,
        d_min=settings.d_min,
        max_entry_price=settings.max_entry_price,
        fee_bps=settings.fee_bps,
        expected_notional_usd=settings.quote_size_usd,
        probability_calibrator=calibrator,
        calibration_input=settings.calibration_input,
        token_metadata_cache=token_metadata_cache,
        rolling_window_seconds=settings.watch_rolling_window_seconds,
        watch_zscore_threshold=settings.watch_zscore_threshold,
        watch_mode_expiry_seconds=settings.watch_mode_expiry_seconds,
    )

    market_state: dict[str, UpDownMarket] = {}
    token_ids: set[str] = set()
    clob_resubscribe_event = asyncio.Event()
    token_change_reason = "initial_bootstrap"
    last_token_change_at = 0.0
    clob_resubscribe_debounce_seconds = 2.0
    last_refresh_ts = 0

    async def wait_for_token_set_to_stabilize() -> None:
        nonlocal last_token_change_at
        while True:
            observed_change_at = last_token_change_at
            await asyncio.sleep(clob_resubscribe_debounce_seconds)
            if observed_change_at == last_token_change_at:
                return

    async def refresh_markets(now_ts: int) -> None:
        nonlocal market_state, token_ids, token_change_reason, last_token_change_at
        s5 = current_start_epoch(now_ts, 300)
        s15 = current_start_epoch(now_ts, 900)
        n5 = s5 + 300
        n15 = s15 + 900
        current_5m, current_15m, next_5m, next_15m = await asyncio.gather(
            gamma.get_market(5, s5),
            gamma.get_market(15, s15),
            gamma.get_market(5, n5),
            gamma.get_market(15, n15),
        )
        active_markets = [current_5m, current_15m]
        subscribed_markets = [current_5m, current_15m, next_5m, next_15m]
        market_state = {m.slug: m for m in active_markets}

        metadata_updates = {}
        for market in subscribed_markets:
            metadata_updates.update(market.token_metadata_by_id)
        if metadata_updates:
            token_metadata_cache.put_many(metadata_updates)

        new_token_ids = {m.up_token_id for m in subscribed_markets} | {m.down_token_id for m in subscribed_markets}
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

    last_refresh_ts = int(time.time())
    await refresh_markets(last_refresh_ts)
    clob = CLOBWebSocket(
        settings.clob_ws_base,
        ping_interval=10,
        pong_timeout=settings.rtds_pong_timeout,
        reconnect_delay_min=settings.rtds_reconnect_delay_min,
        reconnect_delay_max=settings.rtds_reconnect_delay_max,
        book_staleness_threshold=settings.clob_book_staleness_threshold,
    )

    async def process_price(ts: float, px: float, metadata: dict[str, object]) -> None:
        nonlocal last_refresh_ts
        strategy.on_price(ts, px, metadata)
        now = int(ts)
        if (now // 60) != (last_refresh_ts // 60):
            await refresh_markets(now)
            last_refresh_ts = now

        divergence_raw = metadata.get("divergence_pct")
        divergence_pct = float(divergence_raw) if divergence_raw is not None else None
        kill_switch_active = bool(kill_switch_state["active"])

        if divergence_pct is not None:
            kill_switch_state["last_divergence_pct"] = divergence_pct
            threshold = settings.divergence_threshold_pct
            if divergence_pct >= threshold:
                breach_started_at = kill_switch_state["breach_started_at"]
                if breach_started_at is None:
                    kill_switch_state["breach_started_at"] = ts
                elif not kill_switch_active and (ts - float(breach_started_at)) >= settings.divergence_sustain_seconds:
                    kill_switch_state["active"] = True
                    KILL_SWITCH_ACTIVE.set(1)
                    logger.error(
                        "divergence_kill_switch_activated",
                        divergence_pct=divergence_pct,
                        threshold_pct=threshold,
                        sustain_seconds=settings.divergence_sustain_seconds,
                    )
            else:
                kill_switch_state["breach_started_at"] = None
                if kill_switch_active:
                    kill_switch_state["active"] = False
                    KILL_SWITCH_ACTIVE.set(0)
                    logger.info(
                        "divergence_kill_switch_cleared",
                        divergence_pct=divergence_pct,
                        threshold_pct=threshold,
                    )

        if not strategy.watch_mode:
            return

        best = strategy.pick_best(now, list(market_state.values()), {})
        if best and best.ev > 0:
            if bool(kill_switch_state["active"]):
                logger.warning(
                    "order_blocked_by_divergence_kill_switch",
                    token_id=best.token_id,
                    divergence_pct=kill_switch_state["last_divergence_pct"],
                )
                return

            logger.info(
                "trade_candidate",
                ev=best.ev,
                ask=best.ask,
                p_hat=best.p_hat,
                direction=best.direction,
                horizon=best.market.horizon_minutes,
            )
            HAMMER_ATTEMPTED.inc()
            filled = await trader.buy_fok(best.token_id, best.ask, str(best.market.horizon_minutes))
            if filled:
                HAMMER_FILLED.inc()

    feed_state = {
        "mode": "rtds",
        "last_rtds_update": time.time(),
    }
    kill_switch_state: dict[str, object] = {
        "breach_started_at": None,
        "active": False,
        "last_divergence_pct": None,
    }
    KILL_SWITCH_ACTIVE.set(0)
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
                    STALE_FEED.inc()
                    fallback_task = asyncio.create_task(consume_fallback())
                else:
                    logger.warning("still_degraded_using_fallback", staleness_seconds=age)
                    STALE_FEED.inc()
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
                    continue

                next_book_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await next_book_task
                clob_resubscribe_event.clear()
                await wait_for_token_set_to_stabilize()
                subscribe_reason = token_change_reason
                updated_tokens = sorted(token_ids)
                await clob.update_subscriptions(updated_tokens)
                logger.info(
                    "clob_resubscribe_triggered",
                    reason=subscribe_reason,
                    debounce_seconds=clob_resubscribe_debounce_seconds,
                    token_count=len(updated_tokens),
                    token_ids=updated_tokens,
                )

    async def run_resilient(name: str, worker: Callable[[], Awaitable[None]]) -> None:
        backoff = 1.0
        while True:
            try:
                await worker()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("supervisor_worker_crashed", worker=name, error=str(exc), backoff_seconds=backoff)
                await asyncio.sleep(backoff)
                backoff = min(60.0, backoff * 2)

    await asyncio.gather(
        run_resilient("consume_rtds", consume_rtds),
        run_resilient("consume_clob", consume_clob),
        run_resilient("monitor_rtds_staleness", monitor_rtds_staleness),
    )


if __name__ == "__main__":
    asyncio.run(orchestrate())
