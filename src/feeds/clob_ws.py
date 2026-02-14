from __future__ import annotations

import asyncio
import contextlib
import json
import time
from dataclasses import dataclass
from typing import AsyncIterator

import structlog
import websockets

logger = structlog.get_logger(__name__)


@dataclass(slots=True)
class BookTop:
    token_id: str
    best_bid: float | None
    best_ask: float | None
    ts: float
    best_bid_size: float | None = None
    best_ask_size: float | None = None
    fill_prob: float | None = None


@dataclass(slots=True)
class TokenConstraints:
    min_order_size: float | None = None
    tick_size: float | None = None
    updated_at: float = 0.0


class CLOBWebSocket:
    def __init__(
        self,
        ws_url: str,
        ping_interval: int = 30,
        pong_timeout: int = 10,
        reconnect_delay_min: int = 1,
        reconnect_delay_max: int = 60,
        book_staleness_threshold: float = 10,
        stale_after_seconds: float | None = None,
    ) -> None:
        self.ws_url = ws_url
        self.ping_interval = ping_interval
        self.pong_timeout = pong_timeout
        self.reconnect_delay_min = reconnect_delay_min
        self.reconnect_delay_max = reconnect_delay_max
        self.book_staleness_threshold = float(stale_after_seconds) if stale_after_seconds is not None else float(book_staleness_threshold)
        self.token_metadata_cache: dict[str, TokenConstraints] = {}

    @staticmethod
    def _coerce_positive_float(value: object) -> float | None:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        return parsed if parsed > 0 else None

    def _update_token_constraints(self, token_id: str, *, min_order_size: object = None, tick_size: object = None) -> bool:
        if not token_id:
            return False

        parsed_min_order_size = self._coerce_positive_float(min_order_size)
        parsed_tick_size = self._coerce_positive_float(tick_size)
        if parsed_min_order_size is None and parsed_tick_size is None:
            return False

        current = self.token_metadata_cache.get(token_id, TokenConstraints())
        changed = False

        if parsed_min_order_size is not None and current.min_order_size != parsed_min_order_size:
            current.min_order_size = parsed_min_order_size
            changed = True
        if parsed_tick_size is not None and current.tick_size != parsed_tick_size:
            current.tick_size = parsed_tick_size
            changed = True

        if changed:
            current.updated_at = time.time()
            self.token_metadata_cache[token_id] = current
            logger.info(
                "token_constraints_updated",
                token_id=token_id,
                min_order_size=current.min_order_size,
                tick_size=current.tick_size,
            )
        return changed

    def get_token_constraints(self, token_id: str) -> TokenConstraints | None:
        return self.token_metadata_cache.get(token_id)

    async def _heartbeat(self, ws: websockets.WebSocketClientProtocol, failed_pings: list[int]) -> None:
        while True:
            await asyncio.sleep(self.ping_interval)
            try:
                pong = await ws.ping()
                await asyncio.wait_for(pong, timeout=self.pong_timeout)
                failed_pings[0] = 0
            except Exception:
                failed_pings[0] += 1
                logger.warning("clob_ping_failed", failures=failed_pings[0])
                if failed_pings[0] >= 2:
                    raise RuntimeError("CLOB stale heartbeat")

    async def _stale_watchdog(self, last_update: list[float], token_ids: list[str], channel: str) -> None:
        last_warning_at = 0.0
        check_interval = max(0.01, self.book_staleness_threshold / 2)

        while True:
            await asyncio.sleep(check_interval)
            stale_seconds = time.time() - last_update[0]
            if stale_seconds <= self.book_staleness_threshold:
                continue
            if (time.time() - last_warning_at) < self.book_staleness_threshold:
                continue

            logger.warning(
                "clob_orderbook_stale",
                stale_seconds=stale_seconds,
                staleness_threshold_seconds=self.book_staleness_threshold,
                channel=channel,
                token_ids=token_ids,
            )
            last_warning_at = time.time()

    async def stream_books(self, token_ids: list[str]) -> AsyncIterator[BookTop]:
        backoff = self.reconnect_delay_min
        channel = "book"

        while True:
            failed_pings = [0]
            try:
                async with websockets.connect(self.ws_url, ping_interval=None, ping_timeout=None) as ws:
                    sub = {"type": "market", "assets_ids": token_ids, "channel": channel}
                    await ws.send(json.dumps(sub))
                    hb_task = asyncio.create_task(self._heartbeat(ws, failed_pings))
                    last_update = [time.time()]
                    stale_task = asyncio.create_task(self._stale_watchdog(last_update, token_ids, channel))

                    try:
                        while True:
                            raw = await ws.recv() if hasattr(ws, "recv") else await ws.__anext__()
                            data = json.loads(raw)
                            token_id = str(data.get("asset_id") or data.get("token_id") or "")
                            self._update_token_constraints(
                                token_id,
                                min_order_size=data.get("min_order_size"),
                                tick_size=data.get("tick_size"),
                            )
                            if data.get("event_type") != "book":
                                continue
                            bids = data.get("bids", [])
                            asks = data.get("asks", [])
                            bid = float(bids[0][0]) if bids else None
                            ask = float(asks[0][0]) if asks else None
                            bid_size = float(bids[0][1]) if bids and len(bids[0]) > 1 else None
                            ask_size = float(asks[0][1]) if asks and len(asks[0]) > 1 else None
                            fill_prob = data.get("fill_prob")
                            if fill_prob is not None:
                                fill_prob = float(fill_prob)
                            ts = float(data.get("timestamp", time.time()))
                            last_update[0] = time.time()
                            yield BookTop(
                                token_id=token_id,
                                best_bid=bid,
                                best_ask=ask,
                                best_bid_size=bid_size,
                                best_ask_size=ask_size,
                                fill_prob=fill_prob,
                                ts=ts,
                            )

                            backoff = self.reconnect_delay_min
                    finally:
                        hb_task.cancel()
                        stale_task.cancel()
                        with contextlib.suppress(Exception):
                            await hb_task
                        with contextlib.suppress(Exception):
                            await stale_task
            except Exception as exc:
                logger.warning("clob_reconnect", error=str(exc), delay_seconds=backoff)
                await asyncio.sleep(backoff)
                backoff = min(self.reconnect_delay_max, backoff * 2)
