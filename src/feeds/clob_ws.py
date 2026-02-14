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


class CLOBWebSocket:
    def __init__(
        self,
        ws_url: str,
        ping_interval: int = 30,
        pong_timeout: int = 10,
        reconnect_delay_min: int = 1,
        reconnect_delay_max: int = 60,
    ) -> None:
        self.ws_url = ws_url
        self.ping_interval = ping_interval
        self.pong_timeout = pong_timeout
        self.reconnect_delay_min = reconnect_delay_min
        self.reconnect_delay_max = reconnect_delay_max

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

    async def stream_books(self, token_ids: list[str]) -> AsyncIterator[BookTop]:
        backoff = self.reconnect_delay_min
        last_update = 0.0

        while True:
            failed_pings = [0]
            try:
                async with websockets.connect(self.ws_url, ping_interval=None, ping_timeout=None) as ws:
                    sub = {"type": "market", "assets_ids": token_ids, "channel": "book"}
                    await ws.send(json.dumps(sub))
                    hb_task = asyncio.create_task(self._heartbeat(ws, failed_pings))

                    try:
                        async for raw in ws:
                            data = json.loads(raw)
                            if data.get("event_type") != "book":
                                continue
                            token_id = str(data.get("asset_id"))
                            bids = data.get("bids", [])
                            asks = data.get("asks", [])
                            bid = float(bids[0][0]) if bids else None
                            ask = float(asks[0][0]) if asks else None
                            ts = float(data.get("timestamp", time.time()))
                            last_update = time.time()
                            yield BookTop(token_id=token_id, best_bid=bid, best_ask=ask, ts=ts)

                            if time.time() - last_update > 5:
                                logger.warning("clob_orderbook_stale", seconds_without_updates=time.time() - last_update)

                            backoff = self.reconnect_delay_min
                    finally:
                        hb_task.cancel()
                        with contextlib.suppress(Exception):
                            await hb_task
            except Exception as exc:
                logger.warning("clob_reconnect", error=str(exc), delay_seconds=backoff)
                await asyncio.sleep(backoff)
                backoff = min(self.reconnect_delay_max, backoff * 2)
