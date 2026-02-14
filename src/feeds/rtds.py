from __future__ import annotations

import asyncio
import contextlib
import json
import time
from typing import AsyncIterator

import structlog
import websockets

from utils.price_validation import compare_feeds
from utils.time import normalize_ts

logger = structlog.get_logger(__name__)


class RTDSFeed:
    def __init__(
        self,
        ws_url: str,
        symbol: str = "btc/usd",
        topic: str = "crypto_prices_chainlink",
        ping_interval: int = 30,
        pong_timeout: int = 10,
        reconnect_delay_min: int = 1,
        reconnect_delay_max: int = 60,
        price_staleness_threshold: int = 10,
        log_price_comparison: bool = True,
    ) -> None:
        self.ws_url = ws_url
        self.symbol = symbol
        self.topic = topic
        self.ping_interval = ping_interval
        self.pong_timeout = pong_timeout
        self.reconnect_delay_min = reconnect_delay_min
        self.reconnect_delay_max = reconnect_delay_max
        self.price_staleness_threshold = price_staleness_threshold
        self.log_price_comparison = log_price_comparison

        self._last_price_ts: float = 0.0
        self._comparison_count = 0
        self._latest_by_topic_symbol: dict[tuple[str, str], tuple[float, float]] = {}

    async def _heartbeat(self, ws: websockets.WebSocketClientProtocol, failed_pings: list[int]) -> None:
        while True:
            await asyncio.sleep(self.ping_interval)
            try:
                pong = await ws.ping()
                await asyncio.wait_for(pong, timeout=self.pong_timeout)
                failed_pings[0] = 0
            except Exception:
                failed_pings[0] += 1
                logger.warning("rtds_ping_failed", failures=failed_pings[0])
                if failed_pings[0] >= 2:
                    raise RuntimeError("RTDS stale heartbeat: 2 consecutive ping failures")

    async def stream_prices(self) -> AsyncIterator[tuple[float, float, dict[str, object]]]:
        """Yield (timestamp, price, metadata) from Chainlink RTDS feed."""
        logger.info("rtds_startup_feed", topic=self.topic, symbol=self.symbol)
        normalized_symbol = self.symbol.lower()
        subscribed_topics = ("crypto_prices_chainlink", "crypto_prices")

        backoff = self.reconnect_delay_min
        stable_since: float | None = None

        while True:
            failed_pings = [0]
            try:
                async with websockets.connect(self.ws_url, ping_interval=None, ping_timeout=None) as ws:
                    sub = {
                        "action": "subscribe",
                        "subscriptions": [
                            {
                                "topic": topic,
                                "type": "market",
                                "filters": json.dumps({"symbol": self.symbol}),
                            }
                            for topic in subscribed_topics
                        ],
                    }
                    await ws.send(json.dumps(sub))
                    logger.info("rtds_subscribed", subscription=sub)
                    stable_since = time.time()
                    hb_task = asyncio.create_task(self._heartbeat(ws, failed_pings))

                    try:
                        async for message in ws:
                            data = json.loads(message)
                            payload = data.get("payload", {})
                            payload_symbol = str(payload.get("symbol", "")).lower()
                            if payload_symbol != normalized_symbol:
                                continue

                            topic = str(data.get("topic") or payload.get("topic") or self.topic)
                            px = payload.get("value")
                            ts = payload.get("timestamp", payload.get("timestamp_ms"))
                            if px is None or ts is None:
                                continue

                            price = float(px)
                            price_ts = normalize_ts(ts)
                            self._last_price_ts = price_ts
                            self._latest_by_topic_symbol[(topic, payload_symbol)] = (price, price_ts)

                            chainlink_key = ("crypto_prices_chainlink", payload_symbol)
                            spot_key = ("crypto_prices", payload_symbol)
                            chainlink_latest = self._latest_by_topic_symbol.get(chainlink_key)
                            spot_latest = self._latest_by_topic_symbol.get(spot_key)

                            divergence_pct: float | None = None
                            spot_price: float | None = None
                            if chainlink_latest is not None and spot_latest is not None:
                                spot_price = spot_latest[0]
                                divergence_pct = compare_feeds(chainlink_latest[0], spot_price)

                            if topic != "crypto_prices_chainlink":
                                continue

                            metadata: dict[str, object] = {
                                "source": "chainlink_rtds",
                                "topic": topic,
                                "market": payload.get("market", "chainlink"),
                                "received_ts": time.time(),
                                "timestamp": price_ts,
                            }

                            if spot_price is not None:
                                metadata["spot_price"] = spot_price
                            if divergence_pct is not None:
                                metadata["divergence_pct"] = divergence_pct

                            if self.log_price_comparison and self._comparison_count < 100:
                                logger.info(
                                    "price_comparison_sample",
                                    sample_index=self._comparison_count + 1,
                                    chainlink_price=price,
                                    spot_price=spot_price,
                                    divergence_pct=divergence_pct,
                                )
                                self._comparison_count += 1

                            yield price_ts, price, metadata

                            if stable_since and (time.time() - stable_since) >= 300:
                                backoff = self.reconnect_delay_min

                            if time.time() - self._last_price_ts > self.price_staleness_threshold:
                                logger.warning(
                                    "rtds_price_stale",
                                    stale_seconds=(time.time() - self._last_price_ts),
                                )
                    finally:
                        hb_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError, Exception):
                            await hb_task

            except Exception as exc:
                logger.warning("rtds_reconnect", error=str(exc), delay_seconds=backoff)
                await asyncio.sleep(backoff)
                backoff = min(self.reconnect_delay_max, backoff * 2)
