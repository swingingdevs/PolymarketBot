from __future__ import annotations

import asyncio
import contextlib
import json
import time
from typing import AsyncIterator

import structlog
import websockets

from utils.price_validation import compare_feeds

logger = structlog.get_logger(__name__)


class RTDSFeed:
    def __init__(
        self,
        ws_url: str,
        symbol: str = "btc/usd",
        topic: str = "crypto_prices_chainlink",
        spot_topic: str = "crypto_prices",
        spot_max_age_seconds: float = 2.0,
        ping_interval: int = 30,
        pong_timeout: int = 10,
        reconnect_delay_min: int = 1,
        reconnect_delay_max: int = 60,
        reconnect_stability_duration: float = 5.0,
        price_staleness_threshold: int = 10,
        log_price_comparison: bool = True,
    ) -> None:
        self.ws_url = ws_url
        self.symbol = symbol
        self.topic = topic
        self.spot_topic = spot_topic
        self.spot_max_age_seconds = spot_max_age_seconds
        self.ping_interval = ping_interval
        self.pong_timeout = pong_timeout
        self.reconnect_delay_min = reconnect_delay_min
        self.reconnect_delay_max = reconnect_delay_max
        self.reconnect_stability_duration = reconnect_stability_duration
        self.price_staleness_threshold = price_staleness_threshold
        self.log_price_comparison = log_price_comparison

        self._last_price_ts: float = 0.0
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

        backoff = self.reconnect_delay_min

        while True:
            failed_pings = [0]
            stable_since: float | None = None
            stability_met = False
            try:
                async with websockets.connect(self.ws_url, ping_interval=None, ping_timeout=None) as ws:
                    sub = {
                        "action": "subscribe",
                        "subscriptions": [
                            {
                                "topic": self.topic,
                                "type": "*",
                                "filters": json.dumps({"symbol": normalized_symbol}),
                            },
                            {
                                "topic": self.spot_topic,
                                "type": "*",
                                "filters": json.dumps({"symbol": normalized_symbol}),
                            },
                        ],
                    }
                    await ws.send(json.dumps(sub))
                    logger.info("rtds_subscribed", subscription=sub)
                    stable_since = time.time()
                    hb_task = asyncio.create_task(self._heartbeat(ws, failed_pings))

                    try:
                        async for message in ws:
                            if (
                                not stability_met
                                and stable_since is not None
                                and (time.time() - stable_since) >= self.reconnect_stability_duration
                            ):
                                backoff = self.reconnect_delay_min
                                stability_met = True

                            data = json.loads(message)
                            payload = data.get("payload", {})
                            payload_symbol = str(payload.get("symbol", "")).lower()
                            if payload_symbol != normalized_symbol:
                                continue

                            topic = str(data.get("topic") or self.topic)

                            px = payload.get("value")
                            ts_raw = payload.get("timestamp", data.get("timestamp"))
                            if px is None or ts_raw is None:
                                continue

                            price = float(px)
                            ts_value = float(ts_raw)
                            price_ts = ts_value / 1000.0 if ts_value > 1e12 else ts_value
                            self._last_price_ts = price_ts
                            self._latest_by_topic_symbol[(topic, payload_symbol)] = (price, price_ts)

                            if topic != self.topic:
                                continue

                            if not stability_met:
                                backoff = self.reconnect_delay_min
                                stability_met = True

                            metadata: dict[str, object] = {
                                "source": "chainlink_rtds",
                                "topic": topic,
                                "market": payload.get("market", "chainlink"),
                                "received_ts": time.time(),
                                "timestamp": price_ts,
                            }
                            spot_latest = self._latest_by_topic_symbol.get((self.spot_topic, payload_symbol))
                            if spot_latest is not None and (price_ts - spot_latest[1]) <= self.spot_max_age_seconds:
                                metadata["spot_price"] = spot_latest[0]
                                metadata["divergence_pct"] = compare_feeds(price, spot_latest[0])

                            yield price_ts, price, metadata
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
