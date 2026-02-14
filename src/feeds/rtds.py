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
        symbol: str = "BTC/USD",
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

    @staticmethod
    def _extract_binance_price(payload: dict[str, object]) -> float | None:
        candidates = (
            payload.get("binance_price"),
            payload.get("binancePrice"),
            payload.get("spot_price"),
            payload.get("spotPrice"),
        )
        for val in candidates:
            if val is not None:
                return float(val)

        sources = payload.get("sources")
        if isinstance(sources, dict):
            for key in ("binance", "binance_spot", "spot"):
                if key in sources:
                    try:
                        return float(sources[key])
                    except Exception:
                        return None
        return None

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
        stable_since: float | None = None

        while True:
            failed_pings = [0]
            try:
                async with websockets.connect(self.ws_url, ping_interval=None, ping_timeout=None) as ws:
                    sub = {
                        "action": "subscribe",
                        "subscriptions": [
                            {
                                "topic": self.topic,
                                "type": "market",
                                "filters": json.dumps({"symbol": self.symbol}),
                            }
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

                            px = payload.get("value")
                            ts = payload.get("timestamp_ms")
                            if px is None or ts is None:
                                continue

                            chainlink_price = float(px)
                            price_ts_ms = float(ts)
                            price_ts = price_ts_ms / 1000.0
                            self._last_price_ts = price_ts
                            metadata: dict[str, object] = {
                                "source": "chainlink_rtds",
                                "topic": self.topic,
                                "market": payload.get("market", "chainlink"),
                                "received_ts": time.time(),
                                "timestamp": price_ts,
                                "timestamp_ms": price_ts_ms,
                                "timestamp_s": price_ts,
                            }

                            binance_price = self._extract_binance_price(payload)
                            divergence_pct: float | None = None
                            if binance_price is not None:
                                divergence_pct = compare_feeds(chainlink_price, binance_price)
                                metadata["binance_price"] = binance_price
                                metadata["divergence_pct"] = divergence_pct

                            if self.log_price_comparison and self._comparison_count < 100:
                                logger.info(
                                    "price_comparison_sample",
                                    sample_index=self._comparison_count + 1,
                                    chainlink_price=chainlink_price,
                                    binance_price=binance_price,
                                    divergence_pct=divergence_pct,
                                )
                                self._comparison_count += 1

                            yield price_ts, chainlink_price, metadata

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
