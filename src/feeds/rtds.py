from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Mapping, Sequence
from typing import AsyncIterator

import orjson
import structlog
import websockets

from utils.price_validation import compare_feeds
from utils.symbols import normalize_symbol

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
        self.symbol = normalize_symbol(symbol)
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

        normalized_symbol = self.symbol
        encoded_filters = orjson.dumps({"symbol": normalized_symbol}).decode("utf-8")
        self._subscription_bytes_by_symbol: dict[str, bytes] = {
            normalized_symbol: orjson.dumps(
                {
                    "action": "subscribe",
                    "subscriptions": [
                        {"topic": self.topic, "type": "*", "filters": encoded_filters},
                        {"topic": self.spot_topic, "type": "*", "filters": encoded_filters},
                    ],
                }
            )
        }

        self._last_price_ts: float = 0.0
        self._latest_by_topic_symbol: dict[tuple[str, str], tuple[float, float]] = {}

    @staticmethod
    def _shape_redacted(value: object, *, _depth: int = 0) -> object:
        if _depth >= 3:
            return type(value).__name__
        if isinstance(value, Mapping):
            items = list(value.items())[:8]
            return {str(key): RTDSFeed._shape_redacted(item, _depth=_depth + 1) for key, item in items}
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            if not value:
                return []
            return [RTDSFeed._shape_redacted(value[0], _depth=_depth + 1)]
        return type(value).__name__

    def _log_dropped_message(self, *, reason_code: str, data: object) -> None:
        logger.warning(
            "rtds_message_dropped",
            reason_code=reason_code,
            sample_shape=self._shape_redacted(data),
        )

    @staticmethod
    def _find_first_nested_value(candidate: object, keys: tuple[str, ...], *, _depth: int = 0) -> object | None:
        if _depth > 4:
            return None
        if isinstance(candidate, Mapping):
            for key in keys:
                value = candidate.get(key)
                if value is not None:
                    return value
            for value in candidate.values():
                found = RTDSFeed._find_first_nested_value(value, keys, _depth=_depth + 1)
                if found is not None:
                    return found
        elif isinstance(candidate, Sequence) and not isinstance(candidate, (str, bytes, bytearray)):
            for item in candidate[:8]:
                found = RTDSFeed._find_first_nested_value(item, keys, _depth=_depth + 1)
                if found is not None:
                    return found
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
        normalized_symbol = self.symbol

        backoff = self.reconnect_delay_min

        while True:
            failed_pings = [0]
            stable_since: float | None = None
            backoff_reset = False
            try:
                async with websockets.connect(self.ws_url, ping_interval=None, ping_timeout=None) as ws:
                    sub = self._subscription_bytes_by_symbol.get(normalized_symbol)
                    if sub is None:
                        encoded_filters = orjson.dumps({"symbol": normalized_symbol}).decode("utf-8")
                        sub = orjson.dumps(
                            {
                                "action": "subscribe",
                                "subscriptions": [
                                    {"topic": self.topic, "type": "*", "filters": encoded_filters},
                                    {"topic": self.spot_topic, "type": "*", "filters": encoded_filters},
                                ],
                            }
                        )
                        self._subscription_bytes_by_symbol[normalized_symbol] = sub
                    await ws.send(sub)
                    logger.info("rtds_subscribed", symbol=normalized_symbol, topic=self.topic, spot_topic=self.spot_topic)
                    stable_since = time.time()
                    hb_task = asyncio.create_task(self._heartbeat(ws, failed_pings))

                    try:
                        async for message in ws:
                            if (
                                not backoff_reset
                                and stable_since is not None
                                and (time.time() - stable_since) >= self.reconnect_stability_duration
                            ):
                                backoff = self.reconnect_delay_min
                                backoff_reset = True

                            data = orjson.loads(message)
                            payload = data.get("payload", {})
                            try:
                                payload_symbol = normalize_symbol(str(payload.get("symbol", "")))
                            except ValueError:
                                continue
                            if payload_symbol != normalized_symbol:
                                self._log_dropped_message(reason_code="missing_symbol", data=data)
                                continue

                            topic = str(data.get("topic") or self.topic)
                            if topic not in {self.topic, self.spot_topic}:
                                self._log_dropped_message(reason_code="topic_mismatch", data=data)
                                continue

                            px = self._find_first_nested_value(payload, ("value", "price", "px"))
                            ts_raw = self._find_first_nested_value(payload, ("timestamp", "ts", "time"))
                            if ts_raw is None:
                                ts_raw = data.get("timestamp")
                            if px is None:
                                self._log_dropped_message(reason_code="missing_price", data=data)
                                continue
                            if ts_raw is None:
                                self._log_dropped_message(reason_code="missing_timestamp", data=data)
                                continue

                            try:
                                price = float(px)
                            except (TypeError, ValueError):
                                self._log_dropped_message(reason_code="missing_price", data=data)
                                continue
                            try:
                                ts_value = float(ts_raw)
                            except (TypeError, ValueError):
                                self._log_dropped_message(reason_code="missing_timestamp", data=data)
                                continue
                            price_ts = ts_value / 1000.0 if ts_value > 1e12 else ts_value
                            self._last_price_ts = price_ts
                            self._latest_by_topic_symbol[(topic, payload_symbol)] = (price, price_ts)

                            if topic != self.topic:
                                continue

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
