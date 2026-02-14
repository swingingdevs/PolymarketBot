from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import hmac
import time
from collections.abc import AsyncIterator

import orjson
import structlog
import websockets

logger = structlog.get_logger(__name__)


class CoinbaseSpotFeed:
    def __init__(
        self,
        *,
        product_id: str = "BTC-USD",
        public_ws_url: str = "wss://ws-feed.exchange.coinbase.com",
        direct_ws_url: str | None = None,
        api_key: str = "",
        api_secret: str = "",
        api_passphrase: str = "",
        ping_interval: int = 20,
        pong_timeout: int = 10,
        reconnect_delay_min: int = 1,
        reconnect_delay_max: int = 60,
    ) -> None:
        self.product_id = product_id
        self.public_ws_url = public_ws_url
        self.direct_ws_url = direct_ws_url
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.ping_interval = ping_interval
        self.pong_timeout = pong_timeout
        self.reconnect_delay_min = reconnect_delay_min
        self.reconnect_delay_max = reconnect_delay_max

        self._last_message_received_ts = 0.0
        self._last_heartbeat_ts = 0.0

    def _auth_fields(self) -> dict[str, str] | None:
        if not (self.api_key and self.api_secret and self.api_passphrase):
            return None
        timestamp = str(int(time.time()))
        message = f"{timestamp}GET/users/self/verify"
        signature = base64.b64encode(
            hmac.new(base64.b64decode(self.api_secret), message.encode("utf-8"), hashlib.sha256).digest()
        ).decode("utf-8")
        return {
            "signature": signature,
            "key": self.api_key,
            "passphrase": self.api_passphrase,
            "timestamp": timestamp,
        }

    def _subscribe_payload(self, authenticated: bool) -> bytes:
        payload: dict[str, object] = {
            "type": "subscribe",
            "product_ids": [self.product_id],
            "channels": [
                {"name": "ticker", "product_ids": [self.product_id]},
                {"name": "heartbeat", "product_ids": [self.product_id]},
                {"name": "level2", "product_ids": [self.product_id]},
            ],
        }
        if authenticated:
            auth = self._auth_fields()
            if auth:
                payload.update(auth)
        return orjson.dumps(payload)

    async def _heartbeat(self, ws: websockets.WebSocketClientProtocol, failed_pings: list[int]) -> None:
        while True:
            await asyncio.sleep(self.ping_interval)
            try:
                pong = await ws.ping()
                await asyncio.wait_for(pong, timeout=self.pong_timeout)
                failed_pings[0] = 0
            except Exception:
                failed_pings[0] += 1
                logger.warning("coinbase_ping_failed", failures=failed_pings[0])
                if failed_pings[0] >= 2:
                    raise RuntimeError("coinbase websocket stale heartbeat")

    def _feed_targets(self) -> list[tuple[str, bool, str]]:
        targets: list[tuple[str, bool, str]] = [(self.public_ws_url, False, "ws-feed")]
        if self.direct_ws_url:
            targets.insert(0, (self.direct_ws_url, True, "ws-direct"))
        return targets

    @staticmethod
    def _parse_ts(value: object, fallback: float) -> float:
        if not value:
            return fallback
        try:
            as_float = float(value)
            return as_float / 1000.0 if as_float > 1e12 else as_float
        except Exception:
            text = str(value)
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            with contextlib.suppress(Exception):
                from datetime import datetime

                return datetime.fromisoformat(text).timestamp()
        return fallback

    async def stream_prices(self) -> AsyncIterator[tuple[float, float, dict[str, object]]]:
        backoff = self.reconnect_delay_min
        while True:
            for ws_url, authenticated, feed_name in self._feed_targets():
                failed_pings = [0]
                try:
                    async with websockets.connect(ws_url, ping_interval=None, ping_timeout=None) as ws:
                        await ws.send(self._subscribe_payload(authenticated=authenticated))
                        hb_task = asyncio.create_task(self._heartbeat(ws, failed_pings))
                        logger.info("coinbase_ws_subscribed", feed=feed_name, product_id=self.product_id)

                        try:
                            async for raw in ws:
                                received_ts = time.time()
                                data = orjson.loads(raw)
                                msg_type = str(data.get("type", ""))
                                if msg_type == "heartbeat":
                                    self._last_heartbeat_ts = received_ts
                                    continue
                                if msg_type != "ticker":
                                    continue

                                bid_raw = data.get("best_bid")
                                ask_raw = data.get("best_ask")
                                if bid_raw is None or ask_raw is None:
                                    continue

                                bid = float(bid_raw)
                                ask = float(ask_raw)
                                if bid <= 0 or ask <= 0:
                                    continue

                                mid = (bid + ask) / 2.0
                                payload_ts = self._parse_ts(data.get("time") or data.get("timestamp"), fallback=received_ts)
                                self._last_message_received_ts = received_ts
                                heartbeat_age = (
                                    received_ts - self._last_heartbeat_ts if self._last_heartbeat_ts > 0 else None
                                )
                                metadata: dict[str, object] = {
                                    "source": "coinbase_spot",
                                    "feed": feed_name,
                                    "product_id": self.product_id,
                                    "received_ts": received_ts,
                                    "timestamp": payload_ts,
                                    "lag_seconds": max(0.0, received_ts - payload_ts),
                                    "heartbeat_age_seconds": heartbeat_age,
                                }
                                yield payload_ts, mid, metadata
                        finally:
                            hb_task.cancel()
                            with contextlib.suppress(asyncio.CancelledError, Exception):
                                await hb_task

                    backoff = self.reconnect_delay_min
                except Exception as exc:
                    logger.warning(
                        "coinbase_ws_reconnect",
                        feed=feed_name,
                        error=str(exc),
                        delay_seconds=backoff,
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(self.reconnect_delay_max, backoff * 2)
                    continue
