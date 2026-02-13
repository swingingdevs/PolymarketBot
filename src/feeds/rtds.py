from __future__ import annotations

import asyncio
import json
from typing import AsyncIterator

import websockets


class RTDSFeed:
    def __init__(self, ws_url: str, symbol: str = "BTC/USD") -> None:
        self.ws_url = ws_url
        self.symbol = symbol

    async def stream_prices(self) -> AsyncIterator[tuple[float, float]]:
        """Yield (timestamp, price) from RTDS chainlink crypto feed."""
        while True:
            try:
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20) as ws:
                    sub = {
                        "type": "subscribe",
                        "payload": {"market": "chainlink", "symbols": [self.symbol]},
                    }
                    await ws.send(json.dumps(sub))
                    async for message in ws:
                        data = json.loads(message)
                        payload = data.get("payload", {})
                        if payload.get("symbol") != self.symbol:
                            continue
                        px = payload.get("price")
                        ts = payload.get("timestamp")
                        if px is None or ts is None:
                            continue
                        yield float(ts), float(px)
            except Exception:
                await asyncio.sleep(1)
