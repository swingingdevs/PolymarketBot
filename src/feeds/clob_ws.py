from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import AsyncIterator

import websockets


@dataclass(slots=True)
class BookTop:
    token_id: str
    best_bid: float | None
    best_ask: float | None
    ts: float


class CLOBWebSocket:
    def __init__(self, ws_url: str) -> None:
        self.ws_url = ws_url

    async def stream_books(self, token_ids: list[str]) -> AsyncIterator[BookTop]:
        while True:
            try:
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20) as ws:
                    sub = {"type": "market", "assets_ids": token_ids, "channel": "book"}
                    await ws.send(json.dumps(sub))
                    async for raw in ws:
                        data = json.loads(raw)
                        if data.get("event_type") != "book":
                            continue
                        token_id = str(data.get("asset_id"))
                        bids = data.get("bids", [])
                        asks = data.get("asks", [])
                        bid = float(bids[0][0]) if bids else None
                        ask = float(asks[0][0]) if asks else None
                        ts = float(data.get("timestamp", 0.0))
                        yield BookTop(token_id=token_id, best_bid=bid, best_ask=ask, ts=ts)
            except Exception:
                await asyncio.sleep(1)
