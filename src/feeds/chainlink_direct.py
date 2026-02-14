from __future__ import annotations

import asyncio
import time
from typing import AsyncIterator

import aiohttp
import structlog

logger = structlog.get_logger(__name__)


class ChainlinkDirectFeed:
    """Fallback feed that polls a direct Chainlink stream endpoint."""

    def __init__(self, api_url: str, poll_interval: float = 1.0) -> None:
        self.api_url = api_url
        self.poll_interval = poll_interval

    async def stream_prices(self) -> AsyncIterator[tuple[float, float, dict[str, object]]]:
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.api_url, timeout=5) as response:
                        response.raise_for_status()
                        data = await response.json()
                price = float(data.get("price"))
                ts = float(data.get("timestamp", time.time()))
                yield ts, price, {"source": "chainlink_direct", "timestamp": ts}
            except Exception as exc:
                logger.warning("chainlink_direct_poll_failed", error=str(exc))
            await asyncio.sleep(self.poll_interval)
