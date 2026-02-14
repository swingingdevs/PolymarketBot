from __future__ import annotations

import asyncio
import time
from typing import AsyncIterator

import aiohttp
import structlog

from utils.time import normalize_ts

logger = structlog.get_logger(__name__)


class SpotLivenessFallbackFeed:
    """Spot-based liveness fallback feed (not a canonical resolution feed)."""

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
                ts_raw = data.get("time", data.get("timestamp", time.time()))
                ts = normalize_ts(ts_raw if isinstance(ts_raw, (int, float)) else time.time())
                yield ts, price, {"source": "spot_liveness_fallback", "timestamp": ts}
            except Exception as exc:
                logger.warning("spot_liveness_fallback_poll_failed", error=str(exc))
            await asyncio.sleep(self.poll_interval)


# Backward-compatible alias for older imports.
ChainlinkDirectFeed = SpotLivenessFallbackFeed
