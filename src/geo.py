from __future__ import annotations

import aiohttp
import structlog

from metrics import GEOBLOCK_BLOCKED

logger = structlog.get_logger(__name__)

GEOBLOCK_URL = "https://polymarket.com/api/geoblock"


async def check_geoblock(timeout_seconds: float = 5.0) -> tuple[bool, str, str]:
    """Return geoblock status for the current deployment IP.

    Returns a tuple: (blocked, country, region).

    Recommended deployment region for lowest latency (when compliant): eu-west-1.
    Polymarket's primary servers run in eu-west-2, but eu-west-1 is commonly used
    as the closest non-georestricted region.
    """
    blocked = False
    country = "unknown"
    region = "unknown"

    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(GEOBLOCK_URL) as response:
                response.raise_for_status()
                payload = await response.json()
                blocked = bool(payload.get("blocked", False))
                country = str(payload.get("country") or country)
                region = str(payload.get("region") or region)
    except Exception as exc:
        logger.warning("geoblock_preflight_failed", error=str(exc), endpoint=GEOBLOCK_URL)

    GEOBLOCK_BLOCKED.labels(country=country, region=region).set(1 if blocked else 0)
    return blocked, country, region
