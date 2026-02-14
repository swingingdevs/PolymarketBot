from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass

import aiohttp
import structlog

logger = structlog.get_logger(__name__)


@dataclass(slots=True)
class UpDownMarket:
    slug: str
    start_epoch: int
    end_epoch: int
    up_token_id: str
    down_token_id: str
    horizon_minutes: int


def build_slug(horizon_minutes: int, start_epoch: int) -> str:
    return f"btc-updown-{horizon_minutes}m-{start_epoch}"


class GammaCache:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self._cache: dict[str, tuple[UpDownMarket, int]] = {}
        self._hits = 0
        self._misses = 0

    @staticmethod
    def _validate_market_row(row: dict[str, object], slug: str, horizon_minutes: int, start_epoch: int) -> None:
        pattern = r"^btc-updown-(5|15)m-(\d+)$"
        row_slug = str(row.get("slug", ""))
        m = re.match(pattern, row_slug)
        if not m or row_slug != slug:
            raise ValueError(f"Invalid market slug. expected={slug} got={row_slug}")

        if start_epoch % (horizon_minutes * 60) != 0:
            raise ValueError(f"start_epoch not aligned to {horizon_minutes}m boundary: {start_epoch}")

        start_iso = row.get("startDate") or row.get("startTime")
        end_iso = row.get("endDate") or row.get("endTime")
        if not start_iso or not end_iso:
            raise ValueError("Missing start/end time from Gamma")

        from datetime import datetime

        start = int(datetime.fromisoformat(str(start_iso).replace("Z", "+00:00")).timestamp())
        end = int(datetime.fromisoformat(str(end_iso).replace("Z", "+00:00")).timestamp())

        if start != start_epoch:
            raise ValueError(f"start time mismatch. expected={start_epoch} got={start}")
        expected_end = start_epoch + horizon_minutes * 60
        if end != expected_end:
            raise ValueError(f"invalid market duration. expected_end={expected_end} got={end}")

        now = int(time.time())
        if end <= now:
            raise ValueError("market expired")
        if bool(row.get("closed")) or bool(row.get("resolved")):
            raise ValueError("market not active")

        question = str(row.get("question", "")).lower()
        desc = str(row.get("description", "")).lower()
        if "btc" not in question + desc or "usd" not in question + desc:
            raise ValueError("underlying is not BTC/USD")

    async def get_market(self, horizon_minutes: int, start_epoch: int) -> UpDownMarket:
        slug = build_slug(horizon_minutes, start_epoch)
        now = int(time.time())
        cached = self._cache.get(slug)
        if cached and cached[1] > now:
            self._hits += 1
            total = self._hits + self._misses
            logger.info("gamma_cache_hit", slug=slug, hit_rate=(self._hits / max(1, total)))
            return cached[0]

        self._misses += 1
        started = time.perf_counter()
        url = f"{self.base_url}/markets"
        params = {"slug": slug}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=10) as response:
                    response.raise_for_status()
                    rows = await response.json()
        except asyncio.TimeoutError as exc:
            raise RuntimeError("Gamma API timeout") from exc
        except aiohttp.ClientResponseError as exc:
            if exc.status == 429:
                raise RuntimeError("Gamma API rate limited") from exc
            raise

        if not rows:
            raise ValueError(f"No market found for slug={slug}")
        if len(rows) > 1:
            raise ValueError(f"Multiple markets returned for slug={slug}")

        row = rows[0]
        self._validate_market_row(row, slug, horizon_minutes, start_epoch)

        outcomes = row.get("outcomes", [])
        clob_ids = row.get("clobTokenIds", [])
        mapping = {str(outcomes[i]).lower(): str(clob_ids[i]) for i in range(min(len(outcomes), len(clob_ids)))}
        up = mapping.get("up")
        down = mapping.get("down")
        if not up or not down:
            raise ValueError(f"Missing up/down outcomes for slug={slug}")

        market = UpDownMarket(
            slug=slug,
            start_epoch=start_epoch,
            end_epoch=start_epoch + horizon_minutes * 60,
            up_token_id=up,
            down_token_id=down,
            horizon_minutes=horizon_minutes,
        )
        self._cache[slug] = (market, market.end_epoch)

        duration_ms = (time.perf_counter() - started) * 1000
        total = self._hits + self._misses
        logger.info(
            "gamma_market_discovered",
            slug=market.slug,
            start_epoch=market.start_epoch,
            end_epoch=market.end_epoch,
            horizon_minutes=market.horizon_minutes,
            up_token_id=market.up_token_id,
            down_token_id=market.down_token_id,
            time_to_find_market_ms=duration_ms,
            cache_hit_rate=(self._hits / max(1, total)),
            cache_misses=self._misses,
        )
        return market

    async def warm(self, horizon_minutes: int, start_epochs: list[int]) -> None:
        await asyncio.gather(*[self.get_market(horizon_minutes, s) for s in start_epochs])
