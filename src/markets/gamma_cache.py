from __future__ import annotations

import asyncio
from dataclasses import dataclass



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
        self._cache: dict[str, UpDownMarket] = {}

    async def get_market(self, horizon_minutes: int, start_epoch: int) -> UpDownMarket:
        slug = build_slug(horizon_minutes, start_epoch)
        if slug in self._cache:
            return self._cache[slug]

        url = f"{self.base_url}/markets"
        params = {"slug": slug}
        import aiohttp

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
                response.raise_for_status()
                rows = await response.json()

        if not rows:
            raise ValueError(f"No market found for slug={slug}")

        row = rows[0]
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
        self._cache[slug] = market
        return market

    async def warm(self, horizon_minutes: int, start_epochs: list[int]) -> None:
        await asyncio.gather(*[self.get_market(horizon_minutes, s) for s in start_epochs])
