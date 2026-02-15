from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass, field

import aiohttp
import structlog

from markets.token_metadata_cache import TokenMetadata

logger = structlog.get_logger(__name__)


@dataclass(slots=True)
class UpDownMarket:
    slug: str
    start_epoch: int
    end_epoch: int
    up_token_id: str
    down_token_id: str
    horizon_minutes: int
    category: str = "event"
    token_metadata_by_id: dict[str, TokenMetadata] = field(default_factory=dict)


def build_slug(horizon_minutes: int, start_epoch: int) -> str:
    return f"btc-updown-{horizon_minutes}m-{start_epoch}"


class GammaCache:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self._cache: dict[str, tuple[UpDownMarket, int]] = {}
        self._hits = 0
        self._misses = 0
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    def __del__(self) -> None:
        session = self._session
        if session is None or session.closed:
            return

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        loop.create_task(session.close())

    @staticmethod
    def _validate_market_row(
        row: dict[str, object], slug: str, horizon_minutes: int, start_epoch: int
    ) -> tuple[int, int]:
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
            logger.warning(
                "gamma_market_start_mismatch",
                slug=slug,
                horizon_minutes=horizon_minutes,
                expected_start_epoch=start_epoch,
                observed_start_epoch=start,
            )

        now = int(time.time())
        if end <= now:
            raise ValueError("market expired")
        if bool(row.get("closed")) or bool(row.get("resolved")):
            raise ValueError("market not active")

        question = str(row.get("question", "")).lower()
        desc = str(row.get("description", "")).lower()
        if "btc" not in question + desc or "usd" not in question + desc:
            raise ValueError("underlying is not BTC/USD")

        return start, end


    @staticmethod
    def classify_market_category(row: dict[str, object]) -> str:
        sports_keywords = (
            "sport",
            "nba",
            "nfl",
            "mlb",
            "nhl",
            "soccer",
            "football",
            "baseball",
            "basketball",
            "tennis",
            "golf",
            "ufc",
            "mma",
            "f1",
            "formula 1",
        )

        searchable: list[str] = []
        for key in ("category", "question", "description", "eventType", "event_type"):
            value = row.get(key)
            if value is not None:
                searchable.append(str(value).lower())

        tags = row.get("tags")
        if isinstance(tags, list):
            for tag in tags:
                if isinstance(tag, dict):
                    searchable.append(str(tag.get("label") or tag.get("name") or "").lower())
                else:
                    searchable.append(str(tag).lower())

        combined = " ".join(searchable)
        if any(keyword in combined for keyword in sports_keywords):
            return "sports"
        return "event"

    @staticmethod
    def filter_markets_by_banned_categories(markets: list[UpDownMarket], banned_categories: set[str]) -> list[UpDownMarket]:
        if not banned_categories:
            return markets
        normalized = {c.strip().lower() for c in banned_categories if c.strip()}
        return [m for m in markets if m.category.strip().lower() not in normalized]

    @staticmethod
    def _extract_float(value: object) -> float | None:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        return parsed

    @classmethod
    def _extract_token_metadata(cls, row: dict[str, object], token_id: str) -> TokenMetadata:
        tick_size: float | None = None
        min_order_size: float | None = None
        fee_rate_bps: float | None = None

        def first_float(payload: dict[str, object], keys: tuple[str, ...]) -> float | None:
            for key in keys:
                if key not in payload:
                    continue
                parsed = cls._extract_float(payload.get(key))
                if parsed is not None:
                    return parsed
            return None

        global_tick = first_float(row, ("orderPriceMinTickSize", "minimum_tick_size", "minTickSize", "tickSize", "tick_size"))
        global_min_size = first_float(row, ("orderMinSize", "minimum_order_size", "minOrderSize", "min_order_size"))
        global_fee = first_float(row, ("fee_rate_bps", "takerFeeBps", "taker_fee_bps", "baseFeeRateBps"))

        token_containers = []
        for key in ("tokens", "outcomeTokens", "outcomes"):
            values = row.get(key)
            if isinstance(values, list):
                token_containers.extend(values)

        for candidate in token_containers:
            if not isinstance(candidate, dict):
                continue
            candidate_token_id = str(
                candidate.get("clobTokenId")
                or candidate.get("clob_token_id")
                or candidate.get("tokenId")
                or candidate.get("token_id")
                or ""
            )
            if candidate_token_id != token_id:
                continue

            token_tick = first_float(candidate, ("orderPriceMinTickSize", "minimum_tick_size", "minTickSize", "tickSize", "tick_size"))
            token_min_size = first_float(candidate, ("orderMinSize", "minimum_order_size", "minOrderSize", "min_order_size"))
            token_fee = first_float(candidate, ("fee_rate_bps", "takerFeeBps", "taker_fee_bps", "baseFeeRateBps"))
            tick_size = token_tick if token_tick is not None else tick_size
            min_order_size = token_min_size if token_min_size is not None else min_order_size
            fee_rate_bps = token_fee if token_fee is not None else fee_rate_bps

        if tick_size is None:
            tick_size = global_tick
        if min_order_size is None:
            min_order_size = global_min_size
        if fee_rate_bps is None:
            fee_rate_bps = global_fee

        return TokenMetadata(tick_size=tick_size, min_order_size=min_order_size, fee_rate_bps=fee_rate_bps)

    @staticmethod
    def _normalize_string_list_field(field_name: str, value: object) -> list[str]:
        def value_preview(raw: object) -> str:
            preview = repr(raw)
            if len(preview) > 120:
                preview = f"{preview[:117]}..."
            return preview

        if isinstance(value, list):
            return [str(item) for item in value]

        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Invalid {field_name}: failed to parse JSON list from "
                    f"type={type(value).__name__} value={value_preview(value)}"
                ) from exc

            if isinstance(parsed, list):
                return [str(item) for item in parsed]

            raise ValueError(
                f"Invalid {field_name}: expected JSON list but got "
                f"type={type(parsed).__name__} value={value_preview(parsed)}"
            )

        raise ValueError(
            f"Invalid {field_name}: expected list or JSON list string but got "
            f"type={type(value).__name__} value={value_preview(value)}"
        )

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
            session = await self._get_session()
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
        observed_start_epoch, observed_end_epoch = self._validate_market_row(row, slug, horizon_minutes, start_epoch)
        expected_end_epoch = start_epoch + horizon_minutes * 60
        if observed_end_epoch != expected_end_epoch:
            logger.warning(
                "gamma_market_end_drift",
                slug=slug,
                horizon_minutes=horizon_minutes,
                expected_start_epoch=start_epoch,
                observed_start_epoch=observed_start_epoch,
                expected_end_epoch=expected_end_epoch,
                observed_end_epoch=observed_end_epoch,
            )

        outcomes = self._normalize_string_list_field("outcomes", row.get("outcomes", []))
        clob_ids = self._normalize_string_list_field("clobTokenIds", row.get("clobTokenIds", []))
        mapping = {outcomes[i].lower(): clob_ids[i] for i in range(min(len(outcomes), len(clob_ids)))}
        up = mapping.get("up")
        down = mapping.get("down")
        if not up or not down:
            raise ValueError(f"Missing up/down outcomes for slug={slug}")

        token_metadata = {
            up: self._extract_token_metadata(row, up),
            down: self._extract_token_metadata(row, down),
        }

        market = UpDownMarket(
            slug=slug,
            start_epoch=start_epoch,
            end_epoch=expected_end_epoch,
            up_token_id=up,
            down_token_id=down,
            horizon_minutes=horizon_minutes,
            category=self.classify_market_category(row),
            token_metadata_by_id=token_metadata,
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
