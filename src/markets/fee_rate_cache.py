from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

import aiohttp
import structlog

logger = structlog.get_logger(__name__)


@dataclass(slots=True)
class _FeeRateEntry:
    fee_rate_bps: float | None
    updated_at: float


class FeeRateCache:
    def __init__(self, base_url: str, ttl_seconds: float = 60.0, request_timeout_seconds: float = 5.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.ttl_seconds = ttl_seconds
        self.request_timeout_seconds = request_timeout_seconds
        self._cache: dict[str, _FeeRateEntry] = {}
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

    def _is_fresh(self, token_id: str) -> bool:
        entry = self._cache.get(token_id)
        if entry is None:
            return False
        return (time.time() - entry.updated_at) <= self.ttl_seconds

    @staticmethod
    def _parse_fee_rate_bps(payload: object) -> float | None:
        if not isinstance(payload, dict):
            return None
        raw = payload.get("fee_rate_bps")
        if raw is None:
            raw = payload.get("feeRateBps")
        if raw is None:
            raw = payload.get("feeRate")
        try:
            parsed = float(raw)
        except (TypeError, ValueError):
            return None
        return parsed if parsed >= 0 else None

    async def _fetch_fee_rate_bps(self, token_id: str) -> float | None:
        session = await self._get_session()
        url = f"{self.base_url}/fee-rate"
        try:
            async with session.get(
                url,
                params={"token_id": token_id},
                timeout=self.request_timeout_seconds,
            ) as response:
                response.raise_for_status()
                payload = await response.json()
        except Exception as exc:
            logger.warning("fee_rate_fetch_failed", token_id=token_id, error=str(exc))
            return None

        fee_rate_bps = self._parse_fee_rate_bps(payload)
        if fee_rate_bps is None:
            logger.warning("fee_rate_parse_failed", token_id=token_id, payload=payload)
        return fee_rate_bps

    async def warm(self, token_ids: set[str] | list[str]) -> None:
        unique_tokens = sorted({token_id for token_id in token_ids if token_id})
        cold_tokens = [token_id for token_id in unique_tokens if not self._is_fresh(token_id)]
        if not cold_tokens:
            return

        now = time.time()
        fetched = await asyncio.gather(*[self._fetch_fee_rate_bps(token_id) for token_id in cold_tokens])
        for token_id, fee_rate_bps in zip(cold_tokens, fetched, strict=True):
            self._cache[token_id] = _FeeRateEntry(fee_rate_bps=fee_rate_bps, updated_at=now)

    def get_fee_rate_bps(self, token_id: str) -> float | None:
        if not self._is_fresh(token_id):
            return None
        entry = self._cache.get(token_id)
        if entry is None:
            return None
        return entry.fee_rate_bps
