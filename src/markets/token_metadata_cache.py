from __future__ import annotations

import time
from dataclasses import dataclass

import structlog

logger = structlog.get_logger(__name__)


@dataclass(slots=True)
class TokenMetadata:
    tick_size: float | None = None
    min_order_size: float | None = None
    fee_rate_bps: float | None = None


@dataclass(slots=True)
class _CacheEntry:
    metadata: TokenMetadata
    updated_at: float


class TokenMetadataCache:
    def __init__(self, ttl_seconds: float = 300.0) -> None:
        self.ttl_seconds = ttl_seconds
        self._cache: dict[str, _CacheEntry] = {}

    def put(self, token_id: str, metadata: TokenMetadata) -> None:
        self._cache[token_id] = _CacheEntry(metadata=metadata, updated_at=time.time())

    def put_many(self, values: dict[str, TokenMetadata]) -> None:
        now = time.time()
        for token_id, metadata in values.items():
            self._cache[token_id] = _CacheEntry(
                metadata=TokenMetadata(
                    tick_size=metadata.tick_size,
                    min_order_size=metadata.min_order_size,
                    fee_rate_bps=metadata.fee_rate_bps,
                ),
                updated_at=now,
            )

    def _entry(self, token_id: str) -> tuple[_CacheEntry | None, bool]:
        entry = self._cache.get(token_id)
        if entry is None:
            return None, False
        age_seconds = max(0.0, time.time() - entry.updated_at)
        return entry, age_seconds <= self.ttl_seconds

    def get(self, token_id: str, *, allow_stale: bool = True) -> TokenMetadata | None:
        entry, is_fresh = self._entry(token_id)
        if entry is None:
            return None
        if is_fresh or allow_stale:
            return entry.metadata
        return None

    def get_tick_size(self, token_id: str, fallback_tick_size: float) -> float:
        metadata = self.get(token_id, allow_stale=True)
        if metadata and metadata.tick_size is not None and metadata.tick_size > 0:
            return metadata.tick_size
        logger.debug("token_tick_size_fallback", token_id=token_id, fallback_tick_size=fallback_tick_size)
        return fallback_tick_size

    def get_min_order_size(self, token_id: str) -> float | None:
        metadata = self.get(token_id, allow_stale=True)
        if metadata and metadata.min_order_size is not None and metadata.min_order_size > 0:
            return metadata.min_order_size
        logger.debug("token_min_order_size_missing", token_id=token_id)
        return None

    def get_fee_rate_bps(self, token_id: str, fallback_fee_bps: float) -> float:
        metadata = self.get(token_id, allow_stale=True)
        if metadata and metadata.fee_rate_bps is not None and metadata.fee_rate_bps >= 0:
            return metadata.fee_rate_bps
        logger.debug("token_fee_rate_fallback", token_id=token_id, fallback_fee_bps=fallback_fee_bps)
        return fallback_fee_bps


    
