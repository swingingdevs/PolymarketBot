from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import structlog
from py_clob_client.clob_types import OrderArgs

from metrics import BOT_FEE_FETCH_FAILURES_TOTAL, BOT_FEE_RATE_BPS

logger = structlog.get_logger(__name__)

_FEE_RATE_CACHE: dict[str, tuple[float, float]] = {}
_FEE_RATE_TTL_SECONDS = 60.0
_FEE_RATE_BASE_URL = "https://clob.polymarket.com"


def configure_fee_rate_fetcher(*, base_url: str, ttl_seconds: float) -> None:
    global _FEE_RATE_BASE_URL, _FEE_RATE_TTL_SECONDS
    _FEE_RATE_BASE_URL = base_url.rstrip("/")
    _FEE_RATE_TTL_SECONDS = max(1.0, float(ttl_seconds))


def _parse_fee_rate_bps(payload: object) -> float:
    if not isinstance(payload, dict):
        raise ValueError("invalid_fee_rate_payload")
    raw = payload.get("feeRateBps")
    if raw is None:
        raw = payload.get("fee_rate_bps")
    fee_rate_bps = float(raw)
    if fee_rate_bps < 0:
        raise ValueError("fee_rate_bps_must_be_non_negative")
    return fee_rate_bps


def fetch_fee_rate(token_id: str) -> float:
    now = time.time()
    cached = _FEE_RATE_CACHE.get(token_id)
    if cached is not None:
        cached_fee_bps, cached_ts = cached
        if (now - cached_ts) <= _FEE_RATE_TTL_SECONDS:
            return cached_fee_bps

    query = urlencode({"token_id": token_id})
    url = f"{_FEE_RATE_BASE_URL}/fee-rate?{query}"
    try:
        with urlopen(url, timeout=5.0) as response:  # noqa: S310
            payload = json.loads(response.read().decode("utf-8"))
    except (OSError, URLError, TimeoutError, ValueError) as exc:
        BOT_FEE_FETCH_FAILURES_TOTAL.inc()
        raise RuntimeError("fee_rate_fetch_failed") from exc

    fee_rate_bps = _parse_fee_rate_bps(payload)
    _FEE_RATE_CACHE[token_id] = (fee_rate_bps, now)
    BOT_FEE_RATE_BPS.labels(token_id=token_id).set(fee_rate_bps)
    return fee_rate_bps


@dataclass(slots=True)
class OrderBuilder:
    clob_client: Any
    enable_fee_rate: bool = True
    default_fee_rate_bps: float = 0.0

    def resolve_fee_rate_bps(self, token_id: str) -> tuple[float, bool]:
        if not self.enable_fee_rate:
            return self.default_fee_rate_bps, False
        try:
            return fetch_fee_rate(token_id), False
        except Exception as exc:
            logger.warning("fee_rate_fetch_failed", token_id=token_id, error=str(exc))
            return self.default_fee_rate_bps, True

    def build_signed_order(
        self,
        *,
        token_id: str,
        price: float,
        size: float,
        side: str = "BUY",
        time_in_force: str | None = None,
        post_only: bool = False,
        fok: bool = True,
    ) -> tuple[Any, bool, float]:
        if time_in_force is None:
            time_in_force = "FOK" if fok else "GTC"
        normalized_tif = str(time_in_force).strip().upper()
        if not normalized_tif:
            raise ValueError("time_in_force_required")
        if fok and normalized_tif != "FOK":
            raise ValueError("fok_conflicts_with_time_in_force")

        fee_rate_bps, used_fallback = self.resolve_fee_rate_bps(token_id)
        base_payload = {
            "price": price,
            "size": size,
            "side": side,
            "token_id": token_id,
            "fee_rate_bps": int(round(fee_rate_bps)),
        }
        del time_in_force

        if hasattr(self.clob_client, "create_order"):
            order = self.clob_client.create_order(OrderArgs(**payload))
        else:
            raise RuntimeError("unsupported_order_submission_api")

        if hasattr(self.clob_client, "sign_order"):
            order = self.clob_client.sign_order(order)

        return order, used_fallback, fee_rate_bps
