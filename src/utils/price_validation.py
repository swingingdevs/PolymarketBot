from __future__ import annotations

import time

import structlog

logger = structlog.get_logger(__name__)


def validate_price_source(price_data: dict[str, object]) -> bool:
    """Return True only when the feed/source metadata indicates Chainlink."""
    source = str(price_data.get("source") or price_data.get("feed") or "").lower()
    market = str(price_data.get("market") or "").lower()
    topic = str(price_data.get("topic") or "").lower()
    return "chainlink" in source or "chainlink" in market or "chainlink" in topic


def compare_feeds(chainlink_price: float, binance_price: float) -> float:
    """Log and return absolute price delta; warn when gap is meaningful."""
    delta = abs(chainlink_price - binance_price)
    if delta > 20:
        logger.warning(
            "feed_divergence_detected",
            chainlink_price=chainlink_price,
            binance_price=binance_price,
            abs_diff=delta,
        )
    else:
        logger.info(
            "feed_comparison",
            chainlink_price=chainlink_price,
            binance_price=binance_price,
            abs_diff=delta,
        )
    return delta


def is_price_stale(timestamp: float, stale_after_seconds: float = 2.0) -> bool:
    """True when timestamp is older than stale_after_seconds from now."""
    return (time.time() - float(timestamp)) > stale_after_seconds
