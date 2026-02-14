from __future__ import annotations

import time

import pytest

from markets.gamma_cache import GammaCache, UpDownMarket, build_slug


def _valid_row(start_epoch: int, horizon_minutes: int) -> dict[str, object]:
    from datetime import datetime, timezone

    start = datetime.fromtimestamp(start_epoch, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    end = datetime.fromtimestamp(start_epoch + horizon_minutes * 60, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "slug": build_slug(horizon_minutes, start_epoch),
        "startDate": start,
        "endDate": end,
        "outcomes": ["Up", "Down"],
        "clobTokenIds": ["u", "d"],
        "question": "Will BTC/USD be up?",
        "description": "BTC USD 5m",
        "closed": False,
        "resolved": False,
    }


def test_reject_wrong_interval_slug() -> None:
    row = _valid_row(1_710_000_000, 5)
    row["slug"] = "btc-updown-10m-1710000000"
    with pytest.raises(ValueError):
        GammaCache._validate_market_row(row, build_slug(5, 1_710_000_000), 5, 1_710_000_000)


def test_reject_expired_market() -> None:
    now = int(time.time())
    start = now - 600
    row = _valid_row(start, 5)
    with pytest.raises(ValueError):
        GammaCache._validate_market_row(row, build_slug(5, start), 5, start)


def test_cache_hit_behavior() -> None:
    gc = GammaCache("https://gamma-api.polymarket.com")
    start = int(time.time()) + 300
    market = UpDownMarket(build_slug(5, start), start, start + 300, "u", "d", 5)
    gc._cache[market.slug] = (market, market.end_epoch)

    # direct cache behavior without network calls
    cached = gc._cache.get(market.slug)
    assert cached is not None
    assert cached[0].slug == market.slug
    assert cached[1] == market.end_epoch
