from __future__ import annotations

from markets.gamma_cache import GammaCache, UpDownMarket


def test_classify_market_as_sports_from_tags() -> None:
    row = {
        "question": "Will Team A win?",
        "tags": [{"name": "NBA"}],
        "category": "Entertainment",
    }
    assert GammaCache.classify_market_category(row) == "sports"


def test_classify_market_as_event_for_crypto() -> None:
    row = {
        "question": "Will BTC/USD close above 100k?",
        "category": "Crypto",
        "tags": ["bitcoin"],
    }
    assert GammaCache.classify_market_category(row) == "event"


def test_filter_markets_by_banned_categories_excludes_sports() -> None:
    sports_market = UpDownMarket("sports", 0, 1, "u1", "d1", 5, category="sports")
    event_market = UpDownMarket("event", 0, 1, "u2", "d2", 5, category="event")

    allowed = GammaCache.filter_markets_by_banned_categories([sports_market, event_market], {"sports"})

    assert [m.slug for m in allowed] == ["event"]
