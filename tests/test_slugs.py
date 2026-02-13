from markets.gamma_cache import build_slug


def test_slug_patterns() -> None:
    assert build_slug(5, 1710000000) == "btc-updown-5m-1710000000"
    assert build_slug(15, 1710000000) == "btc-updown-15m-1710000000"
