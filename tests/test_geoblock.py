from __future__ import annotations

import asyncio

from config import Settings
from geo import check_geoblock
from main import run_startup_geoblock_preflight
from metrics import GEOBLOCK_BLOCKED


def test_blocked_country_disables_trading_startup(monkeypatch) -> None:
    async def _blocked():
        return True, "US", "us-east-1"

    monkeypatch.setattr("main.check_geoblock", _blocked)
    settings = Settings(geoblock_abort=False)

    trading_allowed = asyncio.run(run_startup_geoblock_preflight(settings))
    assert trading_allowed is False


def test_unblocked_country_allows_trading_startup(monkeypatch) -> None:
    async def _clear():
        return False, "IE", "eu-west-1"

    monkeypatch.setattr("main.check_geoblock", _clear)
    settings = Settings(geoblock_abort=True)

    trading_allowed = asyncio.run(run_startup_geoblock_preflight(settings))
    assert trading_allowed is True


def test_geoblock_metrics_update_from_response(monkeypatch) -> None:
    GEOBLOCK_BLOCKED.clear()

    class _Response:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def raise_for_status(self) -> None:
            return None

        async def json(self):
            return {"blocked": True, "country": "US", "region": "us-east-1"}

    class _Session:
        def __init__(self, *args, **kwargs) -> None:
            return None

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, _url: str):
            return _Response()

    monkeypatch.setattr("geo.aiohttp.ClientSession", _Session)

    blocked, country, region = asyncio.run(check_geoblock())

    assert blocked is True
    assert country == "US"
    assert region == "us-east-1"

    metric_value = GEOBLOCK_BLOCKED.labels(country="US", region="us-east-1")._value.get()
    assert metric_value == 1
