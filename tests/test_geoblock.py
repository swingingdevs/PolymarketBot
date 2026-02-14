from __future__ import annotations

import asyncio

from geo import check_geoblock, resolve_jurisdiction_key
from metrics import GEOBLOCK_BLOCKED


def test_resolve_jurisdiction_key_prefers_account_override() -> None:
    key = resolve_jurisdiction_key(
        country="US",
        region="NY",
        deployment_override="us-deployment",
        account_override="account-us-ny",
    )
    assert key == "account-us-ny"


def test_resolve_jurisdiction_key_falls_back_to_country_region() -> None:
    key = resolve_jurisdiction_key(country="US", region="NJ")
    assert key == "us-nj"


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
