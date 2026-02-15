from __future__ import annotations

import asyncio

from config import Settings
from geo import check_geoblock, resolve_jurisdiction_key
from main import run_startup_geoblock_preflight
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


def test_startup_geoblock_preflight_wires_jurisdiction_overrides(monkeypatch) -> None:
    settings = Settings(
        geoblock_abort=False,
        deployment_jurisdiction_override="deploy-us",
        jurisdiction_override="legacy-us",
        account_jurisdiction_override="acct-us-ny",
    )

    async def _mock_check_geoblock() -> tuple[bool, str, str]:
        return False, "US", "NY"

    captured: dict[str, str] = {}

    def _mock_resolve_jurisdiction_key(*, country: str, region: str, deployment_override: str, account_override: str) -> str:
        captured["country"] = country
        captured["region"] = region
        captured["deployment_override"] = deployment_override
        captured["account_override"] = account_override
        return "resolved-key"

    monkeypatch.setattr("main.check_geoblock", _mock_check_geoblock)
    monkeypatch.setattr("main.resolve_jurisdiction_key", _mock_resolve_jurisdiction_key)

    trading_allowed, country, region, jurisdiction_key = asyncio.run(run_startup_geoblock_preflight(settings))

    assert settings.deployment_jurisdiction_override == "deploy-us"
    assert settings.jurisdiction_override == "legacy-us"
    assert settings.account_jurisdiction_override == "acct-us-ny"
    assert trading_allowed is True
    assert country == "US"
    assert region == "NY"
    assert jurisdiction_key == "resolved-key"
    assert captured == {
        "country": "US",
        "region": "NY",
        "deployment_override": "deploy-us",
        "account_override": "acct-us-ny",
    }


def test_startup_geoblock_preflight_accepts_default_override_fields(monkeypatch) -> None:
    settings = Settings(geoblock_abort=False)

    async def _mock_check_geoblock() -> tuple[bool, str, str]:
        return False, "US", "NJ"

    monkeypatch.setattr("main.check_geoblock", _mock_check_geoblock)

    trading_allowed, country, region, _ = asyncio.run(run_startup_geoblock_preflight(settings))

    assert settings.deployment_jurisdiction_override == ""
    assert settings.jurisdiction_override == ""
    assert settings.account_jurisdiction_override == ""
    assert trading_allowed is True
    assert country == "US"
    assert region == "NJ"
