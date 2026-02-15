from __future__ import annotations

import asyncio
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "smoke_runtime.py"
SPEC = importlib.util.spec_from_file_location("smoke_runtime", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
smoke_runtime = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = smoke_runtime
SPEC.loader.exec_module(smoke_runtime)


class _ResolveFailed(Exception):
    pass


def test_main_closes_gamma_when_market_resolution_fails(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    gamma_close = AsyncMock()
    gamma = SimpleNamespace(close=gamma_close)

    monkeypatch.setattr(
        smoke_runtime,
        "Settings",
        lambda settings_profile, dry_run: SimpleNamespace(
            gamma_api_url="https://gamma.invalid",
            rtds_ws_url="wss://example.invalid/rtds",
            rtds_topic="crypto_prices_chainlink",
            symbol="btc/usd",
            settings_profile="paper",
            dry_run=True,
        ),
    )
    monkeypatch.setattr(smoke_runtime, "GammaCache", lambda api_url: gamma)

    async def _raise_on_resolve(_: object) -> list[object]:
        raise _ResolveFailed("boom")

    monkeypatch.setattr(smoke_runtime, "resolve_markets", _raise_on_resolve)

    with pytest.raises(_ResolveFailed):
        asyncio.run(smoke_runtime.main())

    captured = capsys.readouterr()
    assert "[SMOKE] RTDS_CONFIG" in captured.out
    assert "RTDS_WS_URL=wss://example.invalid/rtds" in captured.out
    assert "RTDS_TOPIC=crypto_prices_chainlink" in captured.out
    assert "SYMBOL=btc/usd" in captured.out
    gamma_close.assert_awaited_once_with()
