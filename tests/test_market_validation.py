from __future__ import annotations

import asyncio
import importlib.util
import sys
import time
from pathlib import Path

import pytest

from markets.gamma_cache import GammaCache, UpDownMarket, build_slug

_SMOKE_RUNTIME_SPEC = importlib.util.spec_from_file_location(
    "smoke_runtime", Path(__file__).resolve().parents[1] / "scripts" / "smoke_runtime.py"
)
assert _SMOKE_RUNTIME_SPEC is not None and _SMOKE_RUNTIME_SPEC.loader is not None
_smoke_runtime = importlib.util.module_from_spec(_SMOKE_RUNTIME_SPEC)
sys.modules["smoke_runtime"] = _smoke_runtime
_SMOKE_RUNTIME_SPEC.loader.exec_module(_smoke_runtime)
resolve_markets = _smoke_runtime.resolve_markets


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


def test_get_market_reuses_single_client_session(monkeypatch: pytest.MonkeyPatch) -> None:
    now = int(time.time())
    start_1 = ((now // 300) + 2) * 300
    start_2 = start_1 + 300

    rows_by_slug = {
        build_slug(5, start_1): [_valid_row(start_1, 5)],
        build_slug(5, start_2): [_valid_row(start_2, 5)],
    }

    created_sessions: list[FakeSession] = []

    class FakeResponse:
        def __init__(self, rows: list[dict[str, object]]) -> None:
            self._rows = rows

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def raise_for_status(self) -> None:
            return

        async def json(self) -> list[dict[str, object]]:
            return self._rows

    class FakeSession:
        def __init__(self) -> None:
            self.closed = False
            self.get_calls = 0

        def get(self, _url: str, *, params: dict[str, str], timeout: int):
            del timeout
            self.get_calls += 1
            slug = params["slug"]
            return FakeResponse(rows_by_slug[slug])

        async def close(self) -> None:
            self.closed = True

    def make_session() -> FakeSession:
        session = FakeSession()
        created_sessions.append(session)
        return session

    monkeypatch.setattr("markets.gamma_cache.aiohttp.ClientSession", make_session)

    async def _run() -> None:
        cache = GammaCache("https://gamma-api.polymarket.com")
        try:
            await cache.get_market(5, start_1)
            await cache.get_market(5, start_2)
        finally:
            await cache.close()

    asyncio.run(_run())

    assert len(created_sessions) == 1
    assert created_sessions[0].get_calls == 2
    assert created_sessions[0].closed is True


def test_get_market_allows_start_time_drift_when_slug_matches(monkeypatch: pytest.MonkeyPatch) -> None:
    now = int(time.time())
    start = ((now // 300) + 3) * 300
    row = _valid_row(start, 5)
    row["startDate"] = row["endDate"]

    class FakeResponse:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def raise_for_status(self) -> None:
            return

        async def json(self) -> list[dict[str, object]]:
            return [row]

    class FakeSession:
        def __init__(self) -> None:
            self.closed = False

        def get(self, _url: str, *, params: dict[str, str], timeout: int):
            del params, timeout
            return FakeResponse()

        async def close(self) -> None:
            self.closed = True

    monkeypatch.setattr("markets.gamma_cache.aiohttp.ClientSession", FakeSession)

    async def _run() -> UpDownMarket:
        cache = GammaCache("https://gamma-api.polymarket.com")
        try:
            return await cache.get_market(5, start)
        finally:
            await cache.close()

    market = asyncio.run(_run())
    assert isinstance(market, UpDownMarket)
    assert market.slug == build_slug(5, start)
    assert market.end_epoch == start + 300


def test_resolve_markets_falls_back_per_horizon(monkeypatch: pytest.MonkeyPatch) -> None:
    now = 1_710_000_123
    start_5m_floor = now - (now % 300)
    start_15m_floor = now - (now % 900)

    market_5m = UpDownMarket(build_slug(5, start_5m_floor - 300), start_5m_floor - 300, start_5m_floor, "u5", "d5", 5)
    market_15m = UpDownMarket(
        build_slug(15, start_15m_floor + 900),
        start_15m_floor + 900,
        start_15m_floor + 1_800,
        "u15",
        "d15",
        15,
    )

    class FakeGamma:
        def __init__(self) -> None:
            self.calls: list[tuple[int, int]] = []

        async def get_market(self, horizon_minutes: int, start_epoch: int) -> UpDownMarket:
            self.calls.append((horizon_minutes, start_epoch))
            if horizon_minutes == 5 and start_epoch == start_5m_floor - 300:
                return market_5m
            if horizon_minutes == 15 and start_epoch == start_15m_floor + 900:
                return market_15m
            raise ValueError(f"no market for {horizon_minutes}m {start_epoch}")

    monkeypatch.setattr("smoke_runtime.time.time", lambda: now)

    gamma = FakeGamma()
    markets = asyncio.run(resolve_markets(gamma))

    assert markets == [market_5m, market_15m]
    assert gamma.calls == [
        (5, start_5m_floor),
        (5, start_5m_floor - 300),
        (15, start_15m_floor),
        (15, start_15m_floor - 900),
        (15, start_15m_floor + 900),
    ]


def test_resolve_markets_raises_single_error_after_all_candidates(monkeypatch: pytest.MonkeyPatch) -> None:
    now = 1_710_000_123
    start_5m_floor = now - (now % 300)

    class AlwaysFailGamma:
        async def get_market(self, horizon_minutes: int, start_epoch: int) -> UpDownMarket:
            raise RuntimeError(f"boom-{horizon_minutes}-{start_epoch}")

    monkeypatch.setattr("smoke_runtime.time.time", lambda: now)

    with pytest.raises(RuntimeError, match=r"Failed to resolve 5m market") as excinfo:
        asyncio.run(resolve_markets(AlwaysFailGamma()))

    assert f"attempted_epochs=[{start_5m_floor}, {start_5m_floor - 300}, {start_5m_floor + 300}]" in str(excinfo.value)
    assert f"last_error=boom-5-{start_5m_floor + 300}" in str(excinfo.value)
