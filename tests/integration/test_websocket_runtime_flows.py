from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date

from config import Settings
from execution.trader import Trader
from feeds.clob_ws import BookTop
from main import stream_clob_with_resubscribe, stream_prices_with_fallback, update_divergence_kill_switch


class FakeFeed:
    def __init__(self, events: list[tuple[float, float, dict[str, object]]], delay: float = 0.0) -> None:
        self._events = events
        self._delay = delay

    async def stream_prices(self):
        for event in self._events:
            if self._delay:
                await asyncio.sleep(self._delay)
            yield event


class FakeClob:
    def __init__(self) -> None:
        self.subscriptions: list[list[str]] = []

    async def stream_books(self, token_ids: list[str]):
        self.subscriptions.append(token_ids)
        yield BookTop(token_id=token_ids[0], best_bid=0.48, best_ask=0.5, ts=1.0)


class _CaptureClient:
    def __init__(self) -> None:
        self.time_in_force_values: list[str] = []

    def create_market_order(self, **kwargs):
        return kwargs

    def post_order(self, order, time_in_force="FOK"):
        self.time_in_force_values.append(time_in_force)
        return {"fills": [{"price": order.get("price", 0.0) or 0.0, "size": order["size"]}]}


@dataclass
class _Now:
    hour: int
    day: date

    def date(self) -> date:
        return self.day


class _FakeDatetime:
    @classmethod
    def now(cls, _tz):
        return _Now(hour=9, day=date(2026, 1, 1))


def test_rtds_clob_happy_path_fallback_then_recover() -> None:
    rtds = FakeFeed(
        [
            (1.0, 50000.0, {"source": "chainlink_rtds", "timestamp": 1.0}),
            (2.0, 50020.0, {"source": "chainlink_rtds", "timestamp": 2.0}),
        ],
        delay=0.05,
    )
    fallback = FakeFeed([(1.5, 50010.0, {"source": "chainlink_direct", "timestamp": 1.5})])

    async def _run() -> list[str]:
        stream = stream_prices_with_fallback(rtds, fallback, use_fallback_feed=True, price_staleness_threshold=0.01)
        out: list[str] = []
        async for _ts, _px, metadata in stream:
            out.append(str(metadata["source"]))
            if len(out) == 3:
                break
        return out

    sources = asyncio.run(_run())
    assert "chainlink_direct" in sources
    assert sources[-1] == "chainlink_rtds"



def test_divergence_kill_switch_activation_flow() -> None:
    settings = Settings(divergence_threshold_pct=0.5, divergence_sustain_seconds=1, rtds_spot_max_age_seconds=2.0)
    state: dict[str, object] = {"breach_started_at": None, "active": False, "last_divergence_pct": None}

    update_divergence_kill_switch(ts=10.0, metadata={"divergence_pct": 0.6}, kill_switch_state=state, settings=settings)
    assert state["active"] is False

    update_divergence_kill_switch(ts=11.1, metadata={"divergence_pct": 0.7}, kill_switch_state=state, settings=settings)
    assert state["active"] is True


def test_buy_fok_flow_posts_fok(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(Settings, "settings_profile", "paper", raising=False)
    monkeypatch.setattr("execution.trader.datetime", _FakeDatetime)
    settings = Settings(dry_run=False, risk_state_path=str(tmp_path / "risk_state.json"), quote_size_usd=10)
    trader = Trader(settings)
    trader.client = _CaptureClient()
    trader._live_auth_ready = True

    async def _run() -> bool:
        return await trader.buy_fok("token-a", ask=0.51, horizon="5")

    assert asyncio.run(_run()) is True
    assert trader.client.time_in_force_values == ["FOK"]


def test_market_roll_resubscribes_to_new_token_set() -> None:
    clob = FakeClob()
    state = {"token_ids": {"epoch-1"}}

    async def _run() -> tuple[str, str]:
        stream = stream_clob_with_resubscribe(clob, lambda: state["token_ids"])
        first = await stream.__anext__()
        state["token_ids"] = {"epoch-2"}
        second = await stream.__anext__()
        return first.token_id, second.token_id

    first, second = asyncio.run(_run())
    assert (first, second) == ("epoch-1", "epoch-2")
    assert clob.subscriptions == [["epoch-1"], ["epoch-2"]]
