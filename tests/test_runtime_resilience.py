from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass

import pytest

from config import Settings
from execution.trader import Trader
from feeds.clob_ws import BookTop, CLOBWebSocket
from main import stream_clob_with_resubscribe, stream_prices_with_fallback


class FakeFeed:
    def __init__(self, events: list[tuple[float, float, dict[str, object]]], delays: list[float] | None = None) -> None:
        self._events = events
        self._delays = delays or [0.0] * len(events)

    async def stream_prices(self):
        for delay, event in zip(self._delays, self._events):
            if delay:
                await asyncio.sleep(delay)
            yield event


async def _collect_n(aiter, n: int):
    out = []
    async for item in aiter:
        out.append(item)
        if len(out) >= n:
            break
    return out


def test_rtds_staleness_triggers_fallback_then_recovers() -> None:
    rtds = FakeFeed(
        events=[
            (1.0, 50000.0, {"source": "chainlink_rtds", "timestamp": 1.0}),
            (2.0, 50020.0, {"source": "chainlink_rtds", "timestamp": 2.0}),
        ],
        delays=[0.0, 0.05],
    )
    fallback = FakeFeed(events=[(1.5, 50010.0, {"source": "chainlink_direct", "timestamp": 1.5})])

    stream = stream_prices_with_fallback(
        rtds,
        fallback,
        use_fallback_feed=True,
        price_staleness_threshold=0.01,
    )

    got = asyncio.run(_collect_n(stream, 3))
    assert [item[2]["source"] for item in got] == ["chainlink_rtds", "chainlink_direct", "chainlink_rtds"]


def test_clob_stale_detection_logs_warning_when_updates_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    warnings: list[dict[str, object]] = []

    class FakeWS:
        def __init__(self) -> None:
            self._sent_first = False

        async def send(self, _payload: str) -> None:
            return

        def __aiter__(self):
            return self

        async def __anext__(self):
            if not self._sent_first:
                self._sent_first = True
                return '{"event_type":"book","asset_id":"token-a","bids":[["0.2","1"]],"asks":[["0.3","1"]],"timestamp":123}'
            await asyncio.sleep(3600)
            raise StopAsyncIteration

    class FakeConnectCtx:
        async def __aenter__(self):
            return FakeWS()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    async def fake_heartbeat(*_args, **_kwargs):
        await asyncio.sleep(3600)

    monkeypatch.setattr("feeds.clob_ws.websockets.connect", lambda *_a, **_k: FakeConnectCtx())
    monkeypatch.setattr("feeds.clob_ws.CLOBWebSocket._heartbeat", fake_heartbeat)
    monkeypatch.setattr("feeds.clob_ws.logger.warning", lambda event, **kwargs: warnings.append({"event": event, **kwargs}))

    async def _run() -> None:
        clob = CLOBWebSocket("wss://unused", stale_after_seconds=0.01)
        stream = clob.stream_books(["token-a"])
        first = await stream.__anext__()
        assert first.token_id == "token-a"

        task = asyncio.create_task(stream.__anext__())
        await asyncio.sleep(0.05)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    asyncio.run(_run())
    assert any(w["event"] == "clob_orderbook_stale" for w in warnings)


@dataclass
class _Now:
    hour: int


class _FakeDatetime:
    fixed_hour = 9

    @classmethod
    def now(cls, _tz):
        return _Now(hour=cls.fixed_hour)


def test_trader_risk_hourly_cap_and_daily_loss_lockout(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = Settings(dry_run=True, max_trades_per_hour=2, quote_size_usd=10, max_daily_loss=50)
    trader = Trader(settings)
    monkeypatch.setattr("execution.trader.datetime", _FakeDatetime)

    async def _run() -> None:
        assert await trader.buy_fok("token-a", ask=0.5, horizon="5") is True
        assert await trader.buy_fok("token-a", ask=0.5, horizon="5") is True
        assert await trader.buy_fok("token-a", ask=0.5, horizon="5") is False

        _FakeDatetime.fixed_hour = 10
        trader.risk.daily_realized_pnl = -50.0
        assert await trader.buy_fok("token-a", ask=0.5, horizon="5") is False

    asyncio.run(_run())


class FakeClob:
    def __init__(self) -> None:
        self.subscriptions: list[list[str]] = []

    async def stream_books(self, token_ids: list[str]):
        self.subscriptions.append(token_ids)
        yield BookTop(token_id=token_ids[0], best_bid=0.1, best_ask=0.2, ts=1.0)


def test_market_epoch_roll_triggers_clob_resubscribe() -> None:
    clob = FakeClob()
    state = {"current": {"epoch-1-token"}}

    async def _run() -> tuple[BookTop, BookTop]:
        stream = stream_clob_with_resubscribe(clob, lambda: state["current"])
        first = await stream.__anext__()
        state["current"] = {"epoch-2-token"}
        second = await stream.__anext__()
        return first, second

    first, second = asyncio.run(_run())
    assert first.token_id == "epoch-1-token"
    assert second.token_id == "epoch-2-token"
    assert clob.subscriptions == [["epoch-1-token"], ["epoch-2-token"]]
