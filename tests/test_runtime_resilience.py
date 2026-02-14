from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from datetime import date

import pytest

from config import Settings
from execution.trader import Trader
from markets.token_metadata_cache import TokenMetadata, TokenMetadataCache
from feeds import clob_ws
from feeds.clob_ws import BookTop, CLOBWebSocket
from main import _wait_for_first_completed, stream_clob_with_resubscribe, stream_prices_with_fallback, update_divergence_kill_switch


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


def test_kill_switch_does_not_progress_when_divergence_missing_or_stale() -> None:
    settings = Settings(divergence_threshold_pct=0.5, divergence_sustain_seconds=5, rtds_spot_max_age_seconds=2.0)
    kill_switch_state: dict[str, object] = {
        "breach_started_at": None,
        "active": False,
        "last_divergence_pct": None,
    }

    update_divergence_kill_switch(
        ts=100.0,
        metadata={"divergence_pct": 1.0, "spot_price": 50000.0},
        kill_switch_state=kill_switch_state,
        settings=settings,
    )
    assert kill_switch_state["breach_started_at"] == 100.0
    assert kill_switch_state["active"] is False

    update_divergence_kill_switch(
        ts=104.0,
        metadata={},
        kill_switch_state=kill_switch_state,
        settings=settings,
    )
    assert kill_switch_state["breach_started_at"] is None
    assert kill_switch_state["active"] is False

    update_divergence_kill_switch(
        ts=105.0,
        metadata={"divergence_pct": 1.0, "spot_price": 50010.0},
        kill_switch_state=kill_switch_state,
        settings=settings,
    )
    assert kill_switch_state["breach_started_at"] == 105.0

    update_divergence_kill_switch(
        ts=109.0,
        metadata={},
        kill_switch_state=kill_switch_state,
        settings=settings,
    )
    update_divergence_kill_switch(
        ts=110.1,
        metadata={"divergence_pct": 1.0, "spot_price": 50020.0},
        kill_switch_state=kill_switch_state,
        settings=settings,
    )
    assert kill_switch_state["active"] is False
    assert kill_switch_state["breach_started_at"] == 110.1


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

    assert hasattr(clob_ws.websockets, "connect")
    assert hasattr(CLOBWebSocket, "_heartbeat")
    assert hasattr(clob_ws.logger, "warning")

    class FakeWS:
        def __init__(self) -> None:
            self._sent_first = False

        async def send(self, _payload: str) -> None:
            return

        async def recv(self) -> str:
            if not self._sent_first:
                self._sent_first = True
                return '{"event_type":"book","asset_id":"token-a","bids":[{"price":"0.2","size":"1"}],"asks":[{"price":"0.3","size":"1"}],"timestamp":123000}'
            await asyncio.sleep(3600)
            return ""

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
        clob = CLOBWebSocket("wss://unused", book_staleness_threshold=0.01)
        stream = clob.stream_books(["token-a"])
        first = await stream.__anext__()
        assert first.token_id == "token-a"

        task = asyncio.create_task(stream.__anext__())
        await asyncio.sleep(1.1)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    asyncio.run(_run())
    stale_warnings = [w for w in warnings if w["event"] == "clob_orderbook_stale"]
    assert stale_warnings
    assert any(
        w.get("staleness_threshold_seconds") == 0.01 and w.get("token_ids") == ["token-a"] for w in stale_warnings
    )


@dataclass
class _Now:
    hour: int
    day: date

    def date(self) -> date:
        return self.day


class _FakeDatetime:
    fixed_hour = 9
    fixed_day = date(2026, 1, 1)

    @classmethod
    def now(cls, _tz):
        return _Now(hour=cls.fixed_hour, day=cls.fixed_day)


def test_trader_risk_hourly_cap_and_daily_loss_lockout(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr(Settings, "settings_profile", "paper", raising=False)
    settings = Settings(dry_run=True, risk_state_path=str(tmp_path / "risk_state.json"), max_trades_per_hour=2, quote_size_usd=10, max_daily_loss=50)
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


def test_wait_for_first_completed_cancels_pending_task() -> None:
    cancelled = asyncio.Event()

    async def fast() -> str:
        return "done"

    async def slow() -> None:
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async def _run() -> None:
        fast_task = asyncio.create_task(fast())
        slow_task = asyncio.create_task(slow())
        winner = await _wait_for_first_completed(fast_task, slow_task)

        assert winner is fast_task
        assert winner.result() == "done"
        assert slow_task.cancelled() is True
        assert cancelled.is_set() is True

    asyncio.run(_run())


def test_wait_for_first_completed_propagates_crash_and_cancels_peer() -> None:
    cancelled = asyncio.Event()

    async def crash() -> None:
        raise RuntimeError("boom")

    async def slow() -> None:
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async def _run() -> None:
        crash_task = asyncio.create_task(crash())
        slow_task = asyncio.create_task(slow())
        winner = await _wait_for_first_completed(crash_task, slow_task)

        assert winner is crash_task
        with pytest.raises(RuntimeError, match="boom"):
            winner.result()
        assert slow_task.cancelled() is True
        assert cancelled.is_set() is True

    asyncio.run(_run())


class _CaptureClient:
    def __init__(self) -> None:
        self.market_order_args: dict[str, object] | None = None
        self.posted_order: dict[str, object] | None = None

    def create_market_order(self, **kwargs):
        self.market_order_args = kwargs
        return kwargs

    def post_order(self, order, time_in_force="FOK"):
        self.posted_order = {"order": order, "time_in_force": time_in_force}
        return {"fills": [{"price": order.get("price", 0.0) or 0.0, "size": order["size"]}]}


def test_buy_fok_uses_market_order_api(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr(Settings, "settings_profile", "paper", raising=False)
    settings = Settings(dry_run=False, risk_state_path=str(tmp_path / "risk_state.json"), quote_size_usd=10)
    trader = Trader(settings)
    trader.client = _CaptureClient()
    trader._live_auth_ready = True

    async def _run() -> None:
        assert await trader.buy_fok("token-a", ask=0.501, horizon="5") is True

    asyncio.run(_run())
    assert trader.client.market_order_args is not None
    assert trader.client.market_order_args["token_id"] == "token-a"
    assert trader.client.posted_order is not None
    assert trader.client.posted_order["time_in_force"] == "FOK"


def test_buy_fok_market_order_requests_fok_for_every_submit(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr(Settings, "settings_profile", "paper", raising=False)
    settings = Settings(dry_run=False, risk_state_path=str(tmp_path / "risk_state.json"), quote_size_usd=10)
    trader = Trader(settings)
    trader.client = _CaptureClient()
    trader._live_auth_ready = True

    asks = [0.5012, 0.50001, 0.9990004]
    submitted_tifs: list[str] = []

    async def _run() -> None:
        for ask in asks:
            assert await trader.buy_fok("token-a", ask=ask, horizon="5") is True
            assert trader.client.posted_order is not None
            submitted_tifs.append(str(trader.client.posted_order["time_in_force"]))

    asyncio.run(_run())
    assert submitted_tifs == ["FOK", "FOK", "FOK"]


def test_buy_fok_best_ask_0983_with_tick_0001_never_submits_0982(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr(Settings, "settings_profile", "paper", raising=False)
    settings = Settings(dry_run=False, risk_state_path=str(tmp_path / "risk_state.json"), quote_size_usd=10)
    trader = Trader(settings)
    trader.client = _CaptureClient()
    trader._live_auth_ready = True
    trader.update_token_constraints("token-a", tick_size=0.001)

    async def _run() -> None:
        assert await trader.buy_fok("token-a", ask=0.983, horizon="5") is True

    asyncio.run(_run())
    assert trader.client.market_order_args is not None
    assert trader.client.posted_order is not None


def test_buy_fok_honors_non_default_tick_size(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr(Settings, "settings_profile", "paper", raising=False)
    settings = Settings(dry_run=False, risk_state_path=str(tmp_path / "risk_state.json"), quote_size_usd=10)
    trader = Trader(settings)
    trader.client = _CaptureClient()
    trader._live_auth_ready = True
    trader.update_token_constraints("token-a", tick_size=0.005)

    async def _run() -> None:
        assert await trader.buy_fok("token-a", ask=0.983, horizon="5") is True

    asyncio.run(_run())
    assert trader.client.market_order_args is not None
    assert trader.client.posted_order is not None
