from __future__ import annotations

import asyncio

from config import Settings
from execution.trader import Trader
from feeds.clob_ws import CLOBWebSocket


def test_order_rejected_when_below_min_size_and_adjustment_breaks_risk_limits(tmp_path) -> None:
    settings = Settings(
        dry_run=True,
        quote_size_usd=0.2,
        max_usd_per_trade=0.4,
        risk_state_path=str(tmp_path / "risk_state.json"),
    )
    trader = Trader(settings)
    trader.update_token_constraints("token-a", min_order_size=1.0)

    ok = asyncio.run(trader.buy_fok("token-a", ask=0.5, horizon="5"))

    assert ok is False
    assert trader.risk.trades_this_hour == 0
    assert trader.risk.total_open_notional_usd == 0.0


def test_order_accepted_when_adjusted_to_min_size_within_risk_limits(tmp_path) -> None:
    settings = Settings(
        dry_run=True,
        quote_size_usd=0.2,
        max_usd_per_trade=5.0,
        risk_state_path=str(tmp_path / "risk_state.json"),
    )
    trader = Trader(settings)
    trader.update_token_constraints("token-a", min_order_size=1.0)

    ok = asyncio.run(trader.buy_fok("token-a", ask=0.5, horizon="5"))

    assert ok is True
    assert trader.risk.trades_this_hour == 1
    assert trader.risk.total_open_notional_usd == 0.5


def test_clob_cache_updates_on_tick_size_change_event(monkeypatch) -> None:
    class FakeWS:
        def __init__(self) -> None:
            self._messages = [
                '{"event_type":"tick_size_change","asset_id":"token-a","tick_size":"0.01"}',
                '{"event_type":"book","asset_id":"token-a","bids":[["0.20","1"]],"asks":[["0.21","1"]],"timestamp":123}',
            ]

        async def send(self, _payload: str) -> None:
            return

        async def recv(self) -> str:
            if self._messages:
                return self._messages.pop(0)
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

    async def _run() -> CLOBWebSocket:
        clob = CLOBWebSocket("wss://unused", book_staleness_threshold=60)
        stream = clob.stream_books(["token-a"])
        first = await stream.__anext__()
        assert first.token_id == "token-a"
        return clob

    clob = asyncio.run(_run())
    constraints = clob.get_token_constraints("token-a")
    assert constraints is not None
    assert constraints.tick_size == 0.01
