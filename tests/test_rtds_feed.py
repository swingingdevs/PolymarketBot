from __future__ import annotations

import asyncio
import json

import pytest

from feeds.rtds import RTDSFeed


class _FakeWebSocket:
    def __init__(self, messages: list[str]) -> None:
        self._messages = messages
        self._idx = 0
        self.sent_payloads: list[str] = []

    async def send(self, payload: str) -> None:
        self.sent_payloads.append(payload)

    def ping(self):
        fut = asyncio.get_event_loop().create_future()
        fut.set_result(None)
        return fut

    def __aiter__(self):
        return self

    async def __anext__(self) -> str:
        if self._idx >= len(self._messages):
            raise StopAsyncIteration
        msg = self._messages[self._idx]
        self._idx += 1
        return msg


class _FakeConnectCtx:
    def __init__(self, ws: _FakeWebSocket) -> None:
        self.ws = ws

    async def __aenter__(self):
        return self.ws

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _connect_factory(ws: _FakeWebSocket):
    def _connect(*_args, **_kwargs):
        return _FakeConnectCtx(ws)

    return _connect


def test_rtds_subscribe_envelope_and_symbol_case_normalization(monkeypatch: pytest.MonkeyPatch) -> None:
    ws = _FakeWebSocket(
        [
            json.dumps(
                {
                    "payload": {
                        "symbol": "btc/usd",
                        "value": "43123.5",
                        "timestamp_ms": 1712345678901,
                    }
                }
            )
        ]
    )
    monkeypatch.setattr("feeds.rtds.websockets.connect", _connect_factory(ws))

    feed = RTDSFeed("wss://unused", symbol="BTC/USD", log_price_comparison=False)

    async def _run():
        stream = feed.stream_prices()
        item = await stream.__anext__()
        await stream.aclose()
        return item

    ts, price, metadata = asyncio.run(_run())

    assert ts == 1712345678.901
    assert price == 43123.5
    assert metadata["timestamp"] == 1712345678.901
    assert metadata["timestamp_ms"] == 1712345678901.0
    assert metadata["timestamp_s"] == 1712345678.901

    assert len(ws.sent_payloads) == 1
    subscribe = json.loads(ws.sent_payloads[0])
    assert subscribe["action"] == "subscribe"
    assert subscribe["subscriptions"][0]["topic"] == "crypto_prices_chainlink"
    assert subscribe["subscriptions"][0]["type"] == "market"
    assert json.loads(subscribe["subscriptions"][0]["filters"]) == {"symbol": "BTC/USD"}


def test_rtds_staleness_uses_second_based_timestamps(monkeypatch: pytest.MonkeyPatch) -> None:
    ws = _FakeWebSocket(
        [
            json.dumps(
                {
                    "payload": {
                        "symbol": "BTC/USD",
                        "value": "50000",
                        "timestamp_ms": 10_000,
                    }
                }
            ),
            json.dumps(
                {
                    "payload": {
                        "symbol": "btc/usd",
                        "value": "50001",
                        "timestamp_ms": 12_000,
                    }
                }
            ),
        ]
    )
    monkeypatch.setattr("feeds.rtds.websockets.connect", _connect_factory(ws))

    warnings: list[dict[str, object]] = []
    monkeypatch.setattr("feeds.rtds.logger.warning", lambda event, **kwargs: warnings.append({"event": event, **kwargs}))

    timeline = iter([11.0, 15.0])
    monkeypatch.setattr("feeds.rtds.time.time", lambda: next(timeline, 15.0))

    feed = RTDSFeed(
        "wss://unused",
        symbol="BTC/USD",
        price_staleness_threshold=3,
        reconnect_delay_min=0,
        reconnect_delay_max=0,
        log_price_comparison=False,
    )

    async def _run() -> None:
        stream = feed.stream_prices()
        ts, _price, _metadata = await stream.__anext__()
        assert ts == 10.0
        await stream.__anext__()
        await stream.aclose()

    asyncio.run(_run())

    stale = [w for w in warnings if w.get("event") == "rtds_price_stale"]
    assert stale
    assert stale[0]["stale_seconds"] == 5.0
