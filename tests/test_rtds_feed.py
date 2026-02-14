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


def test_rtds_subscribe_both_topics_and_timestamp_normalization(monkeypatch: pytest.MonkeyPatch) -> None:
    ws = _FakeWebSocket(
        [
            json.dumps(
                {
                    "topic": "crypto_prices",
                    "payload": {
                        "symbol": "btc/usd",
                        "value": "43120.5",
                        "timestamp": 1712345678901,
                    },
                }
            ),
            json.dumps(
                {
                    "topic": "crypto_prices_chainlink",
                    "payload": {
                        "symbol": "btc/usd",
                        "value": "43123.5",
                        "timestamp": 1712345679901,
                    },
                }
            ),
        ]
    )
    monkeypatch.setattr("feeds.rtds.websockets.connect", _connect_factory(ws))

    feed = RTDSFeed("wss://unused", symbol="btc/usd", log_price_comparison=False)

    async def _run():
        stream = feed.stream_prices()
        item = await stream.__anext__()
        await stream.aclose()
        return item

    ts, price, metadata = asyncio.run(_run())

    assert ts == 1712345679.901
    assert price == 43123.5
    assert metadata["timestamp"] == 1712345679.901
    assert metadata["spot_price"] == 43120.5
    assert metadata["divergence_pct"] > 0

    assert len(ws.sent_payloads) == 1
    subscribe = json.loads(ws.sent_payloads[0])
    assert subscribe["action"] == "subscribe"
    topics = [item["topic"] for item in subscribe["subscriptions"]]
    assert topics == ["crypto_prices_chainlink"]
    assert subscribe["subscriptions"][0]["type"] == "*"
    for sub in subscribe["subscriptions"]:
        assert json.loads(sub["filters"]) == {"symbol": "btc/usd"}


def test_rtds_staleness_uses_normalized_timestamps(monkeypatch: pytest.MonkeyPatch) -> None:
    ws = _FakeWebSocket(
        [
            json.dumps(
                {
                    "topic": "crypto_prices_chainlink",
                    "payload": {
                        "symbol": "btc/usd",
                        "value": "50000",
                        "timestamp": 10.0,
                    },
                }
            ),
            json.dumps(
                {
                    "topic": "crypto_prices_chainlink",
                    "payload": {
                        "symbol": "btc/usd",
                        "value": "50001",
                        "timestamp": 12.0,
                    },
                }
            ),
        ]
    )
    monkeypatch.setattr("feeds.rtds.websockets.connect", _connect_factory(ws))

    warnings: list[dict[str, object]] = []
    monkeypatch.setattr("feeds.rtds.logger.warning", lambda event, **kwargs: warnings.append({"event": event, **kwargs}))

    timeline = iter([11.0, 17.0])
    monkeypatch.setattr("feeds.rtds.time.time", lambda: next(timeline, 15.0))

    feed = RTDSFeed(
        "wss://unused",
        symbol="btc/usd",
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
