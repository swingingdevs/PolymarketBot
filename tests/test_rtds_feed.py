from __future__ import annotations

import asyncio
import json

import orjson
import pytest

from feeds.rtds import RTDSFeed


class _FakeWebSocket:
    def __init__(self, messages: list[str]) -> None:
        self._messages = messages
        self._idx = 0
        self.sent_payloads: list[str | bytes] = []

    async def send(self, payload: str | bytes) -> None:
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


class _FailingConnectCtx:
    async def __aenter__(self):
        raise RuntimeError("connect failed")

    async def __aexit__(self, exc_type, exc, tb):
        return False


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
    subscribe = orjson.loads(ws.sent_payloads[0])
    assert subscribe["action"] == "subscribe"
    topics = [item["topic"] for item in subscribe["subscriptions"]]
    assert topics == ["crypto_prices_chainlink", "crypto_prices"]
    assert subscribe["subscriptions"][0]["type"] == "*"
    for sub in subscribe["subscriptions"]:
        assert json.loads(sub["filters"]) == {"symbol": "btc/usd"}



def test_rtds_divergence_only_when_spot_is_fresh(monkeypatch: pytest.MonkeyPatch) -> None:
    fresh_ws = _FakeWebSocket(
        [
            json.dumps(
                {
                    "topic": "crypto_prices",
                    "payload": {
                        "symbol": "btc/usd",
                        "value": "100.0",
                        "timestamp": 1000.0,
                    },
                }
            ),
            json.dumps(
                {
                    "topic": "crypto_prices_chainlink",
                    "payload": {
                        "symbol": "btc/usd",
                        "value": "101.0",
                        "timestamp": 1001.0,
                    },
                }
            ),
        ]
    )
    monkeypatch.setattr("feeds.rtds.websockets.connect", _connect_factory(fresh_ws))

    fresh_feed = RTDSFeed("wss://unused", symbol="btc/usd", spot_max_age_seconds=2.0, log_price_comparison=False)

    async def _run_once(feed: RTDSFeed):
        stream = feed.stream_prices()
        item = await stream.__anext__()
        await stream.aclose()
        return item

    _ts, _price, fresh_metadata = asyncio.run(_run_once(fresh_feed))
    assert fresh_metadata["spot_price"] == 100.0
    assert "divergence_pct" in fresh_metadata

    stale_ws = _FakeWebSocket(
        [
            json.dumps(
                {
                    "topic": "crypto_prices",
                    "payload": {
                        "symbol": "btc/usd",
                        "value": "100.0",
                        "timestamp": 995.0,
                    },
                }
            ),
            json.dumps(
                {
                    "topic": "crypto_prices_chainlink",
                    "payload": {
                        "symbol": "btc/usd",
                        "value": "101.0",
                        "timestamp": 1001.0,
                    },
                }
            ),
        ]
    )
    monkeypatch.setattr("feeds.rtds.websockets.connect", _connect_factory(stale_ws))

    stale_feed = RTDSFeed("wss://unused", symbol="btc/usd", spot_max_age_seconds=2.0, log_price_comparison=False)
    _ts, _price, stale_metadata = asyncio.run(_run_once(stale_feed))
    assert "spot_price" not in stale_metadata
    assert "divergence_pct" not in stale_metadata

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


def test_rtds_reconnect_before_stability_window_does_not_reset_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = iter(
        [
            _FailingConnectCtx(),
            _FakeConnectCtx(
                _FakeWebSocket(
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
                        )
                    ]
                )
            ),
            _FailingConnectCtx(),
        ]
    )

    def _connect(*_args, **_kwargs):
        return next(attempts)

    reconnect_delays: list[float] = []

    async def _fake_sleep(delay: float) -> None:
        reconnect_delays.append(delay)
        if len(reconnect_delays) >= 2:
            raise RuntimeError("stop")

    monkeypatch.setattr("feeds.rtds.websockets.connect", _connect)
    monkeypatch.setattr("feeds.rtds.asyncio.sleep", _fake_sleep)
    monkeypatch.setattr("feeds.rtds.time.time", lambda: 100.0)

    feed = RTDSFeed(
        "wss://unused",
        symbol="btc/usd",
        reconnect_delay_min=1,
        reconnect_delay_max=8,
        reconnect_stability_duration=60.0,
        log_price_comparison=False,
    )

    async def _run() -> None:
        stream = feed.stream_prices()
        ts, price, _metadata = await stream.__anext__()
        assert ts == 10.0
        assert price == 50000.0
        with pytest.raises(RuntimeError, match="stop"):
            await stream.__anext__()

    asyncio.run(_run())

    assert reconnect_delays == [1, 2]


def test_rtds_reconnect_after_stability_window_resets_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = iter(
        [
            _FailingConnectCtx(),
            _FakeConnectCtx(
                _FakeWebSocket(
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
                        )
                    ]
                )
            ),
            _FailingConnectCtx(),
        ]
    )

    def _connect(*_args, **_kwargs):
        return next(attempts)

    reconnect_delays: list[float] = []

    async def _fake_sleep(delay: float) -> None:
        reconnect_delays.append(delay)
        if len(reconnect_delays) >= 2:
            raise RuntimeError("stop")

    timeline = iter([100.0, 170.0, 170.0, 170.0])

    monkeypatch.setattr("feeds.rtds.websockets.connect", _connect)
    monkeypatch.setattr("feeds.rtds.asyncio.sleep", _fake_sleep)
    monkeypatch.setattr("feeds.rtds.time.time", lambda: next(timeline, 170.0))

    feed = RTDSFeed(
        "wss://unused",
        symbol="btc/usd",
        reconnect_delay_min=1,
        reconnect_delay_max=8,
        reconnect_stability_duration=60.0,
        log_price_comparison=False,
    )

    async def _run() -> None:
        stream = feed.stream_prices()
        ts, price, _metadata = await stream.__anext__()
        assert ts == 10.0
        assert price == 50000.0
        with pytest.raises(RuntimeError, match="stop"):
            await stream.__anext__()

    asyncio.run(_run())

    assert reconnect_delays == [1, 1]


def test_rtds_extracts_alternate_nested_price_and_timestamp_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    ws = _FakeWebSocket(
        [
            json.dumps(
                {
                    "topic": "crypto_prices_chainlink",
                    "payload": {
                        "envelope": {
                            "symbol": "btc/usd",
                            "price": "43123.5",
                            "ts": 1712345679901,
                        }
                    },
                }
            )
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


def test_rtds_logs_reason_codes_for_dropped_messages(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = iter(
        [
            _FakeConnectCtx(
                _FakeWebSocket(
                    [
                        json.dumps({"topic": "crypto_prices_chainlink", "payload": {"value": "100", "timestamp": 1}}),
                        json.dumps({"topic": "crypto_prices_chainlink", "payload": {"symbol": "btc/usd", "timestamp": 1}}),
                        json.dumps({"topic": "crypto_prices_chainlink", "payload": {"symbol": "btc/usd", "value": "100"}}),
                        json.dumps({"topic": "unexpected_topic", "payload": {"symbol": "btc/usd", "value": "100", "timestamp": 1}}),
                    ]
                )
            ),
            _FailingConnectCtx(),
        ]
    )

    def _connect(*_args, **_kwargs):
        return next(attempts)

    warnings: list[dict[str, object]] = []

    async def _fake_sleep(_delay: float) -> None:
        raise RuntimeError("stop")

    monkeypatch.setattr("feeds.rtds.websockets.connect", _connect)
    monkeypatch.setattr("feeds.rtds.asyncio.sleep", _fake_sleep)
    monkeypatch.setattr("feeds.rtds.logger.warning", lambda event, **kwargs: warnings.append({"event": event, **kwargs}))

    feed = RTDSFeed("wss://unused", symbol="btc/usd", log_price_comparison=False)

    async def _run() -> None:
        stream = feed.stream_prices()
        with pytest.raises(RuntimeError, match="stop"):
            await stream.__anext__()

    asyncio.run(_run())

    dropped = [w for w in warnings if w.get("event") == "rtds_message_dropped"]
    reason_codes = [item["reason_code"] for item in dropped]
    assert reason_codes == ["missing_symbol", "missing_price", "missing_timestamp", "topic_mismatch"]
    assert all("sample_shape" in item for item in dropped)
