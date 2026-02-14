import asyncio

from markets.fee_rate_cache import FeeRateCache


class _FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    def raise_for_status(self) -> None:
        return None

    async def json(self) -> dict[str, object]:
        return self._payload


class _FakeSession:
    def __init__(self, payloads: dict[str, dict[str, object]]) -> None:
        self.payloads = payloads
        self.calls: list[str] = []
        self.closed = False

    def get(self, _url: str, *, params: dict[str, str], timeout: float):
        del timeout
        token_id = params["token_id"]
        self.calls.append(token_id)
        return _FakeResponse(self.payloads[token_id])

    async def close(self) -> None:
        self.closed = True


def test_fee_rate_cache_warm_and_ttl(monkeypatch) -> None:
    cache = FeeRateCache("https://clob.polymarket.com", ttl_seconds=60)
    fake_session = _FakeSession({"A": {"fee_rate_bps": 12.5}, "B": {"feeRateBps": 9.0}})
    cache._session = fake_session

    now = [100.0]
    monkeypatch.setattr("markets.fee_rate_cache.time.time", lambda: now[0])

    asyncio.run(cache.warm(["A", "B"]))
    assert cache.get_fee_rate_bps("A") == 12.5
    assert cache.get_fee_rate_bps("B") == 9.0
    assert fake_session.calls == ["A", "B"]

    asyncio.run(cache.warm(["A", "B"]))
    assert fake_session.calls == ["A", "B"]

    now[0] = 200.0
    assert cache.get_fee_rate_bps("A") is None


def test_fee_rate_cache_handles_missing_values(monkeypatch) -> None:
    cache = FeeRateCache("https://clob.polymarket.com", ttl_seconds=60)
    fake_session = _FakeSession({"A": {"unexpected": True}})
    cache._session = fake_session
    monkeypatch.setattr("markets.fee_rate_cache.time.time", lambda: 100.0)

    asyncio.run(cache.warm(["A"]))
    assert cache.get_fee_rate_bps("A") is None
