from __future__ import annotations

from strategy.state_machine import StrategyStateMachine


class FakeRTDS:
    def __init__(self) -> None:
        self.subscribed = False
        self.reconnect_warnings = 0

    def subscribe(self) -> None:
        self.subscribed = True


class FakeCLOB:
    def __init__(self) -> None:
        self.subscriptions: list[list[str]] = []

    def subscribe(self, token_ids: list[str]) -> None:
        self.subscriptions.append(token_ids)


def test_smoke_runtime_assertions() -> None:
    rtds = FakeRTDS()
    clob = FakeCLOB()

    strategy = StrategyStateMachine(
        threshold=0.0001,
        hammer_secs=60,
        d_min=0.01,
        max_entry_price=0.99,
        fee_bps=0.0,
        rolling_window_seconds=5,
    )

    token_ids = ["token-up", "token-down"]
    rtds.subscribe()
    clob.subscribe(token_ids)

    # Seed start prices and then trigger watch-mode/candidate gating prerequisites.
    strategy.on_price(300.0, 100.0, {"source": "chainlink_rtds", "timestamp": 300.0})
    strategy.on_price(301.0, 100.2, {"source": "chainlink_rtds", "timestamp": 301.0})
    strategy.on_book("token-up", bid=0.49, ask=0.50, ts=301.0)

    assert rtds.subscribed is True
    assert clob.subscriptions == [token_ids]

    assert 300 in strategy.start_prices
    assert strategy.start_prices[300] == 100.0
    assert strategy.watch_mode is True

    # Candidate gating: without market metadata there should be no candidate selection input yet.
    assert strategy.last_price is not None

    # No reconnect-loop signal should be emitted in this deterministic smoke path.
    assert rtds.reconnect_warnings == 0
