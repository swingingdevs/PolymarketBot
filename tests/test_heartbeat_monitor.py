from __future__ import annotations

import asyncio

from config import Settings
from execution.heartbeat_monitor import HeartbeatMonitor
from execution.trader import Trader


def test_cancellation_triggered_after_two_consecutive_heartbeat_misses() -> None:
    async def scenario() -> None:
        attempts = {"count": 0}
        cancellations = {"count": 0}

        def send_heartbeat() -> bool:
            attempts["count"] += 1
            return False

        async def cancel_orders() -> None:
            cancellations["count"] += 1

        monitor = HeartbeatMonitor(
            enabled=True,
            interval_seconds=0.1,
            max_consecutive_failures=2,
            cancel_on_failure=True,
            send_heartbeat=send_heartbeat,
            on_failure_threshold_exceeded=cancel_orders,
        )

        monitor.start()
        await asyncio.sleep(0.35)
        await monitor.stop()

        assert attempts["count"] >= 2
        assert cancellations["count"] == 1

    asyncio.run(scenario())


def test_consecutive_miss_counter_resets_after_success() -> None:
    async def scenario() -> None:
        sequence = iter([False, True, False, False])
        cancellations = {"count": 0}

        def send_heartbeat() -> bool:
            try:
                return next(sequence)
            except StopIteration:
                return False

        async def cancel_orders() -> None:
            cancellations["count"] += 1

        monitor = HeartbeatMonitor(
            enabled=True,
            interval_seconds=0.1,
            max_consecutive_failures=2,
            cancel_on_failure=True,
            send_heartbeat=send_heartbeat,
            on_failure_threshold_exceeded=cancel_orders,
        )

        monitor.start()
        await asyncio.sleep(0.45)
        await monitor.stop()

        assert cancellations["count"] == 1

    asyncio.run(scenario())


def test_no_cancellation_in_dry_run_mode(tmp_path) -> None:
    async def scenario() -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.cancel_calls = 0

            def cancel_all(self) -> None:
                self.cancel_calls += 1

        trader = Trader(Settings(dry_run=True, risk_state_path=str(tmp_path / "risk.json")))
        fake_client = FakeClient()
        trader.client = fake_client

        monitor = HeartbeatMonitor(
            enabled=True,
            interval_seconds=0.1,
            max_consecutive_failures=2,
            cancel_on_failure=True,
            send_heartbeat=lambda: False,
            on_failure_threshold_exceeded=trader.cancel_outstanding_orders,
        )

        monitor.start()
        await asyncio.sleep(0.35)
        await monitor.stop()

        assert fake_client.cancel_calls == 0

    asyncio.run(scenario())
