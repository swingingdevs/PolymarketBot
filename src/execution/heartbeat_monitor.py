from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from typing import Awaitable, Callable

import structlog

from metrics import (
    HEARTBEAT_CANCEL_ACTIONS,
    HEARTBEAT_CONSECUTIVE_MISSES,
    HEARTBEAT_SEND_ATTEMPTS,
    HEARTBEAT_SEND_FAILURE,
    HEARTBEAT_SEND_SUCCESS,
)

logger = structlog.get_logger(__name__)


@dataclass(slots=True)
class HeartbeatHealthState:
    enabled: bool
    running: bool
    consecutive_failures: int
    healthy: bool


class HeartbeatMonitor:
    def __init__(
        self,
        *,
        enabled: bool,
        interval_seconds: float,
        max_consecutive_failures: int,
        cancel_on_failure: bool,
        send_heartbeat: Callable[[], bool | Awaitable[bool]],
        on_failure_threshold_exceeded: Callable[[], None | Awaitable[None]] | None = None,
    ) -> None:
        self._enabled = bool(enabled)
        self._interval_seconds = max(0.1, float(interval_seconds))
        self._max_consecutive_failures = max(1, int(max_consecutive_failures))
        self._cancel_on_failure = bool(cancel_on_failure)
        self._send_heartbeat = send_heartbeat
        self._on_failure_threshold_exceeded = on_failure_threshold_exceeded
        self._task: asyncio.Task[None] | None = None
        self._stopped = asyncio.Event()
        self._consecutive_failures = 0
        self._threshold_action_triggered_for_streak = False

    @property
    def health_state(self) -> HeartbeatHealthState:
        healthy = self._consecutive_failures < self._max_consecutive_failures
        return HeartbeatHealthState(
            enabled=self._enabled,
            running=self._task is not None and not self._task.done(),
            consecutive_failures=self._consecutive_failures,
            healthy=healthy,
        )

    def start(self) -> None:
        if not self._enabled:
            logger.info("heartbeat_monitor_disabled")
            return
        if self._task is not None and not self._task.done():
            return
        self._stopped.clear()
        self._task = asyncio.create_task(self._run(), name="heartbeat_monitor")
        logger.info(
            "heartbeat_monitor_started",
            interval_seconds=self._interval_seconds,
            max_consecutive_failures=self._max_consecutive_failures,
            cancel_on_failure=self._cancel_on_failure,
        )

    async def stop(self) -> None:
        self._stopped.set()
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None
        logger.info("heartbeat_monitor_stopped")

    async def _run(self) -> None:
        while not self._stopped.is_set():
            await self._tick_once()
            await asyncio.sleep(self._interval_seconds)

    async def _tick_once(self) -> None:
        HEARTBEAT_SEND_ATTEMPTS.inc()
        heartbeat_ok = False

        try:
            maybe_result = self._send_heartbeat()
            heartbeat_ok = await maybe_result if inspect.isawaitable(maybe_result) else bool(maybe_result)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("heartbeat_send_failed", error=str(exc))
            heartbeat_ok = False

        if heartbeat_ok:
            HEARTBEAT_SEND_SUCCESS.inc()
            self._consecutive_failures = 0
            self._threshold_action_triggered_for_streak = False
            HEARTBEAT_CONSECUTIVE_MISSES.set(0)
            return

        HEARTBEAT_SEND_FAILURE.inc()
        self._consecutive_failures += 1
        HEARTBEAT_CONSECUTIVE_MISSES.set(self._consecutive_failures)

        if self._consecutive_failures < self._max_consecutive_failures:
            return

        if self._threshold_action_triggered_for_streak or not self._cancel_on_failure:
            return

        self._threshold_action_triggered_for_streak = True
        if self._on_failure_threshold_exceeded is None:
            HEARTBEAT_CANCEL_ACTIONS.labels(status="missing_handler").inc()
            return

        try:
            maybe_result = self._on_failure_threshold_exceeded()
            if inspect.isawaitable(maybe_result):
                await maybe_result
            HEARTBEAT_CANCEL_ACTIONS.labels(status="executed").inc()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            HEARTBEAT_CANCEL_ACTIONS.labels(status="error").inc()
            logger.exception("heartbeat_failure_action_failed", error=str(exc))
