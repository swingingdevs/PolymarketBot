from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any

import orjson
import structlog

logger = structlog.get_logger(__name__)


class EventRecorder:
    """Append-only JSONL recorder with non-blocking enqueue semantics."""

    def __init__(self, output_path: str | Path, *, queue_maxsize: int = 10_000, enabled: bool = True) -> None:
        self.output_path = Path(output_path)
        self.enabled = enabled
        self._queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue(maxsize=max(1, queue_maxsize))
        self._writer_task: asyncio.Task[None] | None = None
        self._dropped_events = 0

    async def start(self) -> None:
        if not self.enabled or self._writer_task is not None:
            return
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self._writer_task = asyncio.create_task(self._writer_loop())

    async def stop(self) -> None:
        if not self.enabled:
            return
        if self._writer_task is None:
            return
        await self._queue.put(None)
        await self._writer_task
        self._writer_task = None

    def record(self, event_type: str, **payload: Any) -> None:
        if not self.enabled:
            return
        event = {
            "type": event_type,
            "ts": float(payload.get("ts", time.time())),
            **payload,
        }
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            self._dropped_events += 1
            if self._dropped_events in {1, 10, 100} or self._dropped_events % 1000 == 0:
                logger.warning("recorder_queue_full", dropped_events=self._dropped_events)

    async def _writer_loop(self) -> None:
        with self.output_path.open("ab") as handle:
            while True:
                item = await self._queue.get()
                if item is None:
                    break
                handle.write(orjson.dumps(item))
                handle.write(b"\n")
                if self._queue.empty():
                    handle.flush()


__all__ = ["EventRecorder"]
