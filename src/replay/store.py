from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator


@dataclass(frozen=True, slots=True)
class ReplayEvent:
    ts: float
    stream: str
    payload: dict[str, object]


class SQLiteReplayStore:
    """Append-only replay storage using SQLite."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS replay_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    stream TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_replay_ts ON replay_events(ts, id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_replay_stream ON replay_events(stream, ts, id)")

    def append(self, event: ReplayEvent) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO replay_events(ts, stream, payload_json) VALUES (?, ?, ?)",
                (event.ts, event.stream, json.dumps(event.payload, sort_keys=True)),
            )

    def append_many(self, events: Iterable[ReplayEvent]) -> None:
        rows = [(e.ts, e.stream, json.dumps(e.payload, sort_keys=True)) for e in events]
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany("INSERT INTO replay_events(ts, stream, payload_json) VALUES (?, ?, ?)", rows)

    def replay(self, *, stream: str | None = None) -> Iterator[ReplayEvent]:
        query = "SELECT ts, stream, payload_json FROM replay_events"
        params: tuple[object, ...] = ()
        if stream is not None:
            query += " WHERE stream = ?"
            params = (stream,)
        query += " ORDER BY ts ASC, id ASC"

        with sqlite3.connect(self.db_path) as conn:
            for ts, stream_name, payload_json in conn.execute(query, params):
                payload = json.loads(payload_json)
                if not isinstance(payload, dict):
                    payload = {"value": payload}
                yield ReplayEvent(ts=float(ts), stream=str(stream_name), payload=payload)
