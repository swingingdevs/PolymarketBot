from __future__ import annotations

from replay.store import ReplayEvent, SQLiteReplayStore


def test_replay_store_append_only_ordering(tmp_path) -> None:
    db = tmp_path / "events.sqlite"
    store = SQLiteReplayStore(db)

    store.append_many(
        [
            ReplayEvent(ts=2.0, stream="clob", payload={"token": "a", "ask": 0.5}),
            ReplayEvent(ts=1.0, stream="rtds", payload={"price": 50000.0}),
            ReplayEvent(ts=2.0, stream="rtds", payload={"price": 50010.0}),
        ]
    )

    all_events = list(store.replay())
    assert [(e.ts, e.stream) for e in all_events] == [(1.0, "rtds"), (2.0, "clob"), (2.0, "rtds")]


def test_replay_stream_filter(tmp_path) -> None:
    db = tmp_path / "events.sqlite"
    store = SQLiteReplayStore(db)
    store.append(ReplayEvent(ts=1.0, stream="rtds", payload={"price": 1}))
    store.append(ReplayEvent(ts=2.0, stream="clob", payload={"ask": 0.4}))

    rtds = list(store.replay(stream="rtds"))
    assert len(rtds) == 1
    assert rtds[0].payload == {"price": 1}
