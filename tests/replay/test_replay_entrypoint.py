from __future__ import annotations

import subprocess
import sys

from replay.store import ReplayEvent, SQLiteReplayStore


def test_replay_entrypoint_outputs_ordered_events(tmp_path) -> None:
    db = tmp_path / "events.sqlite"
    store = SQLiteReplayStore(db)
    store.append(ReplayEvent(ts=3.0, stream="clob", payload={"ask": 0.6}))
    store.append(ReplayEvent(ts=1.0, stream="rtds", payload={"price": 50000.0}))

    result = subprocess.run(
        [sys.executable, "scripts/replay_runtime.py", "--db", str(db)],
        capture_output=True,
        text=True,
        check=True,
    )

    lines = [line for line in result.stdout.strip().splitlines() if line]
    assert lines[0].startswith("1.000 [rtds]")
    assert lines[1].startswith("3.000 [clob]")
