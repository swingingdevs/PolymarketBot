from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from replay.store import SQLiteReplayStore


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay deterministic runtime events from SQLite store")
    parser.add_argument("--db", required=True, help="Path to replay SQLite database")
    parser.add_argument("--stream", default=None, help="Optional stream filter")
    args = parser.parse_args()

    store = SQLiteReplayStore(Path(args.db))
    for event in store.replay(stream=args.stream):
        print(f"{event.ts:.3f} [{event.stream}] {event.payload}")


if __name__ == "__main__":
    main()
