from __future__ import annotations


def normalize_ts(value: object) -> float:
    """Normalize seconds/ms epoch inputs to epoch seconds."""
    ts = float(value)
    if ts > 1e12:
        return ts / 1000.0
    return ts

