from __future__ import annotations

import json
from pathlib import Path

import pytest

from feeds.clob_ws import CLOBWebSocket

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "clob_ws"


@pytest.mark.parametrize(
    ("ws_base", "expected"),
    [
        ("wss://ws-subscriptions-clob.polymarket.com", "wss://ws-subscriptions-clob.polymarket.com/ws/market"),
        ("wss://ws-subscriptions-clob.polymarket.com/", "wss://ws-subscriptions-clob.polymarket.com/ws/market"),
        ("wss://ws-subscriptions-clob.polymarket.com/ws/market", "wss://ws-subscriptions-clob.polymarket.com/ws/market"),
        ("wss://example.com/custom/base", "wss://example.com/custom/base/ws/market"),
    ],
)
def test_build_ws_url_appends_market_path_safely(ws_base: str, expected: str) -> None:
    assert CLOBWebSocket._build_ws_url(ws_base) == expected


@pytest.mark.parametrize(
    ("fixture_name", "expected_token", "expected_bid", "expected_ask", "expected_ts"),
    [
        ("snapshot_book_array_levels.json", "token-a", 0.44, 0.45, 1712345678.0),
        ("book_update_changes_object.json", "token-a", 0.46, 0.47, 1712345679.0),
        ("price_update_changes_array_levels.json", "token-b", 0.52, 0.53, 1712345680.0),
    ],
)
def test_parse_fixture_payload_variants(
    fixture_name: str,
    expected_token: str,
    expected_bid: float,
    expected_ask: float,
    expected_ts: float,
) -> None:
    clob = CLOBWebSocket("wss://ws-subscriptions-clob.polymarket.com")
    payload = (FIXTURE_DIR / fixture_name).read_text(encoding="utf-8")

    tops = clob._parse_raw_message(payload, [0.0])

    assert len(tops) == 1
    top = tops[0]
    assert top.token_id == expected_token
    assert top.best_bid == expected_bid
    assert top.best_ask == expected_ask
    assert top.ts == expected_ts


class _DropMetricRecorder:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    def labels(self, *, reason: str, event_type: str) -> "_DropMetricRecorder":
        self.calls.append({"reason": reason, "event_type": event_type})
        return self

    def inc(self) -> None:
        return None


@pytest.mark.parametrize(
    ("raw_frame", "expected_reason", "expected_event_type"),
    [
        ("not-json", "invalid_json", "invalid_json"),
        (json.dumps(1), "message_not_object_or_array", "int"),
        (json.dumps({"event_type": "mystery", "asset_id": "token-z"}), "unrecognized_event_type", "mystery"),
    ],
)
def test_malformed_frames_emit_warning_and_dropped_metrics(
    monkeypatch: pytest.MonkeyPatch,
    raw_frame: str,
    expected_reason: str,
    expected_event_type: str,
) -> None:
    clob = CLOBWebSocket("wss://ws-subscriptions-clob.polymarket.com")
    recorder = _DropMetricRecorder()
    warnings: list[dict[str, object]] = []

    def _fake_warning(_event_name: str, **kwargs: object) -> None:
        warnings.append(kwargs)

    monkeypatch.setattr("feeds.clob_ws.CLOB_DROPPED_MESSAGES", recorder)
    monkeypatch.setattr("feeds.clob_ws.logger.warning", _fake_warning)

    tops = clob._parse_raw_message(raw_frame, [0.0])

    assert tops == []
    assert recorder.calls
    assert recorder.calls[-1] == {"reason": expected_reason, "event_type": expected_event_type}
    assert warnings
    assert warnings[-1]["reason"] == expected_reason
    assert warnings[-1]["event_type"] == expected_event_type


def test_price_change_after_migration_schema_parses_per_token_best_levels() -> None:
    clob = CLOBWebSocket("wss://ws-subscriptions-clob.polymarket.com")
    payload = json.dumps(
        {
            "event_type": "price_change",
            "timestamp": "1758000000000",
            "price_changes": [
                {
                    "asset_id": "token-new-1",
                    "best_bid": "0.41",
                    "best_ask": "0.43",
                    "hash": "0xabc",
                    "side": "BUY",
                    "price": "0.42",
                    "size": "100",
                },
                {
                    "asset_id": "token-new-2",
                    "best_bid": "0.57",
                    "best_ask": "0.59",
                    "hash": "0xdef",
                    "side": "SELL",
                    "price": "0.58",
                    "size": "75",
                },
            ],
        }
    )

    tops = clob._parse_raw_message(payload, [0.0])

    assert len(tops) == 2
    assert tops[0].token_id == "token-new-1"
    assert tops[0].best_bid == 0.41
    assert tops[0].best_ask == 0.43
    assert tops[1].token_id == "token-new-2"
    assert tops[1].best_bid == 0.57
    assert tops[1].best_ask == 0.59


def test_price_change_before_migration_schema_updates_last_update_without_best_levels() -> None:
    clob = CLOBWebSocket("wss://ws-subscriptions-clob.polymarket.com")
    last_update = [0.0]

    snapshot = json.dumps(
        {
            "event_type": "book",
            "asset_id": "token-legacy",
            "bids": [["0.45", "200"]],
            "asks": [["0.47", "220"]],
            "timestamp": "1758000000000",
        }
    )
    clob._parse_raw_message(snapshot, last_update)
    seeded_update_time = last_update[0]

    legacy_price_change = json.dumps(
        {
            "event_type": "price_change",
            "asset_id": "token-legacy",
            "changes": [
                {
                    "side": "BUY",
                    "price": "0.46",
                    "size": "150",
                }
            ],
            "timestamp": "1758000001000",
        }
    )

    tops = clob._parse_raw_message(legacy_price_change, last_update)

    assert len(tops) == 1
    assert tops[0].token_id == "token-legacy"
    assert tops[0].best_bid == 0.46
    assert tops[0].best_ask == 0.47
    assert last_update[0] >= seeded_update_time
