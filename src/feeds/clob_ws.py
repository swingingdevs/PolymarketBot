from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import dataclass
from urllib.parse import urlparse
from typing import AsyncIterator

import orjson
import structlog
import websockets

from metrics import CLOB_DROPPED_MESSAGES, CLOB_PRICE_CHANGE_PARSED
from utils.time import normalize_ts

logger = structlog.get_logger(__name__)


@dataclass(slots=True)
class BookTop:
    token_id: str
    best_bid: float | None
    best_ask: float | None
    ts: float
    best_bid_size: float | None = None
    best_ask_size: float | None = None
    fill_prob: float | None = None
    bids_levels: list[tuple[float, float]] | None = None
    asks_levels: list[tuple[float, float]] | None = None


@dataclass(slots=True)
class TokenConstraints:
    min_order_size: float | None = None
    tick_size: float | None = None
    updated_at: float = 0.0


class CLOBWebSocket:
    def __init__(
        self,
        ws_base: str,
        ping_interval: int = 10,
        pong_timeout: int = 10,
        reconnect_delay_min: int = 1,
        reconnect_delay_max: int = 60,
        book_staleness_threshold: float = 10,
        stale_after_seconds: float | None = None,
        book_depth_levels: int = 10,
    ) -> None:
        self.ws_base = ws_base.rstrip("/")
        self.ws_url = self._build_ws_url(self.ws_base)
        self.ping_interval = ping_interval
        self.pong_timeout = pong_timeout
        self.reconnect_delay_min = reconnect_delay_min
        self.reconnect_delay_max = reconnect_delay_max
        self.book_staleness_threshold = float(stale_after_seconds) if stale_after_seconds is not None else float(book_staleness_threshold)
        self.book_depth_levels = max(1, int(book_depth_levels))
        self.token_metadata_cache: dict[str, TokenConstraints] = {}
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._subscribed_token_ids: set[str] = set()
        self._subscription_cache_token_ids: frozenset[str] | None = None
        self._subscription_cache_payload: bytes | None = None
        self._book_tops: dict[str, BookTop] = {}

    @staticmethod
    def _build_ws_url(ws_base: str) -> str:
        parsed = urlparse(ws_base)
        path = parsed.path.rstrip("/")
        if path.endswith("/ws/market"):
            final_path = path
        else:
            final_path = f"{path}/ws/market" if path else "/ws/market"
        return parsed._replace(path=final_path).geturl()

    @staticmethod
    def _coerce_positive_float(value: object) -> float | None:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        return parsed if parsed > 0 else None

    def _update_token_constraints(self, token_id: str, *, min_order_size: object = None, tick_size: object = None) -> bool:
        if not token_id:
            return False

        parsed_min_order_size = self._coerce_positive_float(min_order_size)
        parsed_tick_size = self._coerce_positive_float(tick_size)
        if parsed_min_order_size is None and parsed_tick_size is None:
            return False

        current = self.token_metadata_cache.get(token_id, TokenConstraints())
        changed = False

        if parsed_min_order_size is not None and current.min_order_size != parsed_min_order_size:
            current.min_order_size = parsed_min_order_size
            changed = True
        if parsed_tick_size is not None and current.tick_size != parsed_tick_size:
            current.tick_size = parsed_tick_size
            changed = True

        if changed:
            current.updated_at = time.time()
            self.token_metadata_cache[token_id] = current
            logger.info(
                "token_constraints_updated",
                token_id=token_id,
                min_order_size=current.min_order_size,
                tick_size=current.tick_size,
            )
        return changed

    def get_token_constraints(self, token_id: str) -> TokenConstraints | None:
        return self.token_metadata_cache.get(token_id)

    async def _heartbeat(self, ws: websockets.WebSocketClientProtocol, failed_pings: list[int]) -> None:
        while True:
            await asyncio.sleep(self.ping_interval)
            try:
                await ws.send("PING")
                failed_pings[0] = 0
            except Exception:
                failed_pings[0] += 1
                logger.warning("clob_ping_failed", failures=failed_pings[0])
                if failed_pings[0] >= 2:
                    raise RuntimeError("CLOB stale heartbeat")

    async def _stale_watchdog(self, last_update: list[float], token_ids: list[str], channel: str) -> None:
        last_warning_at = 0.0
        check_interval = max(0.01, self.book_staleness_threshold / 2)

        while True:
            await asyncio.sleep(check_interval)
            stale_seconds = time.time() - last_update[0]
            if stale_seconds <= self.book_staleness_threshold:
                continue
            if (time.time() - last_warning_at) < self.book_staleness_threshold:
                continue

            logger.warning(
                "clob_orderbook_stale",
                stale_seconds=stale_seconds,
                staleness_threshold_seconds=self.book_staleness_threshold,
                channel=channel,
                token_ids=token_ids,
            )
            last_warning_at = time.time()


    @staticmethod
    def _parse_levels(levels: object, *, max_levels: int) -> list[tuple[float, float]]:
        if not isinstance(levels, list):
            return []

        parsed_levels: list[tuple[float, float]] = []
        for raw in levels:
            if isinstance(raw, (list, tuple)) and len(raw) >= 2:
                raw_price, raw_size = raw[0], raw[1]
            elif isinstance(raw, dict):
                raw_price, raw_size = raw.get("price"), raw.get("size")
            else:
                continue

            try:
                price = float(raw_price)
                size = float(raw_size)
            except (TypeError, ValueError):
                continue
            if price <= 0 or size <= 0:
                continue
            parsed_levels.append((price, size))
            if len(parsed_levels) >= max_levels:
                break
        return parsed_levels

    @staticmethod
    def _extract_price_size(levels: object) -> tuple[float | None, float | None]:
        if isinstance(levels, dict):
            try:
                return float(levels.get("price")), float(levels.get("size"))
            except (TypeError, ValueError):
                return None, None

        if not isinstance(levels, list) or not levels:
            return None, None
        top = levels[0]

        if isinstance(top, (list, tuple)) and len(top) >= 2:
            try:
                return float(top[0]), float(top[1])
            except (TypeError, ValueError):
                return None, None

        if not isinstance(top, dict):
            return None, None
        try:
            return float(top.get("price")), float(top.get("size"))
        except (TypeError, ValueError):
            return None, None

    @staticmethod
    def _event_type(event: dict[str, object]) -> str:
        raw = event.get("event_type") or event.get("type")
        return str(raw) if raw is not None else "unknown"

    def _drop_message(self, reason: str, *, event_type: str, event: object = None) -> None:
        CLOB_DROPPED_MESSAGES.labels(reason=reason, event_type=event_type).inc()
        warning_payload: dict[str, object] = {"reason": reason, "event_type": event_type}
        if isinstance(event, dict):
            warning_payload["keys"] = sorted(event.keys())
        logger.warning("clob_message_dropped", **warning_payload)

    def _extract_book_levels(self, event: dict[str, object]) -> tuple[object, object]:
        payload = event.get("book")
        source = payload if isinstance(payload, dict) else event
        bids = source.get("bids") if isinstance(source, dict) else None
        asks = source.get("asks") if isinstance(source, dict) else None
        if bids is None:
            bids = source.get("buys") if isinstance(source, dict) else None
        if asks is None:
            asks = source.get("sells") if isinstance(source, dict) else None
        return bids, asks

    def _record_book_top(
        self,
        token_id: str,
        *,
        bid: float | None,
        ask: float | None,
        bid_size: float | None,
        ask_size: float | None,
        ts: float,
    ) -> BookTop:
        prev = self._book_tops.get(token_id)
        top = BookTop(
            token_id=token_id,
            best_bid=bid if bid is not None else (prev.best_bid if prev is not None else None),
            best_ask=ask if ask is not None else (prev.best_ask if prev is not None else None),
            best_bid_size=bid_size if bid_size is not None else (prev.best_bid_size if prev is not None else None),
            best_ask_size=ask_size if ask_size is not None else (prev.best_ask_size if prev is not None else None),
            ts=ts,
        )
        self._book_tops[token_id] = top
        return top

    def _apply_legacy_level_update(
        self,
        token_id: str,
        *,
        side: object,
        price: object,
        size: object,
        ts: float,
    ) -> BookTop | None:
        touched_price = self._coerce_positive_float(price)
        if touched_price is None:
            return None

        parsed_size = self._coerce_positive_float(size)
        normalized_side = str(side or "").strip().lower()
        if normalized_side in {"buy", "bid"}:
            bid = touched_price if parsed_size is None or parsed_size > 0 else None
            return self._record_book_top(token_id, bid=bid, ask=None, bid_size=parsed_size, ask_size=None, ts=ts)
        if normalized_side in {"sell", "ask"}:
            ask = touched_price if parsed_size is None or parsed_size > 0 else None
            return self._record_book_top(token_id, bid=None, ask=ask, bid_size=None, ask_size=parsed_size, ts=ts)
        return None

    def _parse_event(self, event: dict[str, object], last_update: list[float]) -> list[BookTop]:
        tops: list[BookTop] = []
        event_type = self._event_type(event)
        token_id = str(event.get("asset_id") or event.get("token_id") or "")

        if event_type == "tick_size_change":
            self._update_token_constraints(token_id, tick_size=event.get("new_tick_size", event.get("tick_size")))
            return tops

        if event_type in {"book", "snapshot", "book_snapshot", "price_snapshot"}:
            bids, asks = self._extract_book_levels(event)
            bids_levels = self._parse_levels(bids, max_levels=self.book_depth_levels)
            asks_levels = self._parse_levels(asks, max_levels=self.book_depth_levels)
            bid, bid_size = self._extract_price_size(bids)
            ask, ask_size = self._extract_price_size(asks)
            if bid is None and ask is None:
                self._drop_message("snapshot_missing_top_of_book", event_type=event_type, event=event)
                return tops
            fill_prob = event.get("fill_prob")
            if fill_prob is not None:
                fill_prob = float(fill_prob)
            ts = normalize_ts(event.get("timestamp", time.time()))
            last_update[0] = time.time()
            top = self._record_book_top(token_id, bid=bid, ask=ask, bid_size=bid_size, ask_size=ask_size, ts=ts)
            top.fill_prob = fill_prob
            tops.append(top)
            return tops

        if event_type in {"price_change", "update", "book_update", "price_update"}:
            price_changes = event.get("price_changes")
            if isinstance(price_changes, dict):
                price_changes = [price_changes]
            if isinstance(price_changes, list):
                outer_ts = event.get("timestamp", time.time())
                for change in price_changes:
                    if not isinstance(change, dict):
                        self._drop_message("change_not_object", event_type=event_type, event=event)
                        continue

                    change_token = str(change.get("asset_id") or change.get("token_id") or token_id)
                    if not change_token:
                        self._drop_message("change_missing_token", event_type=event_type, event=change)
                        continue

                    bid = self._coerce_positive_float(change.get("best_bid"))
                    ask = self._coerce_positive_float(change.get("best_ask"))
                    bid_size = self._coerce_positive_float(change.get("best_bid_size"))
                    ask_size = self._coerce_positive_float(change.get("best_ask_size"))
                    ts = normalize_ts(change.get("timestamp", outer_ts))
                    tops.append(
                        self._record_book_top(
                            change_token,
                            bid=bid,
                            ask=ask,
                            bid_size=bid_size,
                            ask_size=ask_size,
                            ts=ts,
                        )
                    )
                    CLOB_PRICE_CHANGE_PARSED.labels(schema="new").inc()
                    last_update[0] = time.time()
                return tops

            changes = event.get("changes")
            if isinstance(changes, dict):
                changes = [changes]
            if changes is None:
                changes = [event]
            if not isinstance(changes, list):
                self._drop_message("update_changes_not_list", event_type=event_type, event=event)
                return tops

            outer_ts = event.get("timestamp", time.time())
            for change in changes:
                if not isinstance(change, dict):
                    self._drop_message("change_not_object", event_type=event_type, event=event)
                    continue

                change_token = str(change.get("asset_id") or change.get("token_id") or token_id)
                bid = self._coerce_positive_float(change.get("best_bid"))
                ask = self._coerce_positive_float(change.get("best_ask"))
                bid_size = self._coerce_positive_float(change.get("best_bid_size"))
                ask_size = self._coerce_positive_float(change.get("best_ask_size"))

                change_bids_levels: list[tuple[float, float]] | None = None
                change_asks_levels: list[tuple[float, float]] | None = None
                if bid is None and ask is None:
                    change_bids, change_asks = self._extract_book_levels(change)
                    change_bids_levels = self._parse_levels(change_bids, max_levels=self.book_depth_levels)
                    change_asks_levels = self._parse_levels(change_asks, max_levels=self.book_depth_levels)
                    bid, bid_size = self._extract_price_size(change_bids)
                    ask, ask_size = self._extract_price_size(change_asks)

                ts = normalize_ts(change.get("timestamp", outer_ts))
                last_update[0] = time.time()
                CLOB_PRICE_CHANGE_PARSED.labels(schema="legacy").inc()

                if bid is None and ask is None:
                    top = self._apply_legacy_level_update(
                        change_token,
                        side=change.get("side"),
                        price=change.get("price"),
                        size=change.get("size"),
                        ts=ts,
                    )
                    if top is not None:
                        tops.append(top)
                    continue

                tops.append(
                    self._record_book_top(
                        change_token,
                        bid=bid,
                        ask=ask,
                        bid_size=bid_size,
                        ask_size=ask_size,
                        ts=ts,
                    )
                )
            return tops

        self._drop_message("unrecognized_event_type", event_type=event_type, event=event)
        return tops

    def _parse_raw_message(self, raw: str | bytes, last_update: list[float]) -> list[BookTop]:
        try:
            data = orjson.loads(raw)
        except orjson.JSONDecodeError:
            self._drop_message("invalid_json", event_type="invalid_json")
            return []

        if isinstance(data, list):
            events = [e for e in data if isinstance(e, dict)]
            if not events:
                self._drop_message("message_without_event_objects", event_type="batch")
                return []
        elif isinstance(data, dict):
            events = [data]
        else:
            self._drop_message("message_not_object_or_array", event_type=type(data).__name__)
            return []

        tops: list[BookTop] = []
        for event in events:
            tops.extend(self._parse_event(event, last_update))
        return tops

    def _build_subscription_payload(self, token_ids: set[str]) -> bytes:
        token_set = frozenset(token_ids)
        if token_set != self._subscription_cache_token_ids:
            self._subscription_cache_payload = orjson.dumps({"assets_ids": sorted(token_set), "type": "market"})
            self._subscription_cache_token_ids = token_set
        return self._subscription_cache_payload if self._subscription_cache_payload is not None else b""


    async def stream_books(self, token_ids: list[str]) -> AsyncIterator[BookTop]:
        backoff = self.reconnect_delay_min

        while True:
            failed_pings = [0]
            try:
                async with websockets.connect(self.ws_url, ping_interval=None, ping_timeout=None) as ws:
                    self._ws = ws
                    self._subscribed_token_ids = set(token_ids)
                    sub = {"assets_ids": token_ids, "type": "market"}
                    await ws.send(orjson.dumps(sub))
                    hb_task = asyncio.create_task(self._heartbeat(ws, failed_pings))
                    last_update = [time.time()]
                    stale_task = asyncio.create_task(self._stale_watchdog(last_update, token_ids, "book"))

                    try:
                        while True:
                            raw = await ws.recv() if hasattr(ws, "recv") else await ws.__anext__()
                            for top in self._parse_raw_message(raw, last_update):
                                yield top

                            backoff = self.reconnect_delay_min
                    finally:
                        hb_task.cancel()
                        stale_task.cancel()
                        with contextlib.suppress(Exception):
                            await hb_task
                        with contextlib.suppress(Exception):
                            await stale_task
            except Exception as exc:
                logger.warning("clob_reconnect", error=str(exc), delay_seconds=backoff)
                await asyncio.sleep(backoff)
                backoff = min(self.reconnect_delay_max, backoff * 2)
            finally:
                self._ws = None
                self._subscribed_token_ids = set()
