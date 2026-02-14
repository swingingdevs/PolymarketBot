from __future__ import annotations

import asyncio
import csv
import math
import os
import queue
import statistics
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from feeds.clob_ws import CLOBWebSocket
from feeds.rtds import RTDSFeed

st.set_page_config(layout="wide", page_title="Polymarket Bot Ops Dashboard")


@dataclass(slots=True)
class DashboardEvent:
    ts: float
    kind: str
    payload: dict[str, object]


def _default_state() -> None:
    if "event_queue" not in st.session_state:
        st.session_state.event_queue = queue.Queue()
    if "events" not in st.session_state:
        st.session_state.events = deque(maxlen=5000)
    if "rtds_prices" not in st.session_state:
        st.session_state.rtds_prices = deque(maxlen=1500)
    if "divergence" not in st.session_state:
        st.session_state.divergence = deque(maxlen=1500)
    if "clob_books" not in st.session_state:
        st.session_state.clob_books = {}
    if "orders" not in st.session_state:
        st.session_state.orders = deque(maxlen=300)
    if "fills" not in st.session_state:
        st.session_state.fills = deque(maxlen=300)
    if "worker_started" not in st.session_state:
        st.session_state.worker_started = False
    if "worker_status" not in st.session_state:
        st.session_state.worker_status = "idle"


def _compute_zscore(values: list[float], lookback: int = 60) -> float | None:
    if len(values) < max(3, lookback // 3):
        return None
    window = values[-lookback:]
    mean = statistics.fmean(window)
    std = statistics.pstdev(window)
    if std <= 1e-12:
        return 0.0
    return (window[-1] - mean) / std


def _estimate_ev(zscore: float | None, best_ask: float | None, fee_bps: float) -> float | None:
    if zscore is None or best_ask is None:
        return None
    p_hat = 0.5 * (1.0 + math.erf(zscore / math.sqrt(2.0)))
    fee = fee_bps / 10_000.0
    return p_hat - best_ask - fee


async def _rtds_task(cfg: dict[str, object], out_q: queue.Queue[DashboardEvent]) -> None:
    feed = RTDSFeed(
        ws_url=str(cfg["rtds_url"]),
        symbol=str(cfg["symbol"]),
        ping_interval=int(cfg["ping_interval"]),
        pong_timeout=int(cfg["pong_timeout"]),
        price_staleness_threshold=int(cfg["price_staleness"]),
    )
    async for ts, price, metadata in feed.stream_prices():
        out_q.put(
            DashboardEvent(
                ts=ts,
                kind="rtds",
                payload={"price": price, "metadata": metadata},
            )
        )


async def _clob_task(cfg: dict[str, object], out_q: queue.Queue[DashboardEvent]) -> None:
    token_ids = [t.strip() for t in str(cfg["token_ids"]).split(",") if t.strip()]
    if not token_ids:
        return
    ws = CLOBWebSocket(
        ws_base=str(cfg["clob_url"]),
        ping_interval=int(cfg["ping_interval"]),
        pong_timeout=int(cfg["pong_timeout"]),
    )
    async for top in ws.stream_books(token_ids):
        out_q.put(
            DashboardEvent(
                ts=top.ts,
                kind="book",
                payload={
                    "token_id": top.token_id,
                    "best_bid": top.best_bid,
                    "best_ask": top.best_ask,
                    "best_bid_size": top.best_bid_size,
                    "best_ask_size": top.best_ask_size,
                },
            )
        )


async def _runner(cfg: dict[str, object], out_q: queue.Queue[DashboardEvent]) -> None:
    await asyncio.gather(_rtds_task(cfg, out_q), _clob_task(cfg, out_q))


def _start_worker(cfg: dict[str, object]) -> None:
    out_q = st.session_state.event_queue

    def _thread_main() -> None:
        st.session_state.worker_status = "running"
        try:
            asyncio.run(_runner(cfg, out_q))
        except Exception as exc:  # noqa: BLE001
            out_q.put(DashboardEvent(ts=time.time(), kind="error", payload={"error": str(exc)}))
        finally:
            st.session_state.worker_status = "stopped"

    t = threading.Thread(target=_thread_main, daemon=True)
    t.start()
    st.session_state.worker_started = True


def _drain_queue() -> None:
    q: queue.Queue[DashboardEvent] = st.session_state.event_queue
    while True:
        try:
            event = q.get_nowait()
        except queue.Empty:
            break
        st.session_state.events.append(event)
        if event.kind == "rtds":
            px = float(event.payload["price"])
            md = event.payload["metadata"]
            st.session_state.rtds_prices.append((event.ts, px))
            if isinstance(md, dict) and "divergence_pct" in md:
                st.session_state.divergence.append((event.ts, float(md["divergence_pct"])))
        elif event.kind == "book":
            token = str(event.payload["token_id"])
            st.session_state.clob_books[token] = {
                "ts": event.ts,
                "bid": event.payload.get("best_bid"),
                "ask": event.payload.get("best_ask"),
                "bid_size": event.payload.get("best_bid_size"),
                "ask_size": event.payload.get("best_ask_size"),
            }


def _load_replay_csv(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(
                {
                    "ts": float(row["ts"]),
                    "event": row["event"].lower().strip(),
                    "price": float(row["price"]) if row.get("price") else None,
                    "token_id": row.get("token_id") or None,
                    "bid": float(row["bid"]) if row.get("bid") else None,
                    "ask": float(row["ask"]) if row.get("ask") else None,
                }
            )
    return sorted(rows, key=lambda r: float(r["ts"]))


def _render_replay_panel(replay_dir: Path) -> None:
    st.subheader("Replay session selector & deterministic playback")
    sessions = sorted(replay_dir.glob("*.csv")) if replay_dir.exists() else []
    if not sessions:
        st.info("No replay CSV sessions found. Put files in data/replay_sessions/*.csv")
        return

    selected = st.selectbox("Session", sessions, format_func=lambda p: p.name)
    rows = _load_replay_csv(selected)
    if not rows:
        st.warning("Selected replay is empty")
        return

    max_idx = len(rows) - 1
    step = st.slider("Playback position", min_value=0, max_value=max_idx, value=min(max_idx, 300), step=1)
    visible = rows[: step + 1]

    replay_prices = [r for r in visible if r["event"] == "price" and r["price"] is not None]
    replay_books = [r for r in visible if r["event"] == "book"]

    c1, c2 = st.columns(2)
    with c1:
        st.caption("Replay price path")
        st.line_chart({"price": [float(r["price"]) for r in replay_prices]})
    with c2:
        st.caption("Replay top-of-book ask path")
        st.line_chart({"ask": [float(r["ask"]) for r in replay_books if r["ask"] is not None]})

    st.write(
        {
            "rows_visible": len(visible),
            "last_ts": visible[-1]["ts"],
            "last_event": visible[-1]["event"],
        }
    )


def main() -> None:
    _default_state()
    st.title("Polymarket Bot Dashboard")

    with st.sidebar:
        st.header("Runtime controls")
        mode = st.radio("Mode", ["DRY_RUN", "LIVE"], index=0)
        watch_threshold = st.number_input("Watch threshold", value=0.005, min_value=0.0001, max_value=0.05, step=0.0001)
        d_min = st.number_input("D_MIN", value=5.0, min_value=0.1, max_value=100.0, step=0.1)
        max_entry = st.number_input("Max entry price", value=0.97, min_value=0.01, max_value=1.0, step=0.01)
        fee_bps = st.number_input("Fee bps", value=10.0, min_value=0.0, max_value=100.0, step=1.0)

        st.header("Kill-switch & ping")
        kill_global = st.toggle("Global kill-switch", value=False)
        kill_long = st.toggle("Disable UP entries", value=False)
        kill_short = st.toggle("Disable DOWN entries", value=False)
        ping_interval = st.number_input("Ping interval (s)", value=20, min_value=5, max_value=120)
        pong_timeout = st.number_input("Pong timeout (s)", value=10, min_value=2, max_value=60)

        st.header("Feeds")
        rtds_url = st.text_input("RTDS URL", value=os.getenv("RTDS_WS_URL", "wss://ws-live-data.polymarket.com"))
        clob_url = st.text_input(
            "CLOB URL", value=os.getenv("CLOB_WS_BASE", "wss://ws-subscriptions-clob.polymarket.com")
        )
        symbol = st.text_input("Symbol", value=os.getenv("SYMBOL", "btc/usd"))
        token_ids = st.text_area("Token IDs (comma separated)", value="")
        price_staleness = st.number_input("Price staleness threshold (s)", value=10, min_value=1, max_value=300)

        start_ingest = st.button("Start websocket ingest")

    if start_ingest and not st.session_state.worker_started:
        cfg = {
            "rtds_url": rtds_url,
            "clob_url": clob_url,
            "symbol": symbol,
            "token_ids": token_ids,
            "ping_interval": ping_interval,
            "pong_timeout": pong_timeout,
            "price_staleness": price_staleness,
        }
        _start_worker(cfg)

    _drain_queue()

    prices = [p for _, p in st.session_state.rtds_prices]
    price_times = [ts for ts, _ in st.session_state.rtds_prices]
    divergence = [d for _, d in st.session_state.divergence]

    zscore = _compute_zscore(prices, lookback=60)
    top_books = st.session_state.clob_books
    representative_ask = None
    if top_books:
        first = next(iter(top_books.values()))
        ask = first.get("ask")
        representative_ask = float(ask) if ask is not None else None
    ev = _estimate_ev(zscore, representative_ask, fee_bps)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.subheader("RTDS / Spot divergence")
        st.metric("Latest RTDS", f"{prices[-1]:.2f}" if prices else "n/a")
        st.metric("Latest divergence %", f"{divergence[-1]:.4f}" if divergence else "n/a")
        st.line_chart({"divergence_pct": divergence[-200:]})

    with col2:
        st.subheader("Staleness")
        now = time.time()
        staleness = (now - price_times[-1]) if price_times else None
        st.metric("Price staleness (s)", f"{staleness:.2f}" if staleness is not None else "n/a")
        if staleness is not None and staleness > float(price_staleness):
            st.error("RTDS feed appears stale")
        else:
            st.success("RTDS feed healthy")

    with col3:
        st.subheader("Kill-switch state")
        st.write(
            {
                "mode": mode,
                "global_kill": kill_global,
                "disable_up": kill_long,
                "disable_down": kill_short,
                "watch_threshold": watch_threshold,
                "d_min": d_min,
                "max_entry_price": max_entry,
            }
        )

    c4, c5 = st.columns(2)
    with c4:
        st.subheader("CLOB top-of-book / depth")
        if not top_books:
            st.info("No book updates yet")
        else:
            rows = []
            for token_id, top in top_books.items():
                rows.append(
                    {
                        "token_id": token_id,
                        "bid": top.get("bid"),
                        "ask": top.get("ask"),
                        "bid_size": top.get("bid_size"),
                        "ask_size": top.get("ask_size"),
                        "depth": (top.get("bid_size") or 0) + (top.get("ask_size") or 0),
                        "last_update": top.get("ts"),
                    }
                )
            st.dataframe(rows, use_container_width=True)

    with c5:
        st.subheader("Z-score / EV trend")
        z_hist = []
        ev_hist = []
        for i in range(max(1, len(prices) - 240), len(prices)):
            local_z = _compute_zscore(prices[: i + 1], lookback=60)
            z_hist.append(0.0 if local_z is None else local_z)
            ev_hist.append(_estimate_ev(local_z, representative_ask, fee_bps) or 0.0)
        st.metric("Current z-score", f"{zscore:.3f}" if zscore is not None else "n/a")
        st.metric("Current EV", f"{ev:.4f}" if ev is not None else "n/a")
        st.line_chart({"z_score": z_hist, "ev": ev_hist})

    st.subheader("Exposures / orders / fills")
    gross_exposure = len(st.session_state.orders) * 20.0
    net_exposure = gross_exposure - len(st.session_state.fills) * 20.0
    e1, e2, e3 = st.columns(3)
    e1.metric("Open orders", str(len(st.session_state.orders)))
    e2.metric("Fills", str(len(st.session_state.fills)))
    e3.metric("Net exposure (USD)", f"{net_exposure:.2f}")

    with st.expander("Recent raw events"):
        recent = list(st.session_state.events)[-100:]
        st.write([{"ts": e.ts, "kind": e.kind, "payload": e.payload} for e in recent])

    replay_dir = Path("data/replay_sessions")
    _render_replay_panel(replay_dir)


if __name__ == "__main__":
    main()
