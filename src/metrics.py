from __future__ import annotations

try:
    from prometheus_client import Counter, Gauge, start_http_server
except Exception:  # pragma: no cover
    class _NoopMetric:
        def inc(self, *_args, **_kwargs):
            return None

        def set(self, *_args, **_kwargs):
            return None

        def labels(self, *_args, **_kwargs):
            return self

    def Counter(*_args, **_kwargs):
        return _NoopMetric()

    def Gauge(*_args, **_kwargs):
        return _NoopMetric()

    def start_http_server(*_args, **_kwargs):
        return None

WATCH_EVENTS = Counter("bot_watch_events_total", "Number of watch triggers")
WATCH_TRIGGERED = Counter("bot_watch_triggered_total", "Number of watch mode trigger events")
HAMMER_ATTEMPTED = Counter("bot_hammer_attempted_total", "Number of hammer order attempts")
HAMMER_FILLED = Counter("bot_hammer_filled_total", "Number of hammer order fills")
REJECTED_MAX_ENTRY_PRICE = Counter(
    "bot_rejected_max_entry_price_total",
    "Number of candidates rejected due to max entry price guardrail",
)
STALE_FEED = Counter("bot_stale_feed_total", "Number of stale feed/staleness events detected")
TRADES = Counter("bot_trades_total", "Trades placed", ["status", "side", "horizon"])
CLOB_DROPPED_MESSAGES = Counter(
    "bot_clob_dropped_messages_total",
    "Number of CLOB websocket payloads dropped during parsing",
    ["reason", "event_type"],
)
CURRENT_EV = Gauge("bot_current_best_ev", "Best EV at decision point")
DAILY_REALIZED_PNL = Gauge("bot_daily_realized_pnl_usd", "Daily realized PnL in USD")
RISK_LIMIT_BLOCKED = Gauge("bot_risk_limit_blocked", "1 if trading is blocked by risk limits")
KILL_SWITCH_ACTIVE = Gauge("bot_kill_switch_active", "1 if divergence kill-switch is active")
ORACLE_SPOT_DIVERGENCE_PCT = Gauge(
    "oracle_spot_divergence_pct",
    "Absolute percentage divergence between chainlink and spot median",
)
FEED_LAG_SECONDS = Gauge(
    "feed_lag_seconds",
    "Lag between now and latest payload timestamp by feed",
    ["feed"],
)
TRADING_ALLOWED = Gauge("trading_allowed", "1 when quorum health permits trading")


def start_metrics_server(host: str, port: int) -> None:
    start_http_server(port=port, addr=host)
