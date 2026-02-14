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

BOT_API_CREDS_AGE_SECONDS = Gauge("bot_api_creds_age_seconds", "Age of currently active CLOB API credentials")
BOT_FEE_FETCH_FAILURES_TOTAL = Counter("bot_fee_fetch_failures_total", "Number of fee-rate fetch failures")
BOT_FEE_RATE_BPS = Gauge("bot_fee_rate_bps", "Latest fee rate in bps by token", ["token_id"])
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
CLOB_PRICE_CHANGE_PARSED = Counter(
    "clob_price_change_parsed_total",
    "Number of CLOB price_change updates successfully parsed",
    ["schema"],
)
CURRENT_EV = Gauge("bot_current_best_ev", "Best EV at decision point")
DAILY_REALIZED_PNL = Gauge("bot_daily_realized_pnl_usd", "Daily realized PnL in USD")
RISK_LIMIT_BLOCKED = Counter("bot_risk_limit_blocked", "Number of risk-limit order blocks")
MAX_DAILY_LOSS_USD_CONFIGURED = Gauge("bot_max_daily_loss_usd_configured", "Configured absolute max daily loss in USD")
MAX_DAILY_LOSS_PCT_CONFIGURED = Gauge("bot_max_daily_loss_pct_configured", "Configured max daily loss as pct of equity")
MAX_OPEN_EXPOSURE_PER_MARKET_USD_CONFIGURED = Gauge(
    "bot_max_open_exposure_per_market_usd_configured",
    "Configured max open exposure per market in USD",
)
MAX_OPEN_EXPOSURE_PER_MARKET_PCT_CONFIGURED = Gauge(
    "bot_max_open_exposure_per_market_pct_configured",
    "Configured max open exposure per market as pct of equity",
)
MAX_TOTAL_OPEN_EXPOSURE_USD_CONFIGURED = Gauge(
    "bot_max_total_open_exposure_usd_configured",
    "Configured max total open exposure in USD",
)
MAX_TOTAL_OPEN_EXPOSURE_PCT_CONFIGURED = Gauge(
    "bot_max_total_open_exposure_pct_configured",
    "Configured max total open exposure as pct of equity",
)
KILL_SWITCH_ACTIVE = Gauge("bot_kill_switch_active", "1 if divergence kill-switch is active")
TRADING_ALLOWED = Gauge("bot_trading_allowed", "1 if trading is currently allowed")
GEOBLOCK_BLOCKED = Gauge(
    "bot_geoblock_blocked",
    "1 if the current deployment IP is geoblocked for trading",
    ["country", "region"],
)
ORACLE_SPOT_DIVERGENCE_PCT = Gauge(
    "bot_oracle_spot_divergence_pct",
    "Percent divergence between oracle and spot quorum consensus",
)
FEED_LAG_SECONDS = Gauge("feed_lag_seconds", "Current feed lag in seconds", ["feed"])
FEED_BLOCKED_STALE_PRICE = Gauge("feed_blocked_stale_price", "1 if stale price feed is currently blocking trading")
FEED_MODE = Gauge("bot_feed_mode", "Current execution-authoritative feed mode", ["mode"])
RECOVERY_STABILIZATION_ACTIVE = Gauge(
    "bot_recovery_stabilization_active",
    "1 while waiting for RTDS freshness stabilization before live trading resumes",
)
BOT_API_CREDS_AGE_SECONDS = Gauge("bot_api_creds_age_seconds", "Age of active API credentials in seconds")

HEARTBEAT_SEND_ATTEMPTS = Counter("bot_heartbeat_send_attempts_total", "Number of heartbeat send attempts")
HEARTBEAT_SEND_SUCCESS = Counter("bot_heartbeat_send_success_total", "Number of successful heartbeat sends")
HEARTBEAT_SEND_FAILURE = Counter("bot_heartbeat_send_failure_total", "Number of failed heartbeat sends")
HEARTBEAT_CONSECUTIVE_MISSES = Gauge("bot_heartbeat_consecutive_misses", "Current consecutive heartbeat misses")
HEARTBEAT_CANCEL_ACTIONS = Counter(
    "bot_heartbeat_cancel_actions_total",
    "Cancel actions triggered after heartbeat failures",
    ["status"],
)
BOT_FEE_FETCH_FAILURES_TOTAL = Counter("bot_fee_fetch_failures_total", "Fee-rate fetch failures")
BOT_FEE_RATE_BPS = Gauge("bot_fee_rate_bps", "Resolved fee rate bps by token", ["token_id"])


def start_metrics_server(host: str, port: int) -> None:
    start_http_server(port=port, addr=host)
