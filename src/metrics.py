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
TRADES = Counter("bot_trades_total", "Trades placed", ["status", "side", "horizon"])
CURRENT_EV = Gauge("bot_current_best_ev", "Best EV at decision point")


def start_metrics_server(host: str, port: int) -> None:
    start_http_server(port=port, addr=host)
