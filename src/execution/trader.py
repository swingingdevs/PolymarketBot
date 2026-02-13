from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import structlog

from config import Settings
from metrics import TRADES
from utils.rounding import round_price_to_tick, round_size_to_step

logger = structlog.get_logger(__name__)

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import OrderArgs
except Exception:  # pragma: no cover
    ClobClient = None
    OrderArgs = None


@dataclass(slots=True)
class RiskState:
    daily_realized_pnl: float = 0.0
    trades_this_hour: int = 0
    last_trade_hour: int = -1


class Trader:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.risk = RiskState()
        self.client = None
        if not settings.dry_run and ClobClient is not None:
            self.client = ClobClient(
                host=settings.clob_host,
                chain_id=settings.chain_id,
                key=settings.private_key,
            )

    def _check_risk(self, notional_usd: float) -> bool:
        now_hour = datetime.now(UTC).hour
        if now_hour != self.risk.last_trade_hour:
            self.risk.last_trade_hour = now_hour
            self.risk.trades_this_hour = 0

        if notional_usd > self.settings.max_usd_per_trade:
            return False
        if self.risk.daily_realized_pnl <= -abs(self.settings.max_daily_loss):
            return False
        if self.risk.trades_this_hour >= self.settings.max_trades_per_hour:
            return False
        return True

    async def buy_fok(self, token_id: str, ask: float, horizon: str) -> bool:
        size = self.settings.quote_size_usd / ask
        size = round_size_to_step(size, 0.1)
        px = round_price_to_tick(ask, 0.001)

        notional = size * px
        if not self._check_risk(notional):
            logger.info("risk_reject", token_id=token_id, ask=ask, size=size)
            TRADES.labels(status="rejected", side="buy", horizon=horizon).inc()
            return False

        if self.settings.dry_run:
            logger.info("dry_run_order", token_id=token_id, ask=px, size=size)
            self.risk.trades_this_hour += 1
            TRADES.labels(status="dry_run", side="buy", horizon=horizon).inc()
            return True

        if self.client is None or OrderArgs is None:
            logger.error("missing_clob_client")
            TRADES.labels(status="error", side="buy", horizon=horizon).inc()
            return False

        args = OrderArgs(price=px, size=size, side="BUY", token_id=token_id, time_in_force="FOK")
        resp = self.client.create_and_post_order(args)
        ok = bool(resp)
        if ok:
            self.risk.trades_this_hour += 1
            TRADES.labels(status="filled", side="buy", horizon=horizon).inc()
        else:
            TRADES.labels(status="rejected", side="buy", horizon=horizon).inc()
        logger.info("order_result", ok=ok, response=resp)
        return ok
