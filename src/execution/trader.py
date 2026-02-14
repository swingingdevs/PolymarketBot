from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from config import Settings
from metrics import DAILY_REALIZED_PNL, RISK_LIMIT_BLOCKED, TRADES
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
    last_pnl_reset_date_utc: str = ""


class Trader:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = None
        self._risk_state_path = Path(settings.risk_state_path)
        self.risk = self._load_risk_state()
        self._reset_daily_pnl_if_needed()
        self._update_risk_metrics(risk_blocked=False)

        if not settings.dry_run and ClobClient is not None:
            self.client = ClobClient(
                host=settings.clob_host,
                chain_id=settings.chain_id,
                key=settings.private_key,
            )

    def _load_risk_state(self) -> RiskState:
        if not self._risk_state_path.exists():
            return RiskState()
        try:
            with self._risk_state_path.open("r", encoding="utf-8") as fp:
                payload = json.load(fp)
        except (OSError, ValueError, TypeError):
            logger.warning("risk_state_load_failed", path=str(self._risk_state_path))
            return RiskState()

        state = RiskState(
            daily_realized_pnl=float(payload.get("daily_realized_pnl", 0.0)),
            trades_this_hour=int(payload.get("trades_this_hour", 0)),
            last_trade_hour=int(payload.get("last_trade_hour", -1)),
            last_pnl_reset_date_utc=str(payload.get("last_pnl_reset_date_utc", "")),
        )
        logger.info("risk_state_loaded", path=str(self._risk_state_path), risk_state=asdict(state))
        return state

    def _persist_risk_state(self) -> None:
        try:
            self._risk_state_path.parent.mkdir(parents=True, exist_ok=True)
            with self._risk_state_path.open("w", encoding="utf-8") as fp:
                json.dump(asdict(self.risk), fp)
        except OSError:
            logger.warning("risk_state_persist_failed", path=str(self._risk_state_path))

    def _today_utc(self) -> str:
        return datetime.now(timezone.utc).date().isoformat()

    def _reset_daily_pnl_if_needed(self) -> None:
        today_utc = self._today_utc()
        if self.risk.last_pnl_reset_date_utc == today_utc:
            return
        self.risk.daily_realized_pnl = 0.0
        self.risk.last_pnl_reset_date_utc = today_utc
        self._persist_risk_state()
        logger.info("risk_daily_pnl_reset", reset_date_utc=today_utc)

    def _risk_blocked(self, notional_usd: float) -> bool:
        if notional_usd > self.settings.max_usd_per_trade:
            return True
        if self.risk.daily_realized_pnl <= -abs(self.settings.max_daily_loss):
            return True
        if self.risk.trades_this_hour >= self.settings.max_trades_per_hour:
            return True
        return False

    def _update_risk_metrics(self, risk_blocked: bool) -> None:
        DAILY_REALIZED_PNL.set(self.risk.daily_realized_pnl)
        RISK_LIMIT_BLOCKED.set(1 if risk_blocked else 0)

    def _check_risk(self, notional_usd: float) -> bool:
        self._reset_daily_pnl_if_needed()
        now_hour = datetime.now(timezone.utc).hour
        if now_hour != self.risk.last_trade_hour:
            self.risk.last_trade_hour = now_hour
            self.risk.trades_this_hour = 0
            self._persist_risk_state()

        blocked = self._risk_blocked(notional_usd)
        self._update_risk_metrics(risk_blocked=blocked)
        return not blocked

    def _extract_realized_pnl(self, payload: Any) -> float:
        if isinstance(payload, list):
            return sum(self._extract_realized_pnl(item) for item in payload)
        if not isinstance(payload, dict):
            return 0.0

        pnl = 0.0
        pnl_keys = {
            "realized_pnl",
            "realizedPnl",
            "pnl",
            "settlement_pnl",
            "settlementPnl",
        }
        for key in pnl_keys:
            value = payload.get(key)
            if value is None:
                continue
            try:
                pnl += float(value)
            except (TypeError, ValueError):
                continue

        pnl += self._extract_realized_pnl(payload.get("fills"))
        pnl += self._extract_realized_pnl(payload.get("settlements"))
        return pnl

    def _record_post_trade_updates(self, resp: Any) -> None:
        self._reset_daily_pnl_if_needed()
        realized_pnl = self._extract_realized_pnl(resp)
        if realized_pnl:
            self.risk.daily_realized_pnl += realized_pnl

        self._persist_risk_state()
        self._update_risk_metrics(risk_blocked=self._risk_blocked(0.0))

    @staticmethod
    def _classify_submit_exception(exc: Exception) -> str:
        text = str(exc).lower()
        name = exc.__class__.__name__.lower()

        if isinstance(exc, (TimeoutError, asyncio.TimeoutError)) or "timeout" in text or "timeout" in name:
            return "timeout"
        if any(k in text for k in ("401", "403", "unauthorized", "forbidden", "invalid api", "auth")):
            return "auth"
        if any(k in name for k in ("connection", "network", "socket")):
            return "network"
        if any(k in text for k in ("connection", "network", "dns", "socket", "refused", "unreachable")):
            return "network"
        return "error"

    async def buy_fok(self, token_id: str, ask: float, horizon: str) -> bool:
        size = self.settings.quote_size_usd / ask
        size = round_size_to_step(size, 0.1)
        px = round_price_to_tick(ask, 0.001)

        notional = size * px
        if not self._check_risk(notional):
            logger.info(
                "risk_reject",
                token_id=token_id,
                ask=ask,
                size=size,
                daily_realized_pnl=self.risk.daily_realized_pnl,
                max_daily_loss=self.settings.max_daily_loss,
                trades_this_hour=self.risk.trades_this_hour,
            )
            TRADES.labels(status="rejected", side="buy", horizon=horizon).inc()
            return False

        if self.settings.dry_run:
            logger.info("dry_run_order", token_id=token_id, ask=px, size=size)
            self.risk.trades_this_hour += 1
            self._persist_risk_state()
            self._update_risk_metrics(risk_blocked=self._risk_blocked(0.0))
            TRADES.labels(status="dry_run", side="buy", horizon=horizon).inc()
            return True

        if self.client is None or OrderArgs is None:
            logger.error("missing_clob_client")
            TRADES.labels(status="error", side="buy", horizon=horizon).inc()
            return False

        args = OrderArgs(price=px, size=size, side="BUY", token_id=token_id, time_in_force="FOK")
        try:
            resp = await asyncio.wait_for(
                asyncio.to_thread(self.client.create_and_post_order, args),
                timeout=self.settings.order_submit_timeout_seconds,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "order_submit_timeout",
                token_id=token_id,
                ask=px,
                size=size,
                timeout_seconds=self.settings.order_submit_timeout_seconds,
            )
            TRADES.labels(status="timeout", side="buy", horizon=horizon).inc()
            return False
        except Exception as exc:
            failure_type = self._classify_submit_exception(exc)
            logger.warning(
                "order_submit_failed",
                token_id=token_id,
                ask=px,
                size=size,
                failure_type=failure_type,
                error=str(exc),
            )
            TRADES.labels(status=failure_type, side="buy", horizon=horizon).inc()
            return False

        ok = bool(resp)
        if ok:
            self.risk.trades_this_hour += 1
            self._record_post_trade_updates(resp)
            TRADES.labels(status="filled", side="buy", horizon=horizon).inc()
        else:
            TRADES.labels(status="rejected", side="buy", horizon=horizon).inc()
        logger.info(
            "order_result",
            ok=ok,
            response=resp,
            daily_realized_pnl=self.risk.daily_realized_pnl,
            risk_limit_blocked=self._risk_blocked(0.0),
        )
        return ok
