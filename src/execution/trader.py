from __future__ import annotations

import json
import asyncio
import math
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from config import Settings
from markets.token_metadata_cache import TokenMetadataCache
from metrics import DAILY_REALIZED_PNL, RISK_LIMIT_BLOCKED, TRADES
from utils.rounding import round_price_up_to_tick, round_size_to_step

logger = structlog.get_logger(__name__)

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds
    from py_clob_client.clob_types import OrderArgs
except Exception:  # pragma: no cover
    ClobClient = None
    ApiCreds = None
    OrderArgs = None


@dataclass(slots=True)
class RiskState:
    daily_realized_pnl: float = 0.0
    trades_this_hour: int = 0
    last_trade_hour: int = -1
    last_pnl_reset_date_utc: str = ""
    open_exposure_usd_by_market: dict[str, float] | None = None
    total_open_notional_usd: float = 0.0
    trades_since_reconcile: int = 0

    def __post_init__(self) -> None:
        if self.open_exposure_usd_by_market is None:
            self.open_exposure_usd_by_market = {}


@dataclass(slots=True)
class TokenConstraints:
    min_order_size: float | None = None
    tick_size: float | None = None


class Trader:
    def __init__(self, settings: Settings, token_metadata_cache: TokenMetadataCache | None = None) -> None:
        self.settings = settings
        self.token_metadata_cache = token_metadata_cache
        self.client = None
        self._risk_state_path = Path(settings.risk_state_path)
        self.risk = self._load_risk_state()
        self.token_constraints_by_id: dict[str, TokenConstraints] = {}
        self._reset_daily_pnl_if_needed()
        self._update_risk_metrics(risk_blocked=False)
        self._live_auth_ready = False

        if not settings.dry_run and ClobClient is not None:
            self.client = ClobClient(
                host=settings.clob_host,
                chain_id=settings.chain_id,
                key=settings.private_key,
            )
            self._live_auth_ready = self._initialize_live_auth()

    def _initialize_live_auth(self) -> bool:
        if self.client is None:
            logger.error("missing_clob_client")
            return False

        if not (self.settings.api_key and self.settings.api_secret and self.settings.api_passphrase):
            logger.error(
                "missing_clob_l2_credentials",
                has_api_key=bool(self.settings.api_key),
                has_api_secret=bool(self.settings.api_secret),
                has_api_passphrase=bool(self.settings.api_passphrase),
            )
            return False

        try:
            if hasattr(self.client, "set_api_creds"):
                creds_payload: Any
                if ApiCreds is not None:
                    creds_payload = ApiCreds(
                        api_key=self.settings.api_key,
                        api_secret=self.settings.api_secret,
                        api_passphrase=self.settings.api_passphrase,
                    )
                else:
                    creds_payload = {
                        "api_key": self.settings.api_key,
                        "api_secret": self.settings.api_secret,
                        "api_passphrase": self.settings.api_passphrase,
                    }
                self.client.set_api_creds(creds_payload)
            elif hasattr(self.client, "create_or_derive_api_creds"):
                self.client.create_or_derive_api_creds()
            elif hasattr(self.client, "derive_api_key"):
                self.client.derive_api_key()

            if hasattr(self.client, "assert_level_2_auth"):
                self.client.assert_level_2_auth()
        except Exception as exc:
            logger.error("clob_l2_auth_initialization_failed", error=str(exc))
            return False

        logger.info("clob_l2_auth_initialized")
        return True

    def _load_risk_state(self) -> RiskState:
        if not self._risk_state_path.exists():
            return RiskState()
        try:
            with self._risk_state_path.open("r", encoding="utf-8") as fp:
                payload = json.load(fp)
        except (OSError, ValueError, TypeError):
            logger.warning("risk_state_load_failed", path=str(self._risk_state_path))
            return RiskState()

        open_exposure = payload.get("open_exposure_usd_by_market")
        if not isinstance(open_exposure, dict):
            open_exposure = self._migrate_open_exposure_payload(payload.get("open_exposure"))

        state = RiskState(
            daily_realized_pnl=float(payload.get("daily_realized_pnl", 0.0)),
            trades_this_hour=int(payload.get("trades_this_hour", 0)),
            last_trade_hour=int(payload.get("last_trade_hour", -1)),
            last_pnl_reset_date_utc=str(payload.get("last_pnl_reset_date_utc", "")),
            open_exposure_usd_by_market=self._normalize_open_exposure(open_exposure),
            total_open_notional_usd=float(payload.get("total_open_notional_usd", 0.0)),
            trades_since_reconcile=int(payload.get("trades_since_reconcile", 0)),
        )
        if state.total_open_notional_usd <= 0 and state.open_exposure_usd_by_market:
            state.total_open_notional_usd = sum(state.open_exposure_usd_by_market.values())
        logger.info("risk_state_loaded", path=str(self._risk_state_path), risk_state=asdict(state))
        return state

    def _normalize_open_exposure(self, raw: Any) -> dict[str, float]:
        if not isinstance(raw, dict):
            return {}

        normalized: dict[str, float] = {}
        for key, value in raw.items():
            if not isinstance(key, str):
                continue
            try:
                normalized[key] = max(0.0, float(value))
            except (TypeError, ValueError):
                continue
        return normalized

    def _migrate_open_exposure_payload(self, raw: Any) -> dict[str, float]:
        migrated: dict[str, float] = {}
        if not isinstance(raw, dict):
            return migrated

        for token_id, by_horizon in raw.items():
            if not isinstance(token_id, str) or not isinstance(by_horizon, dict):
                continue
            for horizon, by_direction in by_horizon.items():
                if not isinstance(horizon, str) or not isinstance(by_direction, dict):
                    continue
                for direction, notional in by_direction.items():
                    key = self._market_exposure_key(token_id, horizon, str(direction))
                    try:
                        migrated[key] = max(0.0, float(notional))
                    except (TypeError, ValueError):
                        continue

        return migrated

    def _persist_risk_state(self) -> None:
        try:
            self._risk_state_path.parent.mkdir(parents=True, exist_ok=True)
            with self._risk_state_path.open("w", encoding="utf-8") as fp:
                json.dump(asdict(self.risk), fp)
        except OSError:
            logger.warning("risk_state_persist_failed", path=str(self._risk_state_path))

    def _today_utc(self) -> str:
        return time.strftime("%Y-%m-%d", time.gmtime())

    def _reset_daily_pnl_if_needed(self) -> None:
        today_utc = self._today_utc()
        if self.risk.last_pnl_reset_date_utc == today_utc:
            return
        self.risk.daily_realized_pnl = 0.0
        self.risk.last_pnl_reset_date_utc = today_utc
        self._persist_risk_state()
        logger.info("risk_daily_pnl_reset", reset_date_utc=today_utc)

    @staticmethod
    def _market_exposure_key(token_id: str, horizon: str, direction: str) -> str:
        return f"{token_id}|{horizon}|{direction.upper()}"

    def _risk_blocked(self, notional_usd: float, token_id: str, horizon: str, direction: str) -> bool:
        if notional_usd > self.settings.max_usd_per_trade:
            return True
        if self.risk.daily_realized_pnl <= -abs(self.settings.max_daily_loss):
            return True
        if self.risk.trades_this_hour >= self.settings.max_trades_per_hour:
            return True
        market_key = self._market_exposure_key(token_id, horizon, direction)
        next_market_exposure = self.risk.open_exposure_usd_by_market.get(market_key, 0.0) + notional_usd
        if next_market_exposure > self.settings.max_open_exposure_per_market:
            return True
        if (self.risk.total_open_notional_usd + notional_usd) > self.settings.max_total_open_exposure:
            return True
        return False

    def _update_risk_metrics(self, risk_blocked: bool) -> None:
        DAILY_REALIZED_PNL.set(self.risk.daily_realized_pnl)
        RISK_LIMIT_BLOCKED.set(1 if risk_blocked else 0)

    def _check_risk(self, notional_usd: float, token_id: str, horizon: str, direction: str) -> bool:
        self._reset_daily_pnl_if_needed()
        now_hour = datetime.now(timezone.utc).hour
        if now_hour != self.risk.last_trade_hour:
            self.risk.last_trade_hour = now_hour
            self.risk.trades_this_hour = 0
            self._persist_risk_state()

        blocked = self._risk_blocked(notional_usd, token_id=token_id, horizon=horizon, direction=direction)
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

    def _extract_fill_notional(self, payload: Any) -> tuple[float, bool]:
        if not isinstance(payload, dict):
            return 0.0, False

        fills = payload.get("fills")
        if not isinstance(fills, list) or not fills:
            return 0.0, False

        notional = 0.0
        complete = True
        for fill in fills:
            if not isinstance(fill, dict):
                complete = False
                continue
            px = fill.get("price")
            size = fill.get("size")
            fill_notional = fill.get("notional")
            if fill_notional is not None:
                try:
                    notional += abs(float(fill_notional))
                    continue
                except (TypeError, ValueError):
                    complete = False
            if px is None or size is None:
                complete = False
                continue
            try:
                notional += abs(float(px) * float(size))
            except (TypeError, ValueError):
                complete = False

        return notional, complete

    def _apply_open_exposure_delta(self, token_id: str, horizon: str, direction: str, delta_usd: float) -> None:
        if delta_usd == 0:
            return
        key = self._market_exposure_key(token_id, horizon, direction)
        current = self.risk.open_exposure_usd_by_market.get(key, 0.0)
        updated = max(0.0, current + delta_usd)
        if updated == 0.0:
            self.risk.open_exposure_usd_by_market.pop(key, None)
        else:
            self.risk.open_exposure_usd_by_market[key] = updated
        self.risk.total_open_notional_usd = max(
            0.0,
            sum(self.risk.open_exposure_usd_by_market.values()),
        )

    def _maybe_reconcile_exposure_from_exchange(self, force: bool = False) -> None:
        self.risk.trades_since_reconcile += 1
        if not force and self.risk.trades_since_reconcile < self.settings.exposure_reconcile_every_n_trades:
            return
        if self.client is None:
            return

        for method_name in ("get_open_positions", "get_positions", "get_current_positions"):
            method = getattr(self.client, method_name, None)
            if method is None:
                continue
            try:
                payload = method()
            except Exception:
                continue

            positions = payload if isinstance(payload, list) else payload.get("positions") if isinstance(payload, dict) else []
            if not isinstance(positions, list):
                continue

            rebuilt: dict[str, float] = {}
            for position in positions:
                if not isinstance(position, dict):
                    continue
                token_id = str(position.get("token_id") or position.get("tokenId") or "")
                if not token_id:
                    continue
                horizon = str(position.get("horizon") or "unknown")
                direction = str(position.get("direction") or "BUY")
                try:
                    notional = abs(
                        float(
                            position.get("notional")
                            or position.get("notional_usd")
                            or (float(position.get("price")) * float(position.get("size")))
                        )
                    )
                except (TypeError, ValueError):
                    continue
                rebuilt[self._market_exposure_key(token_id, horizon, direction)] = notional

            self.risk.open_exposure_usd_by_market = rebuilt
            self.risk.total_open_notional_usd = sum(rebuilt.values())
            self.risk.trades_since_reconcile = 0
            return

    def _record_post_trade_updates(
        self,
        resp: Any,
        *,
        token_id: str,
        horizon: str,
        direction: str,
        fallback_notional_usd: float,
    ) -> None:
        self._reset_daily_pnl_if_needed()
        realized_pnl = self._extract_realized_pnl(resp)
        if realized_pnl:
            self.risk.daily_realized_pnl += realized_pnl

        filled_notional, complete_fill_details = self._extract_fill_notional(resp)
        applied_notional = filled_notional if filled_notional > 0 else fallback_notional_usd
        self._apply_open_exposure_delta(token_id, horizon, direction, applied_notional)
        self._maybe_reconcile_exposure_from_exchange(force=not complete_fill_details)

        self._persist_risk_state()
        self._update_risk_metrics(risk_blocked=self._risk_blocked(0.0, token_id=token_id, horizon=horizon, direction=direction))

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

    @staticmethod
    def _round_size_up_to_step(size: float, step_size: float) -> float:
        if step_size <= 0:
            raise ValueError("step_size must be > 0")
        return round(math.ceil(size / step_size) * step_size, 8)

    def update_token_constraints(
        self,
        token_id: str,
        *,
        min_order_size: float | None = None,
        tick_size: float | None = None,
    ) -> None:
        if not token_id:
            return
        current = self.token_constraints_by_id.get(token_id, TokenConstraints())
        if min_order_size is not None and min_order_size > 0:
            current.min_order_size = float(min_order_size)
        if tick_size is not None and tick_size > 0:
            current.tick_size = float(tick_size)
        self.token_constraints_by_id[token_id] = current

    async def buy_fok(self, token_id: str, ask: float, horizon: str) -> bool:
        constraints = self.token_constraints_by_id.get(token_id, TokenConstraints())
        tick_size = constraints.tick_size or 0.001
        size_step = constraints.min_order_size or 0.1
        min_order_size = constraints.min_order_size

        size = self.settings.quote_size_usd / ask
        size = round_size_to_step(size, size_step)
        px = round_price_up_to_tick(ask, tick_size)

        if min_order_size is not None and size < min_order_size:
            adjusted_size = self._round_size_up_to_step(min_order_size, size_step)
            adjusted_notional = adjusted_size * px
            if not self._check_risk(adjusted_notional, token_id=token_id, horizon=horizon, direction="BUY"):
                logger.info(
                    "order_reject_min_order_size",
                    token_id=token_id,
                    ask=ask,
                    min_order_size=min_order_size,
                    computed_size=size,
                    adjusted_size=adjusted_size,
                    adjusted_notional=adjusted_notional,
                )
                TRADES.labels(status="rejected_min_size", side="buy", horizon=horizon).inc()
                return False

            logger.info(
                "order_size_adjusted_to_minimum",
                token_id=token_id,
                ask=ask,
                original_size=size,
                adjusted_size=adjusted_size,
                min_order_size=min_order_size,
            )
            size = adjusted_size

        notional = size * px
        if not self._check_risk(notional, token_id=token_id, horizon=horizon, direction="BUY"):
            logger.info(
                "risk_reject",
                token_id=token_id,
                ask=ask,
                size=size,
                daily_realized_pnl=self.risk.daily_realized_pnl,
                max_daily_loss=self.settings.max_daily_loss,
                trades_this_hour=self.risk.trades_this_hour,
                open_exposure_market=self.risk.open_exposure_usd_by_market.get(
                    self._market_exposure_key(token_id, horizon, "BUY"), 0.0
                ),
                total_open_notional_usd=self.risk.total_open_notional_usd,
            )
            TRADES.labels(status="rejected", side="buy", horizon=horizon).inc()
            return False

        if self.settings.dry_run:
            logger.info("dry_run_order", token_id=token_id, ask=px, size=size)
            self.risk.trades_this_hour += 1
            self._record_post_trade_updates(
                {"fills": [{"price": px, "size": size}]},
                token_id=token_id,
                horizon=horizon,
                direction="BUY",
                fallback_notional_usd=notional,
            )
            TRADES.labels(status="dry_run", side="buy", horizon=horizon).inc()
            return True

        if self.client is None or OrderArgs is None:
            logger.error("missing_clob_client")
            TRADES.labels(status="error", side="buy", horizon=horizon).inc()
            return False

        if not self._live_auth_ready:
            logger.error("missing_clob_l2_credentials")
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
            self._record_post_trade_updates(
                resp,
                token_id=token_id,
                horizon=horizon,
                direction="BUY",
                fallback_notional_usd=notional,
            )
            TRADES.labels(status="filled", side="buy", horizon=horizon).inc()
        else:
            TRADES.labels(status="rejected", side="buy", horizon=horizon).inc()
        logger.info(
            "order_result",
            ok=ok,
            response=resp,
            daily_realized_pnl=self.risk.daily_realized_pnl,
            risk_limit_blocked=self._risk_blocked(0.0, token_id=token_id, horizon=horizon, direction="BUY"),
        )
        return ok
