from __future__ import annotations

import asyncio
import math
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import orjson
import structlog

from config import Settings
from markets.token_metadata_cache import TokenMetadataCache
from auth.credentials import CredentialValidationError, init_client
from metrics import BOT_API_CREDS_AGE_SECONDS, DAILY_REALIZED_PNL, RISK_LIMIT_BLOCKED, TRADES
from utils.rounding import round_price_up_to_tick, round_size_to_step

logger = structlog.get_logger(__name__)

_MARKET_SLUG_PATTERN = re.compile(r"^btc-updown-(?P<horizon>\d+)m-(?P<start>\d+)$")

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds
except Exception:  # pragma: no cover
    ClobClient = None
    ApiCreds = None


@dataclass(slots=True)
class RiskState:
    daily_realized_pnl: float = 0.0
    trades_this_hour: int = 0
    last_trade_hour: int = -1
    last_pnl_reset_date_utc: str = ""
    open_exposure_usd_by_market: dict[str, float] | None = None
    total_open_notional_usd: float = 0.0
    trades_since_reconcile: int = 0
    cumulative_realized_pnl: float = 0.0
    consecutive_losses: int = 0
    cooldown_until_ts: float = 0.0
    peak_equity_usd: float = 0.0

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
        self._cached_equity_usd: float = max(0.0, float(settings.equity_usd))
        self._last_equity_refresh_ts: float = 0.0

        if self.risk.peak_equity_usd <= 0:
            self.risk.peak_equity_usd = self._cached_equity_usd
            self._persist_risk_state()

        if not settings.dry_run and ClobClient is not None:
            self._live_auth_ready = self._initialize_live_auth()

    def _build_api_creds_payload(self, derived_creds: Any) -> Any:
        has_static_creds = bool(self.settings.api_key and self.settings.api_secret and self.settings.api_passphrase)

        if has_static_creds:
            if ApiCreds is not None:
                return ApiCreds(
                    api_key=self.settings.api_key,
                    api_secret=self.settings.api_secret,
                    api_passphrase=self.settings.api_passphrase,
                )
            return {
                "api_key": self.settings.api_key,
                "api_secret": self.settings.api_secret,
                "api_passphrase": self.settings.api_passphrase,
            }

        if derived_creds is None:
            return None
        if ApiCreds is not None and isinstance(derived_creds, dict):
            return ApiCreds(**derived_creds)
        return derived_creds

    def _initialize_live_auth(self) -> bool:
        try:
            derived_creds: Any = None
            if bool(self.settings.api_key and self.settings.api_secret and self.settings.api_passphrase):
                if not self.settings.allow_static_creds or not self.settings.static_creds_confirmation:
                    logger.error("static_clob_creds_not_allowed")
                    return False
                logger.warning("using_static_clob_creds_emergency_fallback")
            else:
                if self.settings.api_cred_rotation_seconds <= 0:
                    logger.error("invalid_api_cred_rotation_seconds")
                    return False

            self.client = init_client(
                signer=self.settings.private_key,
                derived_creds=derived_creds,
                signature_type=self.settings.signature_type,
                funder_address=self.settings.funder_address,
                host=self.settings.clob_host,
                chain_id=self.settings.chain_id,
                clob_client_cls=ClobClient,
            )

            if not bool(self.settings.api_key and self.settings.api_secret and self.settings.api_passphrase):
                if hasattr(self.client, "derive_api_key"):
                    derived_creds = self.client.derive_api_key()
                elif hasattr(self.client, "create_api_key"):
                    derived_creds = self.client.create_api_key()

            if hasattr(self.client, "set_api_creds"):
                creds_payload = self._build_api_creds_payload(derived_creds)
                if creds_payload is None:
                    logger.error(
                        "missing_clob_l2_credentials",
                        has_api_key=bool(self.settings.api_key),
                        has_api_secret=bool(self.settings.api_secret),
                        has_api_passphrase=bool(self.settings.api_passphrase),
                    )
                    return False
                self.client.set_api_creds(creds_payload)

            if hasattr(self.client, "assert_level_2_auth"):
                self.client.assert_level_2_auth()

            BOT_API_CREDS_AGE_SECONDS.set(0)
        except CredentialValidationError as exc:
            logger.error("invalid_clob_client_configuration", error=str(exc))
            return False
        except Exception as exc:
            logger.error("clob_l2_auth_initialization_failed", error=str(exc))
            return False

        logger.info("clob_l2_auth_initialized")
        return True

    def _load_risk_state(self) -> RiskState:
        if not self._risk_state_path.exists():
            return RiskState()
        try:
            payload = orjson.loads(self._risk_state_path.read_bytes())
        except (OSError, TypeError, ValueError, orjson.JSONDecodeError):
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
            cumulative_realized_pnl=float(payload.get("cumulative_realized_pnl", 0.0)),
            consecutive_losses=int(payload.get("consecutive_losses", 0)),
            cooldown_until_ts=float(payload.get("cooldown_until_ts", 0.0)),
            peak_equity_usd=float(payload.get("peak_equity_usd", 0.0)),
        )
        if state.total_open_notional_usd <= 0 and state.open_exposure_usd_by_market:
            state.total_open_notional_usd = sum(state.open_exposure_usd_by_market.values())
        logger.info("risk_state_loaded", path=str(self._risk_state_path), risk_state=asdict(state))
        return state

    @staticmethod
    def _extract_first_float(payload: Any) -> float | None:
        if isinstance(payload, (int, float)):
            value = float(payload)
            return value if math.isfinite(value) else None
        if isinstance(payload, str):
            try:
                value = float(payload)
            except ValueError:
                return None
            return value if math.isfinite(value) else None
        if isinstance(payload, list):
            for item in payload:
                value = Trader._extract_first_float(item)
                if value is not None:
                    return value
            return None
        if isinstance(payload, dict):
            preferred = (
                "total",
                "total_usd",
                "equity",
                "equity_usd",
                "balance",
                "balance_usd",
                "available",
                "available_balance",
            )
            for key in preferred:
                if key in payload:
                    value = Trader._extract_first_float(payload.get(key))
                    if value is not None:
                        return value
            for value in payload.values():
                parsed = Trader._extract_first_float(value)
                if parsed is not None:
                    return parsed
        return None

    def _query_exchange_equity_usd(self) -> float | None:
        if self.client is None:
            return None
        for method_name in ("get_balance", "get_balances", "get_account", "get_account_balance", "get_collateral"):
            method = getattr(self.client, method_name, None)
            if method is None:
                continue
            try:
                payload = method()
            except Exception:
                continue
            parsed = self._extract_first_float(payload)
            if parsed is not None and parsed > 0:
                return parsed
        return None

    def _refresh_equity_cache(self, force: bool = False) -> float:
        now = time.time()
        if not force and (now - self._last_equity_refresh_ts) < self.settings.equity_refresh_seconds:
            return self._cached_equity_usd

        refreshed = self._query_exchange_equity_usd()
        if refreshed is not None:
            self._cached_equity_usd = refreshed
            self._last_equity_refresh_ts = now
            return self._cached_equity_usd

        self._last_equity_refresh_ts = now
        self._cached_equity_usd = max(0.0, self.settings.equity_usd + self.risk.cumulative_realized_pnl)
        return self._cached_equity_usd

    def _cooldown_active(self) -> bool:
        return time.time() < self.risk.cooldown_until_ts

    def _maybe_start_cooldown(self) -> None:
        if self.settings.cooldown_minutes <= 0:
            return

        equity_usd = self._refresh_equity_cache(force=True)
        if equity_usd > self.risk.peak_equity_usd:
            self.risk.peak_equity_usd = equity_usd

        peak = max(self.risk.peak_equity_usd, 1e-9)
        drawdown_pct = max(0.0, (peak - equity_usd) / peak)
        triggered_by_losses = (
            self.settings.cooldown_consecutive_losses > 0
            and self.risk.consecutive_losses >= self.settings.cooldown_consecutive_losses
        )
        triggered_by_drawdown = (
            self.settings.cooldown_drawdown_pct > 0
            and drawdown_pct >= self.settings.cooldown_drawdown_pct
        )

        if not (triggered_by_losses or triggered_by_drawdown):
            return
        cooldown_until = time.time() + (self.settings.cooldown_minutes * 60)
        if cooldown_until > self.risk.cooldown_until_ts:
            self.risk.cooldown_until_ts = cooldown_until
            logger.warning(
                "risk_cooldown_activated",
                consecutive_losses=self.risk.consecutive_losses,
                drawdown_pct=drawdown_pct,
                cooldown_minutes=self.settings.cooldown_minutes,
            )

    def _normalize_open_exposure(self, raw: Any) -> dict[str, float]:
        if not isinstance(raw, dict):
            return {}

        normalized: dict[str, float] = {}
        for key, value in raw.items():
            if not isinstance(key, str):
                continue
            try:
                amount = max(0.0, float(value))
            except (TypeError, ValueError):
                continue
            migrated_key = self._migrate_legacy_exposure_key(key)
            normalized[migrated_key] = normalized.get(migrated_key, 0.0) + amount
        return normalized

    @staticmethod
    def _migrate_legacy_exposure_key(key: str) -> str:
        parts = key.split("|")
        if len(parts) >= 4:
            return key
        if len(parts) == 3:
            token_id, horizon, direction = parts
            return f"{token_id}|{horizon}|{direction}|legacy"
        return key

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
                    key = self._market_exposure_key(token_id, horizon, str(direction), market_identity="legacy")
                    try:
                        migrated[key] = max(0.0, float(notional))
                    except (TypeError, ValueError):
                        continue

        return migrated

    def _persist_risk_state(self) -> None:
        try:
            self._risk_state_path.parent.mkdir(parents=True, exist_ok=True)
            self._risk_state_path.write_bytes(orjson.dumps(asdict(self.risk)))
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
    def _market_identity(*, market_slug: str | None = None, market_start_epoch: int | None = None) -> str:
        if market_slug:
            return f"slug:{market_slug}"
        if market_start_epoch is not None:
            return f"start:{market_start_epoch}"
        return "legacy"

    @classmethod
    def _market_exposure_key(
        cls,
        token_id: str,
        horizon: str,
        direction: str,
        *,
        market_slug: str | None = None,
        market_start_epoch: int | None = None,
        market_identity: str | None = None,
    ) -> str:
        identity = market_identity or cls._market_identity(market_slug=market_slug, market_start_epoch=market_start_epoch)
        return f"{token_id}|{horizon}|{direction.upper()}|{identity}"

    @staticmethod
    def _parse_horizon_minutes(horizon: str) -> int | None:
        text = str(horizon).strip().lower()
        if text.endswith("m"):
            text = text[:-1]
        try:
            parsed = int(text)
        except (TypeError, ValueError):
            return None
        return parsed if parsed > 0 else None

    @classmethod
    def _infer_market_end_epoch(cls, horizon: str, market_identity: str) -> int | None:
        if market_identity.startswith("slug:"):
            market_identity = market_identity.split(":", 1)[1]

        slug_match = _MARKET_SLUG_PATTERN.match(market_identity)
        if slug_match:
            horizon_minutes = int(slug_match.group("horizon"))
            start_epoch = int(slug_match.group("start"))
            return start_epoch + horizon_minutes * 60

        if market_identity.startswith("start:"):
            try:
                start_epoch = int(market_identity.split(":", 1)[1])
            except (TypeError, ValueError):
                return None
            horizon_minutes = cls._parse_horizon_minutes(horizon)
            if horizon_minutes is None:
                return None
            return start_epoch + horizon_minutes * 60
        return None

    def _cleanup_expired_exposure(self, now_ts: int | None = None) -> bool:
        if not self.risk.open_exposure_usd_by_market:
            return False

        now = now_ts if now_ts is not None else int(time.time())
        removed = False
        for key in list(self.risk.open_exposure_usd_by_market.keys()):
            parts = key.split("|", 3)
            if len(parts) != 4:
                continue
            _token_id, horizon, _direction, market_identity = parts
            end_epoch = self._infer_market_end_epoch(horizon, market_identity)
            if end_epoch is None or end_epoch > now:
                continue
            self.risk.open_exposure_usd_by_market.pop(key, None)
            removed = True

        if removed:
            self.risk.total_open_notional_usd = max(0.0, sum(self.risk.open_exposure_usd_by_market.values()))
        return removed

    def _risk_blocked(
        self,
        notional_usd: float,
        token_id: str,
        horizon: str,
        direction: str,
        *,
        market_slug: str | None = None,
        market_start_epoch: int | None = None,
    ) -> bool:
        if notional_usd > self.settings.max_usd_per_trade:
            return True
        if self.risk.daily_realized_pnl <= -abs(self.settings.max_daily_loss):
            return True
        if self._cooldown_active():
            return True
        if self.risk.trades_this_hour >= self.settings.max_trades_per_hour:
            return True
        market_key = self._market_exposure_key(
            token_id,
            horizon,
            direction,
            market_slug=market_slug,
            market_start_epoch=market_start_epoch,
        )
        next_market_exposure = self.risk.open_exposure_usd_by_market.get(market_key, 0.0) + notional_usd
        if next_market_exposure > self.settings.max_open_exposure_per_market:
            return True
        if (self.risk.total_open_notional_usd + notional_usd) > self.settings.max_total_open_exposure:
            return True
        return False

    def _update_risk_metrics(self, risk_blocked: bool) -> None:
        DAILY_REALIZED_PNL.set(self.risk.daily_realized_pnl)
        RISK_LIMIT_BLOCKED.set(1 if risk_blocked else 0)

    def _check_risk(
        self,
        notional_usd: float,
        token_id: str,
        horizon: str,
        direction: str,
        *,
        market_slug: str | None = None,
        market_start_epoch: int | None = None,
    ) -> bool:
        self._reset_daily_pnl_if_needed()
        self._refresh_equity_cache()
        cleaned = self._cleanup_expired_exposure()
        now_hour = datetime.now(timezone.utc).hour
        if now_hour != self.risk.last_trade_hour:
            self.risk.last_trade_hour = now_hour
            self.risk.trades_this_hour = 0
            self._persist_risk_state()

        if cleaned:
            self._persist_risk_state()

        blocked = self._risk_blocked(
            notional_usd,
            token_id=token_id,
            horizon=horizon,
            direction=direction,
            market_slug=market_slug,
            market_start_epoch=market_start_epoch,
        )
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

    def _apply_open_exposure_delta(
        self,
        token_id: str,
        horizon: str,
        direction: str,
        delta_usd: float,
        *,
        market_slug: str | None = None,
        market_start_epoch: int | None = None,
    ) -> None:
        if delta_usd == 0:
            return
        key = self._market_exposure_key(
            token_id,
            horizon,
            direction,
            market_slug=market_slug,
            market_start_epoch=market_start_epoch,
        )
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
                market_slug = str(position.get("market_slug") or position.get("slug") or "").strip() or None
                market_start_epoch_raw = position.get("start_epoch") or position.get("startEpoch")
                market_start_epoch = None
                if market_start_epoch_raw is not None:
                    try:
                        market_start_epoch = int(market_start_epoch_raw)
                    except (TypeError, ValueError):
                        market_start_epoch = None
                rebuilt[
                    self._market_exposure_key(
                        token_id,
                        horizon,
                        direction,
                        market_slug=market_slug,
                        market_start_epoch=market_start_epoch,
                    )
                ] = notional

            self.risk.open_exposure_usd_by_market = rebuilt
            self.risk.total_open_notional_usd = sum(rebuilt.values())
            self._cleanup_expired_exposure()
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
        market_slug: str | None = None,
        market_start_epoch: int | None = None,
    ) -> None:
        self._reset_daily_pnl_if_needed()
        realized_pnl = self._extract_realized_pnl(resp)
        if realized_pnl:
            self.risk.daily_realized_pnl += realized_pnl
            self.risk.cumulative_realized_pnl += realized_pnl
            if realized_pnl < 0:
                self.risk.consecutive_losses += 1
            elif realized_pnl > 0:
                self.risk.consecutive_losses = 0

        filled_notional, complete_fill_details = self._extract_fill_notional(resp)
        applied_notional = filled_notional if filled_notional > 0 else fallback_notional_usd
        self._apply_open_exposure_delta(
            token_id,
            horizon,
            direction,
            applied_notional,
            market_slug=market_slug,
            market_start_epoch=market_start_epoch,
        )
        self._maybe_reconcile_exposure_from_exchange(force=not complete_fill_details)
        self._cleanup_expired_exposure()
        self._maybe_start_cooldown()

        self._persist_risk_state()
        self._update_risk_metrics(
            risk_blocked=self._risk_blocked(
                0.0,
                token_id=token_id,
                horizon=horizon,
                direction=direction,
                market_slug=market_slug,
                market_start_epoch=market_start_epoch,
            )
        )

    @staticmethod
    def _classify_submit_exception(exc: Exception) -> str:
        text = str(exc).lower()
        name = exc.__class__.__name__.lower()

        if isinstance(exc, (TimeoutError, asyncio.TimeoutError)) or "timeout" in text or "timeout" in name:
            return "timeout"
        if any(k in text for k in ("401", "403", "unauthorized", "forbidden", "invalid api", "auth")):
            return "auth"
        if any(k in text for k in ("allowance", "approval", "approve", "not approved", "insufficient allowance")):
            return "allowance"
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

    def _submit_live_fok_order(self, token_id: str, px: float, size: float) -> Any:
        if self.client is None:
            raise RuntimeError("missing_clob_client")

        if hasattr(self.client, "create_limit_order") and hasattr(self.client, "post_order"):
            limit_order = self.client.create_limit_order(
                price=px,
                size=size,
                side="BUY",
                token_id=token_id,
                time_in_force="FOK",
            )
            try:
                return self.client.post_order(limit_order, time_in_force="FOK")
            except TypeError:
                return self.client.post_order(limit_order)

        raise RuntimeError("unsupported_order_submission_api")

    async def buy_fok(
        self,
        token_id: str,
        ask: float,
        horizon: str,
        p_hat: float | None = None,
        fee_cost: float = 0.0,
        slippage_cost: float = 0.0,
        *,
        market_slug: str | None = None,
        market_start_epoch: int | None = None,
    ) -> bool:
        constraints = self.token_constraints_by_id.get(token_id, TokenConstraints())
        tick_size = constraints.tick_size
        min_order_size = constraints.min_order_size

        if self.token_metadata_cache is not None and tick_size is None:
            tick_size = self.token_metadata_cache.get_tick_size(token_id, fallback_tick_size=0.001)

        tick_size = tick_size or 0.001
        min_order_size = min_order_size or 0.1
        size_step = min_order_size

        effective_cost = min(0.999, max(1e-6, ask + fee_cost + slippage_cost))
        if p_hat is None:
            kelly_suggested = 0.0
        else:
            kelly_suggested = max(0.0, min(1.0, (float(p_hat) - effective_cost) / max(1e-9, 1.0 - effective_cost)))

        dynamic_risk_pct = min(
            self.settings.max_risk_pct_cap,
            max(self.settings.risk_pct_per_trade, kelly_suggested * self.settings.kelly_fraction),
        )
        equity_usd = self._refresh_equity_cache()
        quote_size_usd = min(self.settings.max_usd_per_trade, equity_usd * dynamic_risk_pct)

        size = quote_size_usd / ask
        size = round_size_to_step(size, size_step)
        px = round_price_up_to_tick(ask, tick_size)

        if min_order_size is not None and size < min_order_size:
            adjusted_size = self._round_size_up_to_step(min_order_size, size_step)
            adjusted_notional = adjusted_size * px
            if not self._check_risk(
                adjusted_notional,
                token_id=token_id,
                horizon=horizon,
                direction="BUY",
                market_slug=market_slug,
                market_start_epoch=market_start_epoch,
            ):
                logger.info(
                    "order_reject_min_order_size",
                    token_id=token_id,
                    ask=ask,
                    min_order_size=min_order_size,
                    computed_size=size,
                    adjusted_size=adjusted_size,
                    adjusted_notional=adjusted_notional,
                    equity_usd=equity_usd,
                    dynamic_risk_pct=dynamic_risk_pct,
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
                quote_size_usd=quote_size_usd,
            )
            size = adjusted_size

        notional = size * px
        if not self._check_risk(
            notional,
            token_id=token_id,
            horizon=horizon,
            direction="BUY",
            market_slug=market_slug,
            market_start_epoch=market_start_epoch,
        ):
            logger.info(
                "risk_reject",
                token_id=token_id,
                ask=ask,
                size=size,
                daily_realized_pnl=self.risk.daily_realized_pnl,
                max_daily_loss=self.settings.max_daily_loss,
                trades_this_hour=self.risk.trades_this_hour,
                open_exposure_market=self.risk.open_exposure_usd_by_market.get(
                    self._market_exposure_key(
                        token_id,
                        horizon,
                        "BUY",
                        market_slug=market_slug,
                        market_start_epoch=market_start_epoch,
                    ),
                    0.0,
                ),
                total_open_notional_usd=self.risk.total_open_notional_usd,
                cooldown_until_ts=self.risk.cooldown_until_ts,
                equity_usd=equity_usd,
                dynamic_risk_pct=dynamic_risk_pct,
            )
            TRADES.labels(status="rejected", side="buy", horizon=horizon).inc()
            return False

        if self.settings.dry_run:
            logger.info(
                "dry_run_order",
                token_id=token_id,
                ask=px,
                size=size,
                quote_size_usd=quote_size_usd,
                equity_usd=equity_usd,
                dynamic_risk_pct=dynamic_risk_pct,
                kelly_suggested=kelly_suggested,
            )
            self.risk.trades_this_hour += 1
            self._record_post_trade_updates(
                {"fills": [{"price": px, "size": size}]},
                token_id=token_id,
                horizon=horizon,
                direction="BUY",
                fallback_notional_usd=notional,
                market_slug=market_slug,
                market_start_epoch=market_start_epoch,
            )
            TRADES.labels(status="dry_run", side="buy", horizon=horizon).inc()
            return True

        if self.client is None:
            logger.error("missing_clob_client")
            TRADES.labels(status="error", side="buy", horizon=horizon).inc()
            return False

        if not self._live_auth_ready:
            logger.error("missing_clob_l2_credentials")
            TRADES.labels(status="error", side="buy", horizon=horizon).inc()
            return False

        try:
            resp = await asyncio.wait_for(
                asyncio.to_thread(self._submit_live_fok_order, token_id, px, size),
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
            event = "order_submit_failed"
            if failure_type == "auth":
                event = "order_submit_auth_failed"
            elif failure_type == "allowance":
                event = "order_submit_allowance_failed"
            logger.warning(
                event,
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
                market_slug=market_slug,
                market_start_epoch=market_start_epoch,
            )
            TRADES.labels(status="filled", side="buy", horizon=horizon).inc()
        else:
            TRADES.labels(status="rejected", side="buy", horizon=horizon).inc()
        logger.info(
            "order_result",
            ok=ok,
            response=resp,
            daily_realized_pnl=self.risk.daily_realized_pnl,
            risk_limit_blocked=self._risk_blocked(
                0.0,
                token_id=token_id,
                horizon=horizon,
                direction="BUY",
                market_slug=market_slug,
                market_start_epoch=market_start_epoch,
            ),
        )
        return ok
