from __future__ import annotations

import os
from typing import Literal

from pydantic import AliasChoices, Field, HttpUrl, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


PROFILE_DEFAULTS: dict[str, dict[str, float | int]] = {
    "paper": {
        "watch_return_threshold": 0.004,
        "hammer_secs": 20,
        "d_min": 4.0,
        "max_entry_price": 0.95,
        "fee_bps": 8.0,
    },
    "live": {
        "watch_return_threshold": 0.006,
        "hammer_secs": 12,
        "d_min": 6.0,
        "max_entry_price": 0.93,
        "fee_bps": 10.0,
    },
    "high_vol": {
        "watch_return_threshold": 0.008,
        "hammer_secs": 10,
        "d_min": 10.0,
        "max_entry_price": 0.90,
        "fee_bps": 12.0,
    },
    "low_vol": {
        "watch_return_threshold": 0.003,
        "hammer_secs": 25,
        "d_min": 2.5,
        "max_entry_price": 0.96,
        "fee_bps": 8.0,
    },
}

JURISDICTION_BANNED_CATEGORIES: dict[str, tuple[str, ...]] = {
    "default": tuple(),
    "us": ("sports",),
    "us-nj": ("sports",),
}


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    rtds_ws_url: str = "wss://ws-live-data.polymarket.com"
    clob_ws_url: str = Field(
        default="wss://ws-subscriptions-clob.polymarket.com",
        validation_alias=AliasChoices("clob_ws_url", "clob_ws_base", "CLOB_WS_URL", "CLOB_WS_BASE"),
    )
    gamma_api_url: HttpUrl = "https://gamma-api.polymarket.com"

    symbol: str = "btc/usd"
    rtds_topic: str = "crypto_prices_chainlink"
    rtds_spot_topic: str = "crypto_prices"
    rtds_spot_max_age_seconds: float = 2.0
    log_price_comparison: bool = True

    coinbase_ws_feed_url: str = "wss://ws-feed.exchange.coinbase.com"
    coinbase_ws_direct_url: str = ""
    coinbase_ws_api_key: str = Field(default="", repr=False)
    coinbase_ws_api_secret: str = Field(default="", repr=False)
    coinbase_ws_api_passphrase: str = Field(default="", repr=False)
    coinbase_product_id: str = "BTC-USD"

    chainlink_max_lag_seconds: float = 2.5
    spot_max_lag_seconds: float = 2.5
    spot_quorum_min_sources: int = 2

    divergence_threshold_pct: float = 0.5
    divergence_sustain_seconds: float = 5.0

    rtds_ping_interval: int = 30
    rtds_pong_timeout: int = 10
    rtds_reconnect_delay_min: int = 1
    rtds_reconnect_delay_max: int = 60
    price_staleness_threshold: int = 10
    clob_book_staleness_threshold: int = 10
    clob_book_depth_levels: int = 10
    chainlink_direct_api_url: str = "https://api.exchange.coinbase.com/products/BTC-USD/ticker"
    use_fallback_feed: bool = False
    allow_orders_while_fallback_active: bool = False
    rtds_recovery_stabilization_seconds: float = 10.0
    rtds_recovery_min_fresh_updates: int = 3

    settings_profile: Literal["paper", "live", "high_vol", "low_vol"] = "paper"
    watch_return_threshold: float = 0.005
    watch_rolling_window_seconds: int = 60
    watch_zscore_threshold: float = 0.0
    price_stale_after_seconds: float = 2.0
    watch_mode_expiry_seconds: int = 60
    hammer_secs: int = 15
    d_min: float = 5.0
    max_entry_price: float = 0.97
    fee_bps: float = 10.0
    fee_formula_exponent: float = 1.0

    calibration_method: str = "none"
    calibration_input: str = "p_hat"
    calibration_params_path: str = ""
    calibration_logistic_coef: float = 1.0
    calibration_logistic_intercept: float = 0.0

    dry_run: bool = True
    max_usd_per_trade: float = 50.0
    risk_pct_per_trade: float = 0.01
    kelly_fraction: float = 0.25
    max_risk_pct_cap: float = 0.02
    equity_usd: float = 1000.0
    equity_refresh_seconds: float = 30.0
    cooldown_consecutive_losses: int = 3
    cooldown_drawdown_pct: float = 0.05
    cooldown_minutes: int = 15
    max_daily_loss_usd: float = Field(
        default=250.0,
        validation_alias=AliasChoices("max_daily_loss_usd", "max_daily_loss", "MAX_DAILY_LOSS_USD", "MAX_DAILY_LOSS"),
    )
    max_daily_loss_pct: float = Field(
        default=0.05,
        validation_alias=AliasChoices("max_daily_loss_pct", "MAX_DAILY_LOSS_PCT"),
    )
    max_trades_per_hour: int = 4
    min_trade_interval_seconds: int = 0
    max_open_exposure_per_market_usd: float = Field(
        default=100.0,
        validation_alias=AliasChoices(
            "max_open_exposure_per_market_usd",
            "max_open_exposure_per_market",
            "MAX_OPEN_EXPOSURE_PER_MARKET_USD",
            "MAX_OPEN_EXPOSURE_PER_MARKET",
        ),
    )
    max_open_exposure_per_market_pct: float = Field(
        default=0.05,
        validation_alias=AliasChoices("max_open_exposure_per_market_pct", "MAX_OPEN_EXPOSURE_PER_MARKET_PCT"),
    )
    max_total_open_exposure_usd: float = Field(
        default=500.0,
        validation_alias=AliasChoices(
            "max_total_open_exposure_usd",
            "max_total_open_exposure",
            "MAX_TOTAL_OPEN_EXPOSURE_USD",
            "MAX_TOTAL_OPEN_EXPOSURE",
        ),
    )
    max_total_open_exposure_pct: float = Field(
        default=0.15,
        validation_alias=AliasChoices("max_total_open_exposure_pct", "MAX_TOTAL_OPEN_EXPOSURE_PCT"),
    )
    allow_dry_run_equity_fallback: bool = True
    exposure_reconcile_every_n_trades: int = 10
    risk_state_path: str = ".state/risk_state.json"

    clob_host: str = "https://clob.polymarket.com"
    chain_id: int = 137
    signature_type: int = Field(
        default=2,
        validation_alias=AliasChoices("signature_type", "SIGNATURE_TYPE"),
        description="Wallet signature mode: EOA (0), Magic-link proxy (1), Gnosis Safe (2).",
    )
    funder_address: str = Field(
        default="",
        validation_alias=AliasChoices("funder_address", "funder", "FUNDER_ADDRESS", "FUNDER"),
        description="Proxy wallet that funds orders. Required for trading clients.",
    )
    allow_static_creds: bool = Field(default=False, validation_alias=AliasChoices("allow_static_creds", "ALLOW_STATIC_CREDS"))
    static_creds_confirmation: bool = Field(
        default=False,
        validation_alias=AliasChoices("static_creds_confirmation", "STATIC_CREDS_CONFIRMATION"),
        description="Explicit confirmation required before static API creds can be used.",
    )
    api_cred_rotation_seconds: int = Field(default=60 * 60 * 24 * 7)
    private_key: str = Field(default="", repr=False)
    api_key: str = Field(default="", repr=False)
    api_secret: str = Field(default="", repr=False)
    api_passphrase: str = Field(default="", repr=False)

    quote_size_usd: float = 20.0
    batch_orders_enabled: bool = True
    batch_order_max_size: int = 10
    order_submit_timeout_seconds: float = 5.0
    order_post_only: bool = False
    order_fok: bool = True
    order_time_in_force: str = ""
    metrics_host: str = "0.0.0.0"
    metrics_port: int = 9102
    token_metadata_ttl_seconds: float = 300.0
    fee_rate_ttl_seconds: float = 60.0
    enable_fee_rate: bool = True
    default_fee_rate_bps: float = 12.0
    recorder_enabled: bool = False
    recorder_output_path: str = "artifacts/session_recording.jsonl"
    recorder_queue_maxsize: int = 10000
    geoblock_abort: bool = True
    deployment_jurisdiction_override: str = Field(
        default="",
        validation_alias=AliasChoices(
            "deployment_jurisdiction_override",
            "DEPLOYMENT_JURISDICTION_OVERRIDE",
        ),
    )
    jurisdiction_override: str = Field(
        default="",
        validation_alias=AliasChoices(
            "jurisdiction_override",
            "JURISDICTION_OVERRIDE",
        ),
    )
    account_jurisdiction_override: str = Field(
        default="",
        validation_alias=AliasChoices(
            "account_jurisdiction_override",
            "ACCOUNT_JURISDICTION_OVERRIDE",
        ),
    )
    heartbeat_enabled: bool = True
    heartbeat_interval_seconds: float = 15.0
    heartbeat_max_consecutive_failures: int = 2
    heartbeat_cancel_on_failure: bool = True

    _PROFILE_TUNABLE_FIELDS: tuple[str, ...] = (
        "watch_return_threshold",
        "hammer_secs",
        "d_min",
        "max_entry_price",
        "fee_bps",
    )

    @classmethod
    def _field_default(cls, field_name: str) -> object:
        return cls.model_fields[field_name].default

    @classmethod
    def _is_explicit_override(cls, settings: "Settings", field_name: str) -> bool:
        if field_name in settings.model_fields_set:
            return True

        env_keys = {field_name, field_name.upper()}
        validation_alias = cls.model_fields[field_name].validation_alias
        if isinstance(validation_alias, AliasChoices):
            env_keys.update(str(choice) for choice in validation_alias.choices)
        elif isinstance(validation_alias, str):
            env_keys.add(validation_alias)

        return any(key in os.environ for key in env_keys)

    @model_validator(mode="after")
    def apply_profile_defaults(self) -> "Settings":
        profile = self.settings_profile.strip().lower()
        if profile not in PROFILE_DEFAULTS:
            allowed = ", ".join(sorted(PROFILE_DEFAULTS.keys()))
            raise ValueError(f"Unknown settings_profile={self.settings_profile}. Allowed: {allowed}")
        self.settings_profile = profile

        defaults = PROFILE_DEFAULTS[profile]
        for field_name in self._PROFILE_TUNABLE_FIELDS:
            if self._is_explicit_override(self, field_name):
                continue

            baseline_default = self._field_default(field_name)
            if getattr(self, field_name) != baseline_default:
                continue

            profile_default = defaults[field_name]
            setattr(self, field_name, type(baseline_default)(profile_default))

        if self.max_entry_price > 0.99:
            raise ValueError("Unsafe configuration: max_entry_price must be <= 0.99")
        if self.max_entry_price <= 0:
            raise ValueError("Unsafe configuration: max_entry_price must be > 0")
        if self.fee_bps < 0:
            raise ValueError("Unsafe configuration: fee_bps must be >= 0")
        if self.hammer_secs <= 0:
            raise ValueError("Unsafe configuration: hammer_secs must be > 0")
        if self.watch_return_threshold <= 0:
            raise ValueError("Unsafe configuration: watch_return_threshold must be > 0")
        if self.d_min <= 0:
            raise ValueError("Unsafe configuration: d_min must be > 0")
        if self.divergence_threshold_pct <= 0:
            raise ValueError("Unsafe configuration: divergence_threshold_pct must be > 0")
        if self.divergence_sustain_seconds <= 0:
            raise ValueError("Unsafe configuration: divergence_sustain_seconds must be > 0")
        if self.chainlink_max_lag_seconds <= 0:
            raise ValueError("Unsafe configuration: chainlink_max_lag_seconds must be > 0")
        if self.spot_max_lag_seconds <= 0:
            raise ValueError("Unsafe configuration: spot_max_lag_seconds must be > 0")
        if self.spot_quorum_min_sources < 2:
            raise ValueError("Unsafe configuration: spot_quorum_min_sources must be >= 2")
        if self.fee_rate_ttl_seconds <= 0:
            raise ValueError("Unsafe configuration: fee_rate_ttl_seconds must be > 0")
        if self.rtds_recovery_stabilization_seconds <= 0:
            raise ValueError("Unsafe configuration: rtds_recovery_stabilization_seconds must be > 0")
        if self.rtds_recovery_min_fresh_updates <= 0:
            raise ValueError("Unsafe configuration: rtds_recovery_min_fresh_updates must be > 0")
        if self.min_trade_interval_seconds < 0:
            raise ValueError("Unsafe configuration: min_trade_interval_seconds must be >= 0")
        if self.batch_order_max_size <= 0:
            raise ValueError("Unsafe configuration: batch_order_max_size must be > 0")

        return self

    @property
    def max_daily_loss(self) -> float:
        return self.max_daily_loss_usd

    @property
    def max_open_exposure_per_market(self) -> float:
        return self.max_open_exposure_per_market_usd

    @property
    def max_total_open_exposure(self) -> float:
        return self.max_total_open_exposure_usd
