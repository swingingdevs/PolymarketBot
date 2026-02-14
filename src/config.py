from __future__ import annotations

from pydantic import Field, HttpUrl, model_validator
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


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    rtds_ws_url: str = "wss://ws-live-data.polymarket.com"
    clob_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/"
    gamma_api_url: HttpUrl = "https://gamma-api.polymarket.com"

    symbol: str = "BTC/USD"
    rtds_topic: str = "crypto_prices_chainlink"
    log_price_comparison: bool = True

    rtds_ping_interval: int = 30
    rtds_pong_timeout: int = 10
    rtds_reconnect_delay_min: int = 1
    rtds_reconnect_delay_max: int = 60
    price_staleness_threshold: int = 10
    clob_book_staleness_threshold: int = 10
    chainlink_direct_api_url: str = "https://api.chain.link/streams/btc-usd"
    use_fallback_feed: bool = True
    settings_profile: str = "live"

    watch_return_threshold: float = 0.005
    settings_profile: str = "paper"
    watch_rolling_window_seconds: int = 60
    watch_zscore_threshold: float = 0.0
    watch_mode_expiry_seconds: int = 60
    hammer_secs: int = 15
    d_min: float = 5.0
    max_entry_price: float = 0.97
    fee_bps: float = 10.0

    calibration_method: str = "none"
    calibration_input: str = "p_hat"
    calibration_params_path: str = ""
    calibration_logistic_coef: float = 1.0
    calibration_logistic_intercept: float = 0.0

    dry_run: bool = True
    max_usd_per_trade: float = 50.0
    max_daily_loss: float = 250.0
    max_trades_per_hour: int = 4
    max_open_exposure_per_market: float = 100.0
    max_total_open_exposure: float = 500.0
    exposure_reconcile_every_n_trades: int = 10
    risk_state_path: str = ".state/risk_state.json"

    clob_host: str = "https://clob.polymarket.com"
    chain_id: int = 137
    private_key: str = Field(default="", repr=False)
    api_key: str = Field(default="", repr=False)
    api_secret: str = Field(default="", repr=False)
    api_passphrase: str = Field(default="", repr=False)

    quote_size_usd: float = 20.0
    order_submit_timeout_seconds: float = 5.0
    metrics_host: str = "0.0.0.0"
    metrics_port: int = 9102
    token_metadata_ttl_seconds: float = 300.0
    settings_profile: str = "paper"

    @model_validator(mode="after")
    def apply_profile_defaults(self) -> "Settings":
        profile = self.settings_profile.strip().lower()
        if profile not in PROFILE_DEFAULTS:
            allowed = ", ".join(sorted(PROFILE_DEFAULTS.keys()))
            raise ValueError(f"Unknown settings_profile={self.settings_profile}. Allowed: {allowed}")
        self.settings_profile = profile

        defaults = PROFILE_DEFAULTS[profile]
        if "watch_return_threshold" not in self.model_fields_set:
            self.watch_return_threshold = float(defaults["watch_return_threshold"])
        if "hammer_secs" not in self.model_fields_set:
            self.hammer_secs = int(defaults["hammer_secs"])
        if "d_min" not in self.model_fields_set:
            self.d_min = float(defaults["d_min"])
        if "max_entry_price" not in self.model_fields_set:
            self.max_entry_price = float(defaults["max_entry_price"])
        if "fee_bps" not in self.model_fields_set:
            self.fee_bps = float(defaults["fee_bps"])

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

        return self
