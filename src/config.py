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

    settings_profile: str = "paper"
    watch_return_threshold: float | None = None
    hammer_secs: int | None = None
    d_min: float | None = None
    max_entry_price: float | None = None
    fee_bps: float | None = None

    dry_run: bool = True
    max_usd_per_trade: float = 50.0
    max_daily_loss: float = 250.0
    max_trades_per_hour: int = 4
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

    @model_validator(mode="after")
    def apply_profile_defaults(self) -> "Settings":
        profile = self.settings_profile.strip().lower()
        if profile not in PROFILE_DEFAULTS:
            allowed = ", ".join(sorted(PROFILE_DEFAULTS.keys()))
            raise ValueError(f"Unknown settings_profile={self.settings_profile}. Allowed: {allowed}")

        defaults = PROFILE_DEFAULTS[profile]
        if self.watch_return_threshold is None:
            self.watch_return_threshold = float(defaults["watch_return_threshold"])
        if self.hammer_secs is None:
            self.hammer_secs = int(defaults["hammer_secs"])
        if self.d_min is None:
            self.d_min = float(defaults["d_min"])
        if self.max_entry_price is None:
            self.max_entry_price = float(defaults["max_entry_price"])
        if self.fee_bps is None:
            self.fee_bps = float(defaults["fee_bps"])

        if self.max_entry_price > 0.99:
            raise ValueError("Unsafe configuration: max_entry_price must be <= 0.99")
        if self.max_entry_price <= 0:
            raise ValueError("Unsafe configuration: max_entry_price must be > 0")
        if self.fee_bps <= 0:
            raise ValueError("Unsafe configuration: fee_bps must be > 0")
        if self.hammer_secs <= 0:
            raise ValueError("Unsafe configuration: hammer_secs must be > 0")
        if self.watch_return_threshold <= 0:
            raise ValueError("Unsafe configuration: watch_return_threshold must be > 0")
        if self.d_min <= 0:
            raise ValueError("Unsafe configuration: d_min must be > 0")

        return self
