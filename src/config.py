from __future__ import annotations

from pydantic import Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    chainlink_direct_api_url: str = "https://api.chain.link/streams/btc-usd"
    use_fallback_feed: bool = True

    watch_return_threshold: float = 0.005
    hammer_secs: int = 15
    d_min: float = 5.0
    max_entry_price: float = 0.97
    fee_bps: float = 10.0

    dry_run: bool = True
    max_usd_per_trade: float = 50.0
    max_daily_loss: float = 250.0
    max_trades_per_hour: int = 4

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
