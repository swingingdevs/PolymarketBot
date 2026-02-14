# Polymarket BTC Up/Down Hammer Bot

Production-ready Python 3.11 trading bot for Polymarket BTC Up/Down 5m and 15m contracts.

## Features

- RTDS websocket consumer (`wss://ws-live-data.polymarket.com`) for Chainlink BTC/USD.
- 1-minute watch trigger when `abs(60s return) >= 0.005`.
- Maintains contract start prices at exact 5m and 15m boundaries.
- Resolves active markets via Gamma slug pattern:
  - `btc-updown-5m-<start_epoch>`
  - `btc-updown-15m-<start_epoch>`
- Streams CLOB best bid/ask for Up and Down tokens.
- Hammer decision logic in last `HAMMER_SECS` before expiry:
  - computes distance-to-start `d`
  - realized `sigma1` from 1-second returns over last 60s
  - estimates `p_hat` using normal CDF
  - computes `EV = p_hat - ask - fee_cost`
  - executes highest EV candidate under constraints
- Dry-run mode, risk limits, JSON logs, Prometheus metrics endpoint.


## Price Feed Details

This bot **must** use the Chainlink Data Streams aggregated BTC/USD reference feed for decisioning.
Polymarket market resolution is based on Chainlink aggregated reference prices, not Binance spot prints.

- ✅ Correct feed: `crypto_prices_chainlink` topic with `BTC/USD` symbol.
- ❌ Incorrect feed: Binance-only spot prices (e.g., `BTCUSDT`) for resolution logic.

> **Warning:** Using the wrong price feed can systematically misprice outcomes and cause persistent losses,
> even if all other strategy logic is correct.

## Repo layout

```text
src/
  feeds/rtds.py
  feeds/clob_ws.py
  markets/gamma_cache.py
  strategy/state_machine.py
  execution/trader.py
  utils/rounding.py
  main.py
tests/
```

## Setup

1. Use Python 3.11.
2. Create and activate a virtualenv.
3. Install dependencies:

```bash
pip install -e .
pip install -e .[test]
```

4. Create `.env` from example.

```bash
cp .env.example .env
```

5. Run tests:

```bash
pytest
```

6. Start bot:

```bash
python -m main
```

## Example `.env`

```env
RTDS_WS_URL=wss://ws-live-data.polymarket.com
CLOB_WS_BASE=wss://ws-subscriptions-clob.polymarket.com
GAMMA_API_URL=https://gamma-api.polymarket.com

SYMBOL=btc/usd
WATCH_RETURN_THRESHOLD=0.005
HAMMER_SECS=15
D_MIN=5
MAX_ENTRY_PRICE=0.97
FEE_BPS=10

DRY_RUN=true
MAX_USD_PER_TRADE=50
MAX_DAILY_LOSS=250
MAX_TRADES_PER_HOUR=4
QUOTE_SIZE_USD=20

# required only for live trading
CLOB_HOST=https://clob.polymarket.com
CHAIN_ID=137
PRIVATE_KEY=
API_KEY=
API_SECRET=
API_PASSPHRASE=

METRICS_HOST=0.0.0.0
METRICS_PORT=9102
```

## Parameter calibration workflow

Use the replay/sweep utility to evaluate strategy parameters on historical data while reusing `StrategyStateMachine` logic.

```bash
python -m strategy.parameter_eval   --replay-csv data/replay.csv   --markets-json data/markets.json   --grid-json '{"watch_return_threshold":[0.003,0.005],"hammer_secs":[10,15,20],"d_min":[3,5],"max_entry_price":[0.9,0.95],"fee_bps":[8,10]}'   --output-prefix reports/calibration/latest
```

The utility exports:
- `reports/calibration/latest.csv` with per-run metrics: win rate, EV error, drawdown, trade frequency, total PnL.
- `reports/calibration/latest.json` with full runs and robust (top-20%) parameter ranges.

## Environment profiles and guardrails

`Settings` now supports profile-based defaults for strategy parameters:
- `SETTINGS_PROFILE=paper` (default)
- `SETTINGS_PROFILE=live`
- `SETTINGS_PROFILE=high_vol`
- `SETTINGS_PROFILE=low_vol`

You can still override individual values with env vars (`WATCH_RETURN_THRESHOLD`, `HAMMER_SECS`, etc.).

Startup is rejected for unsafe parameter combos, including:
- `MAX_ENTRY_PRICE > 0.99`
- `MAX_ENTRY_PRICE <= 0`
- `FEE_BPS <= 0`
- non-positive `HAMMER_SECS`, `WATCH_RETURN_THRESHOLD`, or `D_MIN`

## Notes

- Keep `DRY_RUN=true` until all connectivity and pricing checks are validated.
- For live orders, set credentials and ensure py-clob-client account setup is complete.

## Runtime smoke script (paper mode)

Use the smoke runner to verify end-to-end runtime wiring (Gamma + RTDS + CLOB + strategy + paper trader):

```bash
python scripts/smoke_runtime.py
```

Quick operator verification markers:
- `[SMOKE] SETTINGS_PROFILE=paper DRY_RUN=True`
- `[SMOKE] RESOLVED_MARKETS` followed by `[SMOKE] MARKET ...`
- `[SMOKE] BOOK_SNAPSHOT` and repeated `[SMOKE] SNAPSHOT ... bid=... ask=...`
- `[SMOKE] CANDIDATE ... ev=...` (strategy EV details printed at least once)
- `[SMOKE] PAPER_ORDER ... result=True|False` (paper `Trader.buy_fok` path)
- `[SMOKE] SUMMARY rtds_events=... clob_events=... candidate_found=... order_result=...`

The script targets ~120 seconds runtime max and exits with a concise summary for go/no-go checks.
