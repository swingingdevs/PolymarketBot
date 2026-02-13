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
CLOB_WS_URL=wss://ws-subscriptions-clob.polymarket.com/ws/
GAMMA_API_URL=https://gamma-api.polymarket.com

SYMBOL=BTC/USD
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

## Notes

- Keep `DRY_RUN=true` until all connectivity and pricing checks are validated.
- For live orders, set credentials and ensure py-clob-client account setup is complete.
