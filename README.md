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
Polymarket market resolution is based on Chainlink aggregated reference prices, not spot exchange prints.

- ✅ Canonical decision feed: `crypto_prices_chainlink` topic with `BTC/USD` symbol.
- ⚠️ Spot liveness fallback feed: public spot ticker (`chainlink_direct_api_url`) used only to keep runtime alive during RTDS stalls.
- ❌ Incorrect approach: treating spot fallback prints as canonical resolution truth.

> **Warning:** Spot fallback mode is a liveness-only degradation path. It can diverge from canonical resolution data and should be treated as reduced-confidence trading conditions.

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
CLOB_WS_URL=wss://ws-subscriptions-clob.polymarket.com
GAMMA_API_URL=https://gamma-api.polymarket.com

SYMBOL=btc/usd
WATCH_RETURN_THRESHOLD=0.005
HAMMER_SECS=15
D_MIN=5
MAX_ENTRY_PRICE=0.97
FEE_BPS=10
FEE_RATE_TTL_SECONDS=300

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

## RTDS outage runbook (fallback behavior)

Expected behavior when RTDS degrades or disconnects:

1. Bot detects stale RTDS updates after `price_staleness_threshold`.
2. If `USE_FALLBACK_FEED=true`, it enters **spot liveness fallback mode** and emits warning logs with `reduced_trading_confidence=true`.
3. By default, order placement is gated while fallback is active (`ALLOW_ORDERS_WHILE_FALLBACK_ACTIVE=false`).
4. RTDS recovery automatically returns the bot to canonical feed mode.

Operational limits for fallback mode:

- Fallback pricing is **not** a canonical resolution feed and may diverge from Chainlink reference prices.
- Use fallback only for continuity and observability during outages; do not treat it as normal trading conditions.
- If you intentionally allow trading during fallback, set `ALLOW_ORDERS_WHILE_FALLBACK_ACTIVE=true` and monitor divergence closely.

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

## Streamlit operations dashboard

A dedicated operations dashboard is available at `dashboard.py`.

Key panels include:
- RTDS vs spot divergence.
- Feed staleness and heartbeat health.
- CLOB top-of-book and depth.
- Z-score and EV trend.
- Exposures, orders, and fills.
- Kill-switch and mode state.

The dashboard starts a background websocket ingest worker (async loop in a thread) and pushes updates into a queue consumed by `st.session_state`.

### Run locally

```bash
pip install -e .
pip install streamlit
streamlit run dashboard.py
```

### Replay sessions

For deterministic playback visualizations, place replay CSV files in:

```text
data/replay_sessions/
```

Then select a session in the dashboard and move the playback slider.

### Docker deployment

Build and run:

```bash
docker build -t polymarket-bot-dashboard .
docker run --rm -p 8501:8501 --env-file .env polymarket-bot-dashboard
```

### Reverse proxy example (Nginx)

```nginx
server {
    listen 80;
    server_name dashboard.example.com;

    location / {
        proxy_pass http://127.0.0.1:8501;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

### Environment variables for dashboard/runtime

Common values:

- `RTDS_WS_URL`
- `CLOB_WS_URL`
- `SYMBOL`
- `DRY_RUN`
- `WATCH_RETURN_THRESHOLD`
- `D_MIN`
- `MAX_ENTRY_PRICE`
- `FEE_BPS`
- `FEE_RATE_TTL_SECONDS`
- `HAMMER_SECS`
- `METRICS_HOST`
- `METRICS_PORT`

### Streamlit Cloud deployment

1. Push this repository to GitHub.
2. Create a new Streamlit app.
3. Set entrypoint to `dashboard.py`.
4. Configure secrets/environment values in Streamlit Cloud settings.

### Self-host deployment

- Run the Docker image behind Nginx/Caddy/Traefik.
- Restrict ingress with network policy and/or auth.
- Prefer `DRY_RUN=true` for dashboards in shared environments.
