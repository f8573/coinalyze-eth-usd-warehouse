# coinalyze-eth-usd-warehouse

`coinalyze-eth-usd-warehouse` is a small Python warehouse service that discovers ETH-USD markets from Coinalyze, evaluates strict cross-metric and cross-granularity completeness, and stores qualifying `15min`, `1hour`, and `daily` data in PostgreSQL.

The warehouse treats completeness as a contiguous shared window, not just matching minimum and maximum timestamps. Candidate activation uses recent contiguous suffixes, active markets can grow their shared window backward and forward only when every required series stays attached, and pending sync work fills both historical gaps and newly opened right-edge gaps.

## Requirements

- Python 3.10+
- PostgreSQL
- A local `.env` file with:
  - `DATABASE_URL`
  - `COINALYZE_API_KEY`

## Install

```powershell
py -m pip install .
```

Create `.env` from `.env.example` and fill in your values before running the CLI.

## Commands

Apply the schema:

```powershell
coinalyze-eth-usd-warehouse init-db
```

Discover scoped ETH-USD markets from Coinalyze:

```powershell
coinalyze-eth-usd-warehouse discover
```

Evaluate candidates for strict eligibility:

```powershell
coinalyze-eth-usd-warehouse evaluate
```

Backfill all active markets:

```powershell
coinalyze-eth-usd-warehouse backfill
```

Backfill or catch up only the missing chunks inside each active shared window:

```powershell
coinalyze-eth-usd-warehouse backfill-pending
```

Probe active markets and extend their shared windows when newer or older contiguous data is available:

```powershell
coinalyze-eth-usd-warehouse extend-active
```

Refresh recent closed bars:

```powershell
coinalyze-eth-usd-warehouse refresh
```

Run the live closed-bar ingester:

```powershell
coinalyze-eth-usd-warehouse sync-live
```

Run the daily rerunnable pipeline:

```powershell
coinalyze-eth-usd-warehouse sync-daily
```

`sync-daily` performs:

1. market discovery and delist removal
2. bounded candidate evaluation
3. active-window extension
4. pending chunk loading for both left-edge history and right-edge catch-up
5. recent-bar refresh

Optional live-ingest settings:

- `LIVE_POLL_LAG_SECONDS` defaults to `10.0`
- `LIVE_LOOKBACK_CLOSED_BUCKETS` defaults to `2`
- `LIVE_IDLE_SLEEP_SECONDS` defaults to `5.0`
- `MIN_REQUEST_INTERVAL_SECONDS` defaults to `20.0`

Run the full pipeline:

```powershell
coinalyze-eth-usd-warehouse sync-all
```
