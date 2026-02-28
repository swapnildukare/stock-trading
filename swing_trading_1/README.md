# Swing Trading 1 — NSE Impulse Scanner

## Architecture

Each layer has a single responsibility:

```
 ┌─────────────────────────────────────────────────────────────┐
 │                        DATA SOURCES                         │
 │              yfinance API  ·  NSE Index CSV                 │
 └────────────────────────────┬────────────────────────────────┘
                              │ raw OHLCV (pandas, bridge only)
                              ▼
 ┌─────────────────────────────────────────────────────────────┐
 │                PYDANTIC  (models.py)                        │
 │   Validates + types every record before it touches the DB   │
 │   CandleRecord · ImpulseSignal · RunLog · IngestionConfig   │
 └────────────────────────────┬────────────────────────────────┘
                              │ typed objects
                              ▼
 ┌─────────────────────────────────────────────────────────────┐
 │              DATABASE  —  DuckDB  (db.py)                   │
 │   Single file: data/market.duckdb                           │
 │   Tables: candles · impulse_signals · run_log               │
 │   Write: upsert_candles, upsert_impulses, log_run           │
 │   Read:  DuckDB SQL API → Polars DataFrame                  │
 └────────────────────────────┬────────────────────────────────┘
                              │ DuckDB → .pl() → Polars DF
                              ▼
 ┌─────────────────────────────────────────────────────────────┐
 │              ANALYSER  —  Polars  (impulse_finder.py)       │
 │   Filter abs(change_pct) >= threshold                       │
 │   Add direction column (BULL / BEAR)                        │
 │   Return Polars rows → back to Pydantic ImpulseSignal list  │
 └─────────────────────────────────────────────────────────────┘
```

### Why each tool?

| Layer | Tool | Why |
|-------|------|-----|
| External fetch | yfinance + pandas | yfinance only speaks pandas — used as a bridge only |
| Validation | Pydantic | Type-safe objects, catches bad data before DB write |
| Storage | DuckDB | Embedded, zero-config, SQL on a local file |
| Analysis | Polars | Clean, chainable API similar to PySpark — readable transformation pipelines without index complexity. Chosen over pandas for expressiveness, speed, and lazy evaluation support for future large-scale backtests |

---

## Project Structure

```
swing_trading_1/
├── src/
│   ├── models.py          # Pydantic: CandleRecord, ImpulseSignal, FunnelSnapshot, RunLog
│   ├── fetcher.py         # yfinance → list[CandleRecord]  (pandas bridge)
│   ├── db.py              # DuckDB: write + read (returns Polars DataFrames)
│   ├── impulse_finder.py  # DuckDB → Polars → list[ImpulseSignal]  (pure read)
│   ├── conditions.py      # Strategy Pattern: Condition ABC + StabilityCondition, VolumeCondition
│   ├── funnel_processor.py# compute_funnel_state() pure function + print_tracker()
│   ├── nse_fetcher.py     # NSE index CSV → ticker list
│   ├── ingestor.py        # CLI: ingest only
│   └── pipeline.py        # CLI: ingest + impulse detection + funnel snapshots + auto-catchup
├── backtest/
│   └── engine.py          # Day-by-day funnel simulation — writes candles only, rest in-memory
├── data/
│   └── market.duckdb      # created at runtime
├── config.py              # all tunable parameters
├── scanner.py             # quick-scan impulse filter (no DB)
└── main.py                # quick-scan entry point
```

---

## Setup

```bash
# First-time (installs deps + activates venv)
source setup_env.sh

# Or manually
uv sync && source trader/bin/activate
```

---

## Config (`config.py`)

| Key | Default | Description |
|-----|---------|-------------|
| `IMPULSE_THRESHOLD` | `6.0` | Min % daily move to flag as impulse |
| `INTERVAL` | `1d` | Candle interval |
| `DB_PATH` | `data/market.duckdb` | DuckDB file location |
| `NSE_INDEX` | `NIFTY_500` | Index to fetch when WATCHLIST is empty |
| `WATCHLIST` | `[]` | Manual tickers — empty means use NSE_INDEX |

Supported indices: `NIFTY_50` · `NIFTY_100` · `NIFTY_200` · `NIFTY_500`

---

## Running the Scripts

### 1 · Quick Scanner (no DB needed)
Scans live prices directly, prints matching stocks to console.
```bash
python main.py
```

---

### 2 · Ingestor (OHLCV → DuckDB)
Downloads candles and writes them to `data/market.duckdb`.
```bash
# Today's data
python -m src.ingestor

# Specific date
python -m src.ingestor --date 2026-02-27

# Historical fill — 90 days of 1h candles
python -m src.ingestor --date 2026-02-27 --lookback 90 --interval 1h

# Override tickers
python -m src.ingestor --tickers RELIANCE.NS TCS.NS INFY.NS
```

All options:
```
--date       End date YYYY-MM-DD          (default: today)
--lookback   Days to look back            (default: 1)
--interval   1m 5m 15m 1h 1d             (default: from config.py)
--tickers    Space-separated tickers      (default: WATCHLIST or NSE_INDEX)
--db-path    DuckDB file path             (default: data/market.duckdb)
```

---

### 3 · Pipeline (Ingest + Impulse Detection + Auto-Catchup)
Full daily pipeline — ingest candles → detect impulses → log run.
Automatically picks up any missed days (server down, bug, etc).

```bash
# Daily run — auto-catchup from last successful date to today
python -m src.pipeline

# Backtest a date range
python -m src.pipeline --from 2026-01-01 --to 2026-02-27

# Single date
python -m src.pipeline --from 2026-02-25 --to 2026-02-25
```

### 4 · Backtest (historical day-by-day simulation)
Opens DuckDB in **read-only mode** — zero writes to any table.
Candles must be loaded first with `src.ingestor`. Simulates the funnel
day-by-day using the same pure-compute functions as the pipeline.

```bash
# Simulate over a range — prints daily breakdown
python -m backtest.engine --from 2026-01-01 --to 2026-02-27

# Custom impulse threshold
python -m backtest.engine --from 2026-01-01 --threshold 7.0
```

Output per day:
```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  2026-01-15  ·  Day 11/42  ·  3 active  ·  1 new impulse(s)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ○  DAY 0 — new impulses
       RELIANCE.NS            +8.2%    High 2480.00
  ●  CONSOLIDATING
       TATAMOTORS.NS          +6.5%    Day 2/4  ·  High 850.00
  ◆  WATCHLIST — ready to trade
       REDINGTON.NS           +7.1%    Day 4/4  ·  High 286.00  ·  impulse 2026-01-11
  ✕  FALLOUTS
       SARDAEN.NS             +6.2%    Day 3 high 601.80 > day0_high 561.00 + 1.0%
```

**Per-date flow:**
```
fetch candles (yfinance + pandas bridge)
       │
       ▼  Pydantic validates each CandleRecord
       ▼
write → candles table (DuckDB upsert)
       │
       ▼  DuckDB SQL read → Polars DataFrame
       ▼  Polars: filter abs(change%) >= IMPULSE_THRESHOLD, add BULL/BEAR
       ▼  Polars rows → Pydantic ImpulseSignal objects
       ▼
write → impulse_signals table (DuckDB upsert)
       │
       ▼  Pydantic RunLog object
       ▼
write → run_log table (success / failed + counts)
```

**Auto-catchup:**
On every daily run, `run_log` is checked for the last successful date.
Any weekday between that date and today missing a success entry is re-processed.
Failed runs are retried automatically on the next execution.

---

### 4 · Impulse Finder (standalone — runs on already-ingested data)
```bash
python -m src.impulse_finder
```
Or call from Python:
```python
import sys; sys.path.insert(0, ".")
from src.db import get_conn
from src.impulse_finder import find_impulses
from datetime import date

conn    = get_conn("data/market.duckdb")
signals = find_impulses(conn, date(2026, 2, 27), threshold=6.0)
for s in signals:
    print(s.ticker, s.change_pct, s.direction)
conn.close()
```

---

## Querying Results (Polars)

```python
import sys; sys.path.insert(0, ".")
from src.db import get_conn, query

conn = get_conn("data/market.duckdb")

# All impulse signals
df = query(conn, """
    SELECT ticker, trade_date, change_pct, direction
    FROM impulse_signals
    ORDER BY trade_date DESC, change_pct DESC
""")
print(df)

# Pipeline run history / catchup status
runs = query(conn, "SELECT * FROM run_log ORDER BY run_date DESC LIMIT 10")
print(runs)

conn.close()
```

---

## DuckDB Schema

```sql
-- Raw OHLCV candles (written by ingestor / pipeline)
CREATE TABLE candles (
    ticker       VARCHAR,
    datetime     TIMESTAMP,
    interval     VARCHAR,
    open         DOUBLE,
    high         DOUBLE,
    low          DOUBLE,
    close        DOUBLE,
    volume       DOUBLE,
    ingested_at  TIMESTAMP,
    PRIMARY KEY (ticker, datetime, interval)
);

-- Stocks with abs(change%) >= IMPULSE_THRESHOLD (written by pipeline)
CREATE TABLE impulse_signals (
    ticker       VARCHAR,
    trade_date   DATE,
    open         DOUBLE,
    close        DOUBLE,
    change_pct   DOUBLE,
    direction    VARCHAR,     -- BULL | BEAR
    interval     VARCHAR,
    detected_at  TIMESTAMP,
    PRIMARY KEY (ticker, trade_date, interval)
);

-- Tracks every pipeline run (powers auto-catchup)
CREATE TABLE run_log (
    run_date           DATE PRIMARY KEY,
    status             VARCHAR,   -- success | failed
    tickers_processed  INTEGER,
    candles_written    INTEGER,
    impulses_found     INTEGER,
    ran_at             TIMESTAMP,
    error              VARCHAR
);
```

---

## Dependencies

| Package | Role |
|---------|------|
| `yfinance` | Fetch OHLCV from Yahoo Finance |
| `pandas` | Bridge only — yfinance output conversion. **Not used for analysis.** |
| `pydantic` | Validate and type all data objects |
| `duckdb` | Embedded database (read + write) |
| `polars` | Analysis layer — PySpark-style chainable API, replaces pandas for all transforms |
| `requests` | NSE index CSV fetch with browser headers |
---

See [FUTURE_SCOPE.md](FUTURE_SCOPE.md) for planned features and improvements.

source setup_env.sh                              # setup + run scanner

python main.py                                   # quick scan (no DB)
python -m src.ingestor                           # ingest today → DB
python -m src.pipeline                           # full pipeline + auto-catchup
python -m src.pipeline --from 2026-01-01 --to 2026-02-27  # backtest range
python -m src.impulse_finder                     # detect impulses on ingested data

# Write to file (nothing printed to terminal except the final path line)
python -m backtest.engine --from 2025-11-01 --to 2026-02-27 --out backtest/reports/nov_feb.txt

# Still works as before (prints to terminal)
python -m backtest.engine --from 2025-11-01 --to 2026-02-27