import duckdb
from datetime import date
from pathlib import Path
from src.models import CandleRecord, ImpulseSignal, RunLog, FunnelSnapshot

_DDL_CANDLES = """
CREATE TABLE IF NOT EXISTS candles (
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
)
"""

_DDL_IMPULSES = """
CREATE TABLE IF NOT EXISTS impulse_signals (
    ticker       VARCHAR,
    trade_date   DATE,
    open         DOUBLE,
    close        DOUBLE,
    change_pct   DOUBLE,
    direction    VARCHAR,
    interval     VARCHAR,
    detected_at  TIMESTAMP,
    PRIMARY KEY (ticker, trade_date, interval)
)
"""

_DDL_RUN_LOG = """
CREATE TABLE IF NOT EXISTS run_log (
    run_date           DATE PRIMARY KEY,
    status             VARCHAR,
    tickers_processed  INTEGER,
    candles_written    INTEGER,
    impulses_found     INTEGER,
    ran_at             TIMESTAMP,
    error              VARCHAR
)
"""

_DDL_FUNNEL_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS funnel_snapshots (
    ticker          VARCHAR,
    snapshot_date   DATE,
    impulse_date    DATE,
    state           VARCHAR,
    stable_days     INTEGER,
    day0_high       DOUBLE,
    day0_volume     DOUBLE,
    failure_reason  VARCHAR,
    PRIMARY KEY (ticker, snapshot_date)
)
"""
# PRIMARY KEY (ticker, snapshot_date) means one row per stock per day.
# INSERT OR IGNORE is used for all writes — see write_funnel_snapshots().
# Re-running a date never mutates existing rows. Full history is preserved.
# Re-running any date range in any order always produces the same table state.


def get_conn(db_path: str) -> duckdb.DuckDBPyConnection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(db_path)
    conn.execute(_DDL_CANDLES)
    conn.execute(_DDL_IMPULSES)
    conn.execute(_DDL_RUN_LOG)
    conn.execute(_DDL_FUNNEL_SNAPSHOTS)
    return conn


def upsert_candles(conn: duckdb.DuckDBPyConnection, records: list[CandleRecord]) -> int:
    if not records:
        return 0
    rows = [(r.ticker, r.datetime, r.interval, r.open, r.high,
             r.low, r.close, r.volume, r.ingested_at) for r in records]
    conn.executemany("""
        INSERT OR REPLACE INTO candles
            (ticker, datetime, interval, open, high, low, close, volume, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    return len(rows)


def upsert_impulses(conn: duckdb.DuckDBPyConnection, signals: list[ImpulseSignal]) -> int:
    if not signals:
        return 0
    rows = [(s.ticker, s.trade_date, s.open, s.close, s.change_pct,
             s.direction, s.interval, s.detected_at) for s in signals]
    conn.executemany("""
        INSERT OR REPLACE INTO impulse_signals
            (ticker, trade_date, open, close, change_pct, direction, interval, detected_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    return len(rows)


def log_run(conn: duckdb.DuckDBPyConnection, run: RunLog) -> None:
    conn.execute("""
        INSERT OR REPLACE INTO run_log
            (run_date, status, tickers_processed, candles_written, impulses_found, ran_at, error)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (run.run_date, run.status, run.tickers_processed,
          run.candles_written, run.impulses_found, run.ran_at, run.error))


def get_missing_dates(conn: duckdb.DuckDBPyConnection, since: date, until: date) -> list[date]:
    """Return weekdays in [since, until] that have no successful run_log entry."""
    result = conn.execute("""
        SELECT DISTINCT run_date FROM run_log WHERE status = 'success'
    """).fetchall()
    completed = {r[0] for r in result}

    from datetime import timedelta
    missing = []
    d = since
    while d <= until:
        if d.weekday() < 5 and d not in completed:  # Mon–Fri only
            missing.append(d)
        d += timedelta(days=1)
    return missing


def query(conn: duckdb.DuckDBPyConnection, sql: str):
    """Run a SQL query and return a Polars DataFrame."""
    return conn.execute(sql).pl()


# ---------------------------------------------------------------------------
# Funnel snapshots — append-only daily state (INSERT OR IGNORE)
# ---------------------------------------------------------------------------

def write_funnel_snapshots(conn: duckdb.DuckDBPyConnection, snapshots: list[FunnelSnapshot]) -> int:
    """
    Write funnel snapshot rows using INSERT OR IGNORE.

    Idempotency guarantee
    ---------------------
    PRIMARY KEY is (ticker, snapshot_date). If a row already exists for a
    given ticker + date (because the pipeline already ran today, or because
    this date was processed during a backfill / re-run), the INSERT is
    silently skipped. The existing row is never touched.

    This means:
    • Run today twice → second run is a no-op                  ✓
    • Re-run a past date out of order → existing rows untouched ✓
    • Process any date range in any order → same final state    ✓
    """
    if not snapshots:
        return 0
    rows = [
        (s.ticker, s.snapshot_date, s.impulse_date, s.state.value,
         s.stable_days, s.day0_high, s.day0_volume, s.failure_reason)
        for s in snapshots
    ]
    conn.executemany("""
        INSERT OR IGNORE INTO funnel_snapshots
            (ticker, snapshot_date, impulse_date, state,
             stable_days, day0_high, day0_volume, failure_reason)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    return len(rows)
