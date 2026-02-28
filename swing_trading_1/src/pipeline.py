"""
Daily pipeline: ingest OHLCV → detect impulses → compute funnel snapshots → log run.

Steps per trading date
----------------------
  1. Ingest OHLCV candles into DuckDB (immutable, INSERT OR REPLACE)
  2. Detect impulse signals >= IMPULSE_THRESHOLD % (immutable, INSERT OR REPLACE)
  3. Compute funnel snapshots (pure function, append-only INSERT OR IGNORE)
       • Reads all impulse signals within the active window (impulse_date >= D - CONSOLIDATION_DAYS)
       • For each, walks candles from impulse_date+1 to trade_date applying Conditions
       • Derives state: IMPULSE / CONSOLIDATING / WATCHLIST / FALLOUT
       • Writes one FunnelSnapshot row per (ticker, trade_date) — never mutates

Idempotency
-----------
All three tables are safe to re-run:
  candles         : INSERT OR REPLACE on (ticker, datetime, interval)
  impulse_signals : INSERT OR REPLACE on (ticker, trade_date, interval)
  funnel_snapshots: INSERT OR IGNORE  on (ticker, snapshot_date) — append-only

Re-run today twice, re-run a past date, run a range out of order — the
final state of every table is always identical.

Daily run (auto-catchup if days were missed):
    python -m src.pipeline

Backtest range:
    python -m src.pipeline --from 2026-01-01 --to 2026-02-27

Single date:
    python -m src.pipeline --from 2026-02-20 --to 2026-02-20
"""

import argparse
import sys
import time
import traceback
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import (
    WATCHLIST, NSE_INDEX, INTERVAL, DB_PATH,
    IMPULSE_THRESHOLD, CONSOLIDATION_DAYS,
    STABLE_MAX_UP_PCT, STABLE_MAX_DOWN_PCT,
)
from src.nse_fetcher import resolve_tickers
from src.models import RunLog, ImpulseSignal
from src.fetcher import fetch_candles
from src.db import (
    get_conn, upsert_candles, upsert_impulses, log_run, get_missing_dates,
    write_funnel_snapshots,
)
from src.impulse_finder import find_impulses
from src.conditions import StabilityCondition, VolumeCondition
from src.funnel_processor import compute_funnel_state, print_tracker

_W   = 56                  # output width
_SEP = "━" * _W             # heavy separator line used at section boundaries
_sep = "─" * _W             # light separator used between dates in multi-date runs


def get_tickers() -> tuple[list[str], str]:
    """Return (tickers, source_label) — source_label is shown in the header."""
    if WATCHLIST:
        return WATCHLIST, f"watchlist ({len(WATCHLIST)} tickers)"
    tickers = resolve_tickers(NSE_INDEX)
    return tickers, NSE_INDEX


# Conditions applied to Days 1-4 of consolidation — add more here to extend.
_CONDITIONS = [
    StabilityCondition(
        max_up_pct=STABLE_MAX_UP_PCT,
        max_down_pct=STABLE_MAX_DOWN_PCT,
    ),
    VolumeCondition(hard=False),   # soft — flags but does not eject
]


def process_date(conn, trade_date: date, tickers: list[str], multi: bool = False) -> tuple[RunLog, int, int]:
    """
    Ingest + impulse detection + funnel snapshot for one trading date.
    Returns (RunLog, watchlist_count, fallout_count).
    """
    if multi:
        print(f"  {_sep}")
    print(f"  {trade_date}")
    try:
        # Step 1: ingest candles
        records         = fetch_candles(tickers, trade_date, lookback_days=1, interval=INTERVAL)
        candles_written = upsert_candles(conn, records)
        print(f"    candles    {candles_written:>5}  ingested")

        # Step 2: detect impulses
        signals        = find_impulses(conn, trade_date, IMPULSE_THRESHOLD, INTERVAL)
        impulses_found = upsert_impulses(conn, signals)
        print(f"    impulses   {impulses_found:>5}  detected  (≥ {IMPULSE_THRESHOLD}%)")

        # Step 3: compute funnel snapshots
        import datetime as dt
        window_start = trade_date - dt.timedelta(days=CONSOLIDATION_DAYS + 2)
        active_rows  = conn.execute("""
            SELECT ticker, trade_date, open, close, change_pct, direction, interval, detected_at
            FROM   impulse_signals
            WHERE  trade_date >= ? AND trade_date <= ? AND interval = ?
        """, [window_start, trade_date, INTERVAL]).fetchall()

        impulse_objs = [
            ImpulseSignal(
                ticker=r[0], trade_date=r[1], open=r[2], close=r[3],
                change_pct=r[4], direction=r[5], interval=r[6], detected_at=r[7],
            )
            for r in active_rows
        ]

        snapshots       = compute_funnel_state(conn, trade_date, impulse_objs, _CONDITIONS, CONSOLIDATION_DAYS, INTERVAL)
        snaps_written   = write_funnel_snapshots(conn, snapshots)
        watchlist_count = sum(1 for s in snapshots if s.state.value == "watchlist")
        fallout_count   = sum(1 for s in snapshots if s.state.value == "fallout")
        print(f"    snapshots  {snaps_written:>5}  written    "
              f"({watchlist_count} watchlist · {fallout_count} fallout)")

        return (
            RunLog(
                run_date=trade_date, status="success",
                tickers_processed=len(tickers),
                candles_written=candles_written,
                impulses_found=impulses_found,
            ),
            watchlist_count, fallout_count,
        )
    except Exception as e:
        print(f"    ERROR: {e}")
        return (
            RunLog(
                run_date=trade_date, status="failed",
                tickers_processed=0, candles_written=0, impulses_found=0,
                error=traceback.format_exc(limit=3),
            ),
            0, 0,
        )


def run(from_date: date, to_date: date, force: bool = False) -> None:
    t_start  = time.time()
    conn     = get_conn(DB_PATH)
    tickers, source = get_tickers()

    date_range = str(from_date) if from_date == to_date else f"{from_date} → {to_date}"

    print(_SEP)
    print(f"  SWING PIPELINE  ·  {date_range}")
    print(f"  {len(tickers)} tickers  ·  {source}  ·  interval {INTERVAL}")
    print(_SEP)

    if force:
        import datetime as dt
        all_dates = []
        d = from_date
        while d <= to_date:
            if d.weekday() < 5:
                all_dates.append(d)
            d += dt.timedelta(days=1)
        missing = all_dates
        print(f"  --force: reprocessing {len(missing)} date(s) regardless of run_log")
    else:
        missing = get_missing_dates(conn, from_date, to_date)

    if not missing:
        print("  Nothing to do — all dates already processed.")
        print(_SEP)
        conn.close()
        return

    total_impulses  = 0
    total_watchlist = 0
    multi           = len(missing) > 1

    for d in missing:
        run_log, wl, fo = process_date(conn, d, tickers, multi=multi)
        log_run(conn, run_log)
        total_impulses  += run_log.impulses_found
        total_watchlist += wl

    print_tracker(conn, consolidation_days=CONSOLIDATION_DAYS, as_of=missing[-1])

    elapsed = time.time() - t_start
    print(_SEP)
    print(
        f"  Done  ·  {len(missing)} date{'s' if len(missing) > 1 else ''} processed"
        f"  ·  {total_impulses} impulses"
        f"  ·  {total_watchlist} watchlist"
        f"  ·  {elapsed:.1f}s"
    )
    print(_SEP)
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Swing trading pipeline")
    parser.add_argument("--from", dest="from_date", default=None,
                        help="Start date YYYY-MM-DD (default: last successful run or today)")
    parser.add_argument("--to",   dest="to_date",   default=date.today().isoformat(),
                        help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--force", action="store_true",
                        help="Reprocess dates even if already in run_log (useful after schema changes)")
    args = parser.parse_args()

    to_date = date.fromisoformat(args.to_date)

    # Auto-catchup: find last successful run, resume from next day
    if args.from_date is None:
        conn = get_conn(DB_PATH)
        row  = conn.execute(
            "SELECT MAX(run_date) FROM run_log WHERE status = 'success'"
        ).fetchone()
        conn.close()
        last_run  = row[0] if row and row[0] else None
        from_date = (last_run + __import__("datetime").timedelta(days=1)) if last_run else to_date
    else:
        from_date = date.fromisoformat(args.from_date)

    run(from_date, to_date, force=args.force)


if __name__ == "__main__":
    main()
