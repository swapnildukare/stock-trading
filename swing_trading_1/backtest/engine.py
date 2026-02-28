"""
backtest/engine.py
==================

Simulate the swing-trade funnel day-by-day over a historical date range.

DB contract
-----------
  READ-ONLY — opens DuckDB with read_only=True
  No tables are written. Candles must already be loaded
  (run ``python -m src.ingestor --lookback N`` first).

All funnel logic is in-memory using existing pure functions:

  src.impulse_finder.find_impulses()       — reads candles, returns ImpulseSignal list
  src.funnel_processor.compute_funnel_state() — reads candles, returns FunnelSnapshot list
  src.conditions.*                         — Strategy Pattern, zero I/O

Usage
-----
    python -m backtest.engine --from 2026-01-01 --to 2026-02-27
    python -m backtest.engine --from 2026-01-01 --threshold 7.0
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb

from config import (
    INTERVAL, DB_PATH,
    IMPULSE_THRESHOLD, CONSOLIDATION_DAYS,
    STABLE_MAX_UP_PCT, STABLE_MAX_DOWN_PCT,
)
from src.impulse_finder import find_impulses
from src.funnel_processor import compute_funnel_state
from src.conditions import StabilityCondition, VolumeCondition
from src.models import ImpulseSignal, StockState


def _open_readonly(db_path: str) -> duckdb.DuckDBPyConnection:
    """Open an existing DuckDB file in read-only mode."""
    if not Path(db_path).exists():
        raise FileNotFoundError(
            f"DB not found: {db_path}\n"
            "Run 'python -m src.ingestor --lookback N' first to load candles."
        )
    return duckdb.connect(db_path, read_only=True)

_W   = 56
_SEP = "━" * _W

_CONDITIONS = [
    StabilityCondition(max_up_pct=STABLE_MAX_UP_PCT, max_down_pct=STABLE_MAX_DOWN_PCT),
    VolumeCondition(hard=False),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _trading_dates(
    conn:      duckdb.DuckDBPyConnection,
    from_date: date,
    to_date:   date,
    interval:  str,
) -> list[date]:
    """Return sorted distinct trading dates present in candles within range."""
    rows = conn.execute("""
        SELECT DISTINCT CAST(datetime AS DATE) AS d
        FROM   candles
        WHERE  CAST(datetime AS DATE) BETWEEN ? AND ?
          AND  interval = ?
        ORDER  BY d
    """, [from_date, to_date, interval]).fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# Main simulation
# ---------------------------------------------------------------------------

def run_backtest(
    from_date:          date,
    to_date:            date,
    conditions          = _CONDITIONS,
    consolidation_days: int    = CONSOLIDATION_DAYS,
    interval:           str    = INTERVAL,
    threshold:          float  = IMPULSE_THRESHOLD,
    db_path:            str    = DB_PATH,
    out                        = None,
) -> dict:
    """
    Full day-by-day backtest — read-only.

    Candles must already be in DuckDB (run src.ingestor first).
    Opens the DB with read_only=True — zero writes.

    Steps
    -----
    1. Verify candles exist for the requested range.
    2. For each trading day D:
         a. find_impulses(D)             — reads candles
         b. compute_funnel_state(D)      — reads candles
         c. Print: Day 0 / Consolidating / Watchlist / Fallouts
    """
    p       = lambda *a, **kw: print(*a, file=(out or sys.stdout), **kw)  # noqa: E731
    t_start = time.time()
    conn    = _open_readonly(db_path)

    p(_SEP)
    p(f"  BACKTEST  ·  {from_date} → {to_date}")
    p(f"  interval {interval}  ·  threshold ≥ {threshold}%  ·  read-only")
    p(_SEP)

    # ── 1. Verify candles are present for the requested range ─────────────────
    trading_days = _trading_dates(conn, from_date, to_date, interval)
    if not trading_days:
        p(
            f"\n  No candle data found for {from_date} → {to_date} (interval={interval}).\n"
            "  Load candles first:\n"
            f"    python -m src.ingestor --from {from_date} --to {to_date} --lookback 90\n"
        )
        conn.close()
        return {"trading_days": 0, "impulses": 0, "watchlist": 0}
    p(f"\n  {len(trading_days)} trading days in candles  ·  {from_date} → {to_date}\n")

    # ── 3. Day-by-day simulation ──────────────────────────────────────────────
    # accumulated: every impulse seen so far, keyed by (ticker, impulse_date)
    # so the same signal is never double-counted.
    accumulated: dict[tuple[str, date], ImpulseSignal] = {}
    total_impulses  = 0
    total_watchlist = 0

    for i, d in enumerate(trading_days, 1):
        # a) Detect new impulses from candles for today
        new_signals = find_impulses(conn, d, threshold, interval)
        for sig in new_signals:
            key = (sig.ticker, sig.trade_date)
            if key not in accumulated:
                accumulated[key] = sig
                total_impulses += 1

        # b) Active impulses = within the consolidation look-back window
        window_start    = d - dt.timedelta(days=consolidation_days + 2)
        active_impulses = [
            sig
            for (ticker, imp_date), sig in accumulated.items()
            if window_start <= imp_date <= d
        ]

        # c) Pure funnel-state compute — reads only candles
        snapshots = compute_funnel_state(
            conn, d, active_impulses, conditions, consolidation_days, interval
        )

        # Bucket by state
        day0          = [s for s in snapshots if s.state == StockState.IMPULSE]
        consolidating = [s for s in snapshots if s.state == StockState.CONSOLIDATING]
        watchlist     = [s for s in snapshots if s.state == StockState.WATCHLIST]
        fallouts      = [s for s in snapshots if s.state == StockState.FALLOUT]

        total_watchlist += len(watchlist)

        # ── Print day block ───────────────────────────────────────────────────
        total_active = len(day0) + len(consolidating) + len(watchlist) + len(fallouts)
        p(_SEP)
        p(f"  {d}  ·  Day {i}/{len(trading_days)}"
          f"  ·  {total_active} active  ·  {len(new_signals)} new impulse(s)")
        p(_SEP)

        p("  ○  DAY 0 — new impulses")
        if day0:
            for s in sorted(day0, key=lambda x: x.ticker):
                sig = accumulated.get((s.ticker, s.impulse_date))
                chg = f"+{sig.change_pct:.1f}%" if sig else ""
                p(f"       {s.ticker:<22}  {chg:<8}  High {s.day0_high:.2f}")
        else:
            p("       (none)")

        p("  ●  CONSOLIDATING")
        if consolidating:
            for s in sorted(consolidating, key=lambda x: (-x.stable_days, x.ticker)):
                sig = accumulated.get((s.ticker, s.impulse_date))
                chg = f"+{sig.change_pct:.1f}%" if sig else ""
                p(f"       {s.ticker:<22}  {chg:<8}  "
                  f"Day {s.stable_days}/{consolidation_days}  ·  High {s.day0_high:.2f}")
        else:
            p("       (none)")

        p("  ◆  WATCHLIST — ready to trade")
        if watchlist:
            for s in sorted(watchlist, key=lambda x: x.ticker):
                sig = accumulated.get((s.ticker, s.impulse_date))
                chg = f"+{sig.change_pct:.1f}%" if sig else ""
                p(f"       {s.ticker:<22}  {chg:<8}  "
                  f"Day {s.stable_days}/{consolidation_days}  ·  High {s.day0_high:.2f}  ·  impulse {s.impulse_date}")
        else:
            p("       (none)")

        p("  ✕  FALLOUTS")
        if fallouts:
            for s in sorted(fallouts, key=lambda x: x.ticker):
                short = s.failure_reason.split("] ", 1)[-1] if "]" in s.failure_reason else s.failure_reason
                sig   = accumulated.get((s.ticker, s.impulse_date))
                chg   = f"+{sig.change_pct:.1f}%" if sig else ""
                p(f"       {s.ticker:<22}  {chg:<8}  {short}")
        else:
            p("       (none)")

        p()

    # ── Footer ────────────────────────────────────────────────────────────────
    elapsed = time.time() - t_start
    p(_SEP)
    p(
        f"  Done  ·  {len(trading_days)} days"
        f"  ·  {total_impulses} impulses"
        f"  ·  {total_watchlist} watchlist hits"
        f"  ·  {elapsed:.1f}s"
    )
    p(_SEP)
    conn.close()
    return {"trading_days": len(trading_days), "impulses": total_impulses, "watchlist": total_watchlist}


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Swing-trade funnel backtest — read-only. "
            "Load candles first with: python -m src.ingestor --lookback N"
        )
    )
    parser.add_argument("--from",      dest="from_date",  required=True,
                        help="Start date YYYY-MM-DD")
    parser.add_argument("--to",        dest="to_date",    default=date.today().isoformat(),
                        help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--threshold", type=float,        default=IMPULSE_THRESHOLD,
                        help=f"Impulse threshold %% (default: {IMPULSE_THRESHOLD})")
    parser.add_argument("--db-path",   default=DB_PATH,
                        help=f"DuckDB file path (default: {DB_PATH})")
    parser.add_argument("--out",       default=None,
                        help="Write full report to this file (e.g. backtest/reports/run.txt)")
    args = parser.parse_args()

    _from = date.fromisoformat(args.from_date)
    _to   = date.fromisoformat(args.to_date)

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8") as fh:
            run_backtest(from_date=_from, to_date=_to,
                         db_path=args.db_path, threshold=args.threshold, out=fh)
        print(f"  Report written → {out_path}")
    else:
        run_backtest(from_date=_from, to_date=_to,
                     db_path=args.db_path, threshold=args.threshold)
