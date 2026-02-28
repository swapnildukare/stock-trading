"""
Funnel Processor — pure compute layer, zero DB writes.
=======================================================

Responsibilities
----------------
Given a snapshot_date and the raw tables already in DuckDB (candles +
impulse_signals), compute the complete funnel state for every active
candidate as a list of FunnelSnapshot objects.

The pipeline then writes those objects to funnel_snapshots via
INSERT OR IGNORE (see db.py).

Why this is a pure function, not a state machine
-------------------------------------------------
An earlier design tracked mutable state (stable_days, last_updated) in a
candidates table and advanced it one day at a time. That approach broke
down when re-running past dates out of order — guards like
"last_updated < today" could corrupt rows that were already correctly
written for later dates.

The current approach computes each day's output entirely from first
principles:

  For snapshot_date D, for a given impulse that fired on date I:

    1. Query candles for dates (I+1 ... D) — the consolidation window.
    2. Check each of those days against every Condition (StabilityCondition,
       VolumeCondition, etc.).
    3. Count how many consecutive days passed before the first failure.
    4. Derive state: IMPULSE / CONSOLIDATING / WATCHLIST / FALLOUT.

  No prior state is read. No row is mutated. The function only needs
  candles and impulse_signals in DuckDB, which are both immutable.

  Consequence: process any date range in any order → identical results.

Duplicate impulse handling
--------------------------
If a ticker fires multiple impulses (e.g. +6% again while consolidating),
each impulse_date is treated as an independent swing attempt. The snapshot
for each (ticker, impulse_date, snapshot_date) is computed separately.
There is no "supersede" step — old attempts complete their 4-day window
independently and their history is fully preserved.

Standalone tracker display
--------------------------
    python -m src.funnel_processor

    Reads today's snapshot from funnel_snapshots and pretty-prints
    the current watchlist, active consolidators, and recent fallouts.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import duckdb

from src.conditions import Condition, DayCandle, FunnelContext, StabilityCondition, VolumeCondition
from src.models import FunnelSnapshot, ImpulseSignal, StockState


# ---------------------------------------------------------------------------
# Day candle loader
# ---------------------------------------------------------------------------

def fetch_day_candles(
    conn:       duckdb.DuckDBPyConnection,
    trade_date: date,
    interval:   str = "1d",
) -> dict[str, DayCandle]:
    """
    Read one day's OHLCV from the candles table.
    Returns { ticker: DayCandle } for O(1) lookups during condition evaluation.
    """
    rows = conn.execute("""
        SELECT
            ticker,
            high,
            low,
            close,
            volume,
            CASE WHEN open > 0 THEN (close - open) / open * 100 ELSE 0 END
        FROM candles
        WHERE CAST(datetime AS DATE) = ?
          AND interval = ?
          AND open > 0
    """, [trade_date, interval]).fetchall()

    return {
        row[0]: DayCandle(
            ticker=row[0], high=row[1], low=row[2],
            close=row[3], volume=row[4], change_pct=row[5],
        )
        for row in rows
    }


# ---------------------------------------------------------------------------
# Core compute function
# ---------------------------------------------------------------------------

def compute_funnel_state(
    conn:               duckdb.DuckDBPyConnection,
    snapshot_date:      date,
    impulses:           list[ImpulseSignal],
    conditions:         list[Condition],
    consolidation_days: int = 4,
    interval:           str = "1d",
) -> list[FunnelSnapshot]:
    """
    Compute the funnel state for all active impulses as of snapshot_date.

    For each impulse signal:
      • If impulse_date == snapshot_date → state=IMPULSE, stable_days=0
        (Day 0: just detected, no consolidation check yet)

      • If impulse_date < snapshot_date → walk candles from impulse_date+1
        to snapshot_date, applying each Condition day by day:
          - First failure  → FALLOUT  (failure_reason = which day + why)
          - All days pass  → stable_days = days walked
          - stable_days >= consolidation_days → WATCHLIST
          - Otherwise      → CONSOLIDATING

    Candle data for both Day 0 (anchor) and Days 1-4 is loaded from DuckDB.
    No external state is read or written. This is a pure function.

    Args:
        conn           : Open DuckDB connection (read-only use).
        snapshot_date  : The trading date we are computing state for.
        impulses       : All impulse signals still potentially within their
                         consolidation window (caller filters by age).
        conditions     : Ordered list of Condition strategies to evaluate.
        consolidation_days : Days required to graduate to WATCHLIST (default 4).
        interval       : Candle interval to read (must match ingestion interval).

    Returns:
        List of FunnelSnapshot objects — one per impulse, ready for INSERT OR IGNORE.
    """
    snapshots: list[FunnelSnapshot] = []

    for sig in impulses:
        # --- Day 0 anchor: read the impulse day's full candle ---
        day0_candle = fetch_day_candles(conn, sig.trade_date, interval).get(sig.ticker)
        day0_high   = day0_candle.high   if day0_candle else sig.close
        day0_vol    = day0_candle.volume if day0_candle else 0.0

        # --- Day 0 itself: just record as IMPULSE, no stability check ---
        if sig.trade_date == snapshot_date:
            snapshots.append(FunnelSnapshot(
                ticker        = sig.ticker,
                snapshot_date = snapshot_date,
                impulse_date  = sig.trade_date,
                state         = StockState.IMPULSE,
                stable_days   = 0,
                day0_high     = day0_high,
                day0_volume   = day0_vol,
            ))
            continue

        # --- Days 1-N: walk candles from impulse_date+1 to snapshot_date ---
        import datetime as dt
        stable_days   = 0
        fallout       = False
        failure_note  = ""

        check_date = sig.trade_date + dt.timedelta(days=1)
        while check_date <= snapshot_date:
            day_candles = fetch_day_candles(conn, check_date, interval)
            candle      = day_candles.get(sig.ticker)

            if candle is None:
                # No data for this day (holiday / data gap) — skip, don't penalise
                check_date += dt.timedelta(days=1)
                continue

            ctx = FunnelContext(
                day0_high   = day0_high,
                day0_volume = day0_vol,
                stable_days = stable_days,
            )

            for cond in conditions:
                ok, note = cond.evaluate(ctx, candle)
                if not ok:
                    fallout      = True
                    failure_note = f"[{cond.name}] {note}"
                    break

            if fallout:
                break

            stable_days += 1
            check_date  += dt.timedelta(days=1)

        # --- Determine final state ---
        if fallout:
            state = StockState.FALLOUT
        elif stable_days >= consolidation_days:
            state = StockState.WATCHLIST
        elif stable_days > 0:
            state = StockState.CONSOLIDATING
        else:
            state = StockState.IMPULSE

        snapshots.append(FunnelSnapshot(
            ticker        = sig.ticker,
            snapshot_date = snapshot_date,
            impulse_date  = sig.trade_date,
            state         = state,
            stable_days   = stable_days,
            day0_high     = day0_high,
            day0_volume   = day0_vol,
            failure_reason= failure_note,
        ))

    return snapshots


# ---------------------------------------------------------------------------
# Tracker display
# ---------------------------------------------------------------------------

def print_tracker(
    conn:               duckdb.DuckDBPyConnection,
    consolidation_days: int = 4,
    as_of:              date | None = None,
) -> None:
    """Print the current funnel state from funnel_snapshots in four sections."""
    target = as_of or date.today()
    W      = 56
    SEP    = "━" * W

    # Join with impulse_signals to get change_pct for display
    rows = conn.execute("""
        SELECT
            f.ticker,
            f.state,
            f.stable_days,
            f.day0_high,
            f.impulse_date,
            COALESCE(i.change_pct, 0.0) AS change_pct,
            f.failure_reason
        FROM   funnel_snapshots f
        LEFT JOIN impulse_signals i
               ON i.ticker     = f.ticker
              AND i.trade_date = f.impulse_date
        WHERE  f.snapshot_date = ?
        ORDER  BY
            CASE f.state
                WHEN 'watchlist'     THEN 1
                WHEN 'consolidating' THEN 2
                WHEN 'impulse'       THEN 3
                WHEN 'fallout'       THEN 4
            END,
            f.stable_days DESC,
            f.ticker
    """, [target]).fetchall()

    watchlist     = [r for r in rows if r[1] == "watchlist"]
    consolidating = [r for r in rows if r[1] == "consolidating"]
    impulses      = [r for r in rows if r[1] == "impulse"]

    # Recent fallouts from all history, not just today (last 10)
    fallout_rows = conn.execute("""
        SELECT
            f.ticker,
            f.snapshot_date,
            f.failure_reason,
            COALESCE(i.change_pct, 0.0) AS change_pct
        FROM   funnel_snapshots f
        LEFT JOIN impulse_signals i
               ON i.ticker     = f.ticker
              AND i.trade_date = f.impulse_date
        WHERE  f.state = 'fallout'
        ORDER  BY f.snapshot_date DESC
        LIMIT  10
    """).fetchall()

    print(f"\n{SEP}")
    print(f"  FUNNEL SNAPSHOT  ·  {target}")
    print(SEP)

    # --- WATCHLIST ---
    print("\n  ◆  WATCHLIST  — ready to trade")
    if watchlist:
        for ticker, _, days, anchor, imp_date, chg, _ in watchlist:
            print(f"      {ticker:<18}  +{chg:.1f}%   "
                  f"Day {days}/{consolidation_days}  ·  High {anchor:.2f}  ·  impulse {imp_date}")
    else:
        print("      (none)")

    # --- CONSOLIDATING ---
    print("\n  ●  CONSOLIDATING  — watching the base")
    if consolidating:
        for ticker, _, days, anchor, imp_date, chg, _ in consolidating:
            print(f"      {ticker:<18}  +{chg:.1f}%   "
                  f"Day {days}/{consolidation_days}  ·  High {anchor:.2f}  ·  impulse {imp_date}")
    else:
        print("      (none)")

    # --- DAY 0 impulses ---
    print("\n  ◦  DAY 0  — impulses detected today")
    if impulses:
        for ticker, _, _, anchor, imp_date, chg, _ in impulses:
            print(f"      {ticker:<18}  +{chg:.1f}%   High {anchor:.2f}")
    else:
        print("      (none)")

    # --- FALLOUTS ---
    print("\n  ✕  FALLOUTS")
    if fallout_rows:
        for ticker, snap_date, reason, chg in fallout_rows:
            # strip the condition name prefix [ConditionName] for cleaner reading
            short = reason.split("] ", 1)[-1] if "]" in reason else reason
            print(f"      {ticker:<18}  {snap_date}  {short}")
    else:
        print("      (none)")

    print()


# ---------------------------------------------------------------------------
# Standalone runner — computes live from impulse_signals, no pipeline needed
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    """
    Compute and display today's funnel state directly from impulse_signals
    and candles — does NOT require funnel_snapshots to be populated first.

    This is safe to run at any time, even if pipeline.py hasn't been run
    today or after a schema change that wiped funnel_snapshots.
    """
    import datetime as _dt
    sys.path.insert(0, str(Path(__file__).parent.parent))

    from config import DB_PATH, CONSOLIDATION_DAYS, INTERVAL, STABLE_MAX_UP_PCT, STABLE_MAX_DOWN_PCT
    from src.db import get_conn
    from src.models import ImpulseSignal as _ImpulseSignal

    _conditions = [
        StabilityCondition(max_up_pct=STABLE_MAX_UP_PCT, max_down_pct=STABLE_MAX_DOWN_PCT),
        VolumeCondition(hard=False),
    ]

    conn        = get_conn(DB_PATH)
    today       = _dt.date.today()
    window_start = today - _dt.timedelta(days=CONSOLIDATION_DAYS + 2)

    # Load all impulses within the active consolidation window
    rows = conn.execute("""
        SELECT ticker, trade_date, open, close, change_pct, direction, interval, detected_at
        FROM   impulse_signals
        WHERE  trade_date >= ? AND trade_date <= ? AND interval = ?
        ORDER  BY trade_date
    """, [window_start, today, INTERVAL]).fetchall()

    impulse_objs = [
        _ImpulseSignal(
            ticker=r[0], trade_date=r[1], open=r[2], close=r[3],
            change_pct=r[4], direction=r[5], interval=r[6], detected_at=r[7],
        )
        for r in rows
    ]

    if not impulse_objs:
        print(f"\nNo impulse signals found in window {window_start} → {today}.")
        print("Run: python -m src.pipeline  to ingest and detect impulses first.")
        conn.close()
        sys.exit(0)

    # Compute live state — pure function, no writes
    snapshots = compute_funnel_state(
        conn, today, impulse_objs, _conditions, CONSOLIDATION_DAYS, INTERVAL
    )

    # --- Pretty print without needing funnel_snapshots to be populated ---
    print("\n" + "=" * 50)
    print("         SWING TRADE TRACKER  (live compute)")
    print("=" * 50)

    active   = [s for s in snapshots if s.state.value in ("watchlist", "consolidating")]
    new_day0 = [s for s in snapshots if s.state.value == "impulse"]
    fallout  = [s for s in snapshots if s.state.value == "fallout"]

    print(f"\n[ACTIVE FUNNEL]  ({today})")
    if active:
        for s in sorted(active, key=lambda x: (-x.stable_days, x.state.value)):
            label = (
                f"READY TO TRADE! (Day {s.stable_days}/{CONSOLIDATION_DAYS})"
                if s.state.value == "watchlist"
                else f"Consolidating (Day {s.stable_days}/{CONSOLIDATION_DAYS})"
            )
            print(f"  • {s.ticker:<18} {label:<40} | Day0 High: {s.day0_high:.2f}  | Impulse: {s.impulse_date}")
    else:
        print("  (none)")

    print("\n[NEW IMPULSES — DAY 0]")
    if new_day0:
        for s in new_day0:
            print(f"  • {s.ticker:<18} Day0 High: {s.day0_high:.2f}  | Date: {s.impulse_date}")
    else:
        print("  (none)")

    print("\n[RECENT FALLOUTS]")
    if fallout:
        for s in sorted(fallout, key=lambda x: x.impulse_date, reverse=True):
            print(f"  ✕ {s.ticker:<18} {s.failure_reason}")
    else:
        print("  (none)")

    print()
    conn.close()
