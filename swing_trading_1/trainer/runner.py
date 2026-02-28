"""
trainer/runner.py
=================

Grid-search over (threshold, consolidation_days, max_up_pct, max_down_pct)
and rank runs by watchlist hits.

Each combination runs the backtest engine with output suppressed.
Results are ranked by:  watchlist hits  →  conversion rate  (descending)

Usage
-----
    python -m trainer.runner --from 2025-11-01 --to 2026-02-27

    # Custom grids (comma-separated values)
    python -m trainer.runner --from 2025-11-01 \\
        --threshold 6,7,8 --days 4,5,6 --up 1,2,3 --down 1.5,2,3
"""

from __future__ import annotations

import argparse
import io
import itertools
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DB_PATH, INTERVAL
from backtest.engine import run_backtest
from src.conditions import StabilityCondition, VolumeCondition


def _grid(args) -> list[tuple]:
    thresholds = [float(x) for x in args.threshold.split(",")]
    days_list  = [int(x)   for x in args.days.split(",")]
    ups        = [float(x) for x in args.up.split(",")]
    downs      = [float(x) for x in args.down.split(",")]
    return list(itertools.product(thresholds, days_list, ups, downs))


def run(from_date: date, to_date: date, combos: list[tuple], db_path: str) -> list[dict]:
    results = []
    n       = len(combos)
    for i, (threshold, days, up, down) in enumerate(combos, 1):
        conds = [StabilityCondition(max_up_pct=up, max_down_pct=down), VolumeCondition(hard=False)]
        print(f"\r  [{i:>{len(str(n))}}/{n}]  thresh={threshold}  days={days}  "
              f"up={up}  down={down}  ...", end="", flush=True)
        stats = run_backtest(
            from_date          = from_date,
            to_date            = to_date,
            conditions         = conds,
            consolidation_days = days,
            threshold          = threshold,
            db_path            = db_path,
            out                = io.StringIO(),   # suppress output
        )
        conv = (stats["watchlist"] / stats["impulses"] * 100) if stats["impulses"] else 0.0
        results.append({
            "threshold": threshold,
            "days":      days,
            "up":        up,
            "down":      down,
            "watchlist": stats["watchlist"],
            "impulses":  stats["impulses"],
            "conv_pct":  conv,
        })
    print()   # newline after progress
    return results


def print_table(results: list[dict], top: int = 20) -> None:
    ranked = sorted(results, key=lambda r: (-r["watchlist"], -r["conv_pct"]))[:top]
    W      = 76
    SEP    = "━" * W

    print(f"\n{SEP}")
    print(f"  TRAINER RESULTS  —  top {min(top, len(ranked))} of {len(results)} combinations")
    print(SEP)
    hdr = f"  {'#':>3}  {'thresh':>6}  {'days':>4}  {'up%':>4}  {'down%':>5}  {'watchlist':>9}  {'impulses':>8}  {'conv%':>6}"
    print(hdr)
    print(f"  {'─'*3}  {'─'*6}  {'─'*4}  {'─'*4}  {'─'*5}  {'─'*9}  {'─'*8}  {'─'*6}")
    for rank, r in enumerate(ranked, 1):
        print(
            f"  {rank:>3}  {r['threshold']:>6.1f}  {r['days']:>4}  "
            f"{r['up']:>4.1f}  {r['down']:>5.1f}  "
            f"{r['watchlist']:>9}  {r['impulses']:>8}  {r['conv_pct']:>5.1f}%"
        )
    print(SEP)

    best = ranked[0]
    print(
        f"\n  Best  →  threshold={best['threshold']}  days={best['days']}  "
        f"up={best['up']}%  down={best['down']}%"
        f"  →  {best['watchlist']} watchlist  ({best['conv_pct']:.1f}% conversion)\n"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Grid-search backtest params — ranks by watchlist hits."
    )
    parser.add_argument("--from",      dest="from_date", required=True,  help="Start date YYYY-MM-DD")
    parser.add_argument("--to",        dest="to_date",   default=date.today().isoformat(), help="End date")
    parser.add_argument("--threshold", default="6,7,8",  help="Impulse threshold %% values  (default: 6,7,8)")
    parser.add_argument("--days",      default="4,5,6",  help="Consolidation days           (default: 4,5,6, min 4)")
    parser.add_argument("--up",        default="1,2,3",  help="Max up %% from day0_high     (default: 1,2,3)")
    parser.add_argument("--down",      default="1,2,3",  help="Max down %% from day0_high   (default: 1,2,3)")
    parser.add_argument("--top",       type=int, default=20, help="Rows to show in table    (default: 20)")
    parser.add_argument("--db-path",   default=DB_PATH,  help=f"DuckDB path (default: {DB_PATH})")
    args = parser.parse_args()

    # enforce min consolidation_days = 4
    days_list = [max(4, int(x)) for x in args.days.split(",")]
    args.days = ",".join(str(d) for d in days_list)

    combos = _grid(args)
    print(f"  {len(combos)} combinations  ·  {args.from_date} → {args.to_date}")

    results = run(
        from_date = date.fromisoformat(args.from_date),
        to_date   = date.fromisoformat(args.to_date),
        combos    = combos,
        db_path   = args.db_path,
    )
    print_table(results, top=args.top)
