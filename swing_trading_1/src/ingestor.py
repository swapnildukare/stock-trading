"""
Ingestion service — fetches OHLCV and writes to DuckDB.

Daily run (today, 1d interval):
    python -m src.ingestor

Backtest fill (90 days of 1h candles):
    python -m src.ingestor --date 2026-02-27 --lookback 90 --interval 1h

Custom tickers:
    python -m src.ingestor --tickers RELIANCE.NS TCS.NS
"""

import argparse
import sys
from datetime import date
from pathlib import Path

# Allow running from swing_trading_1/ root
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import WATCHLIST, NSE_INDEX, INTERVAL
from src.nse_fetcher import resolve_tickers
from src.models import IngestionConfig
from src.fetcher import fetch_candles
from src.db import get_conn, upsert_candles


def build_config(args: argparse.Namespace) -> IngestionConfig:
    tickers = args.tickers or WATCHLIST or resolve_tickers(NSE_INDEX)
    return IngestionConfig(
        end_date=date.fromisoformat(args.date),
        lookback_days=args.lookback,
        interval=args.interval,
        tickers=tickers,
        db_path=args.db_path,
    )


def run(cfg: IngestionConfig) -> None:
    print(f"[ingestor] {cfg.end_date}  lookback={cfg.lookback_days}d  "
          f"interval={cfg.interval}  tickers={len(cfg.tickers)}")

    records = fetch_candles(cfg.tickers, cfg.end_date, cfg.lookback_days, cfg.interval)
    print(f"[ingestor] Fetched {len(records)} candles")

    conn = get_conn(cfg.db_path)
    written = upsert_candles(conn, records)
    conn.close()

    print(f"[ingestor] Written {written} rows → {cfg.db_path}")


def main():
    parser = argparse.ArgumentParser(description="OHLCV ingestion service")
    parser.add_argument("--date",     default=date.today().isoformat(), help="End date YYYY-MM-DD")
    parser.add_argument("--lookback", default=1, type=int,              help="Days to look back")
    parser.add_argument("--interval", default=INTERVAL,                 help="Candle interval e.g. 1d 1h 15m")
    parser.add_argument("--tickers",  nargs="*",                        help="Override tickers list")
    parser.add_argument("--db-path",  default="data/market.duckdb",     help="DuckDB file path")
    args = parser.parse_args()

    cfg = build_config(args)
    run(cfg)


if __name__ == "__main__":
    main()
