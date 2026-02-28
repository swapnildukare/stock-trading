from datetime import date
import duckdb
import polars as pl
from src.models import ImpulseSignal


def find_impulses(
    conn: duckdb.DuckDBPyConnection,
    trade_date: date,
    threshold: float,
    interval: str = "1d",
) -> list[ImpulseSignal]:
    """
    Read candles via DuckDB → analyse with Polars → return ImpulseSignal objects.
    """
    # Step 1: Read raw data via DuckDB API → Polars DataFrame
    df: pl.DataFrame = conn.execute("""
        SELECT
            ticker,
            CAST(datetime AS DATE)         AS trade_date,
            open,
            close,
            ((close - open) / open * 100)  AS change_pct
        FROM candles
        WHERE CAST(datetime AS DATE) = ?
          AND interval = ?
          AND open > 0
    """, [trade_date, interval]).pl()

    # Step 2: Analyse with Polars — filter only positive moves >= threshold (BULL only)
    hits = (
        df
        .with_columns(
            pl.lit("BULL").alias("direction"),
        )
        .filter(pl.col("change_pct") >= threshold)
    )

    # Step 3: Convert Polars rows → Pydantic ImpulseSignal objects
    return [
        ImpulseSignal(
            ticker=row["ticker"],
            trade_date=row["trade_date"],
            open=round(row["open"], 2),
            close=round(row["close"], 2),
            change_pct=round(row["change_pct"], 2),
            direction=row["direction"],
            interval=interval,
        )
        for row in hits.to_dicts()
    ]


if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))

    from config import DB_PATH, IMPULSE_THRESHOLD, INTERVAL
    from src.db import get_conn

    _date = date.today()
    conn    = get_conn(DB_PATH)
    signals = find_impulses(conn, _date, IMPULSE_THRESHOLD, INTERVAL)
    conn.close()

    print(f"\nImpulses for {_date}  (>={IMPULSE_THRESHOLD}%)  —  {len(signals)} found\n")
    if signals:
        df = pl.DataFrame([s.model_dump() for s in signals]).select(
            "ticker", "trade_date", "open", "close", "change_pct", "direction"
        ).sort("change_pct", descending=True)
        print(df)
    else:
        print("No impulses found for this date.")
