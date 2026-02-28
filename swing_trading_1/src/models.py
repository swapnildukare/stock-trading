from datetime import date, datetime
from enum import Enum
from typing import Literal
from pydantic import BaseModel, Field


class StockState(str, Enum):
    IMPULSE       = "impulse"       # Day 0 — just hit the threshold
    CONSOLIDATING = "consolidating" # Days 1-3 — stable, accumulating
    WATCHLIST     = "watchlist"     # Day 4 passed — ready for entry
    FALLOUT       = "fallout"       # broke stability rule, removed from funnel


class IngestionConfig(BaseModel):
    """Parameters that drive a single ingestion run."""
    end_date:      date       = Field(default_factory=date.today)
    lookback_days: int        = Field(default=1, ge=1)
    interval:      str        = Field(default="1d")
    tickers:       list[str]  = Field(default_factory=list)
    db_path:       str        = Field(default="data/market.duckdb")


class CandleRecord(BaseModel):
    """Single OHLCV candle row written to DuckDB."""
    ticker:      str
    datetime:    datetime
    interval:    str
    open:        float
    high:        float
    low:         float
    close:       float
    volume:      float
    ingested_at: datetime = Field(default_factory=datetime.utcnow)


class ImpulseSignal(BaseModel):
    """A stock that moved >= threshold % on a given date."""
    ticker:      str
    trade_date:  date
    open:        float
    close:       float
    change_pct:  float
    direction:   Literal["BULL", "BEAR"]
    interval:    str
    detected_at: datetime = Field(default_factory=datetime.utcnow)


class RunLog(BaseModel):
    """Tracks each pipeline run so missed days can be caught up."""
    run_date:          date
    status:            Literal["success", "failed"]
    tickers_processed: int
    candles_written:   int
    impulses_found:    int
    ran_at:            datetime = Field(default_factory=datetime.utcnow)
    error:             str = ""


class FunnelSnapshot(BaseModel):
    """
    The computed state of one swing trade candidate on a specific trading date.

    Design principles
    -----------------
    • Append-only: one row per (ticker, snapshot_date). Never mutated.
    • Written with INSERT OR IGNORE — re-running the same date is a silent no-op.
    • Computed fresh from raw candles + impulse_signals every time a date is
      processed. No state is carried forward between runs.

    Why append-only beats mutable state
    ------------------------------------
    Mutable rows (UPDATE stable_days each day) break when you re-run a past
    date out of order — the guard condition (last_updated < today) can corrupt
    rows that were already correctly processed for later dates.

    With INSERT OR IGNORE:
    • Re-run today twice  → second run is silently skipped  ✓
    • Re-run a past date  → all existing rows untouched     ✓
    • Run dates out of order → identical final state        ✓

    UI queries
    ----------
    Today's full funnel:
        SELECT * FROM funnel_snapshots WHERE snapshot_date = CURRENT_DATE

    Ticker history (all states over time):
        SELECT * FROM funnel_snapshots WHERE ticker = 'RELIANCE.NS' ORDER BY snapshot_date

    Current watchlist:
        SELECT * FROM funnel_snapshots WHERE snapshot_date = CURRENT_DATE AND state = 'watchlist'

    Recent fallouts with reasons:
        SELECT * FROM funnel_snapshots WHERE state = 'fallout' ORDER BY snapshot_date DESC
    """
    ticker:         str
    snapshot_date:  date                    # the trading date this row was computed for
    impulse_date:   date                    # Day 0 — the +6% event date
    state:          StockState              # funnel bucket
    stable_days:    int        = 0          # 0 on Day 0, max = CONSOLIDATION_DAYS
    day0_high:      float      = 0.0        # stability anchor (Day 0 high price)
    day0_volume:    float      = 0.0        # Day 0 volume — for volume flagging
    failure_reason: str        = ""         # if state=FALLOUT: explains why and which day
