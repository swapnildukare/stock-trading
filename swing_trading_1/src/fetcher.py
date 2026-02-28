from datetime import date, timedelta
import yfinance as yf
from src.models import CandleRecord


def _parse_yf_raw(raw, tickers: list[str], interval: str) -> list[CandleRecord]:
    """Convert a raw yf.download() DataFrame into a flat list of CandleRecords."""
    records: list[CandleRecord] = []
    for ticker in tickers:
        try:
            df = raw[ticker].dropna() if len(tickers) > 1 else raw.dropna()
            for ts, row in df.iterrows():
                records.append(CandleRecord(
                    ticker=ticker,
                    datetime=ts.to_pydatetime(),
                    interval=interval,
                    open=float(row["Open"]),
                    high=float(row["High"]),
                    low=float(row["Low"]),
                    close=float(row["Close"]),
                    volume=float(row["Volume"]),
                ))
        except (KeyError, TypeError):
            pass
    return records


def fetch_candles(
    tickers:      list[str],
    end_date:     date,
    lookback_days: int,
    interval:     str,
) -> list[CandleRecord]:
    """Fetch OHLCV for a single end-date window (lookback_days back from end_date)."""
    start_date = end_date - timedelta(days=lookback_days)
    raw = yf.download(
        tickers,
        start=start_date.isoformat(),
        end=(end_date + timedelta(days=1)).isoformat(),
        interval=interval,
        group_by="ticker",
        auto_adjust=True,
        progress=False,
    )
    return _parse_yf_raw(raw, tickers, interval)


def fetch_candles_range(
    tickers:   list[str],
    from_date: date,
    to_date:   date,
    interval:  str,
) -> list[CandleRecord]:
    """
    Bulk-fetch OHLCV for an explicit date range in a SINGLE yfinance API call.

    Use this instead of calling fetch_candles() in a loop when processing
    multiple dates — avoids N round-trips and reduces rate-limit risk.

    Returns a flat list of CandleRecords covering all tickers and all dates
    in [from_date, to_date].
    """
    raw = yf.download(
        tickers,
        start=from_date.isoformat(),
        end=(to_date + timedelta(days=1)).isoformat(),  # end is exclusive
        interval=interval,
        group_by="ticker",
        auto_adjust=True,
        progress=False,
    )
    return _parse_yf_raw(raw, tickers, interval)
