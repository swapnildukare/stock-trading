from datetime import date, timedelta
import yfinance as yf
from src.models import CandleRecord


def fetch_candles(
    tickers: list[str],
    end_date: date,
    lookback_days: int,
    interval: str,
) -> list[CandleRecord]:
    """Fetch OHLCV from yfinance and return a list of CandleRecords."""
    start_date = end_date - timedelta(days=lookback_days)

    raw = yf.download(
        tickers,
        start=start_date.isoformat(),
        end=(end_date + timedelta(days=1)).isoformat(),  # end is exclusive
        interval=interval,
        group_by="ticker",
        auto_adjust=True,
        progress=False,
    )

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
