# Future Scope

Planned features and improvements, tracked here so nothing gets forgotten.

---

## Data & Ingestion

- [ ] **Missing ticker notifications** — detect when yfinance returns no data or NaN
  for a ticker during ingestion and raise an alert (log warning, send notification,
  or write to a `failed_tickers` table). Useful when NSE stocks are suspended,
  delisted, or Yahoo Finance has a data gap.

- [ ] **Intraday intervals** — extend pipeline to support 5m / 15m / 1h candles
  alongside daily for more granular impulse detection.

- [ ] **Data quality checks** — flag candles where volume = 0 or OHLC values are
  clearly erroneous (e.g. close > high).

---

## Signals & Strategy

- [ ] **4-day consolidation detector** — after an impulse day, check whether the
  stock consolidates (low volatility, range-bound) over the next 4 sessions.
  This is Step 2 of the swing trading strategy.
  *(Implemented via `StabilityCondition` in `src/conditions.py` — see below.)*

- [ ] **`VolumeCondition` (hard gate)** — `VolumeCondition` is already implemented
  in `src/conditions.py` as a **soft** flag (warns but does not eject). When ready
  to enforce it, pass `VolumeCondition(hard=True)` in the `_CONDITIONS` list in
  `pipeline.py`. No other changes needed — this is the Strategy Pattern payoff.

- [ ] **`RSICondition`** — add an RSI check during consolidation: require RSI to
  stay between 40–60 (cooling off without going oversold). Implement by subclassing
  `Condition` in `src/conditions.py` and appending to `_CONDITIONS` in `pipeline.py`.
  Example skeleton:
  ```python
  class RSICondition(Condition):
      name = "RSICondition"
      def __init__(self, low: float = 40, high: float = 60): ...
      def evaluate(self, candidate, candle) -> tuple[bool, str]: ...
  ```

- [ ] **`ATRCondition`** — flag stocks whose consolidation volatility (ATR) is
  abnormally high for the base period. Plugs into the same Strategy interface.

- [ ] **Entry signal** — trigger a trade signal when price breaks out of the
  consolidation range after the impulse + 4-day base.

- [ ] **Stop-loss / target levels** — compute SL (below base low) and target
  (1.5x or 2x risk) automatically per signal.

- [ ] **Multi-timeframe confirmation** — cross-check daily impulse with weekly
  trend direction before generating a signal.

---

## Notifications & Alerts

- [ ] **Missing ticker alert** — notify (email / Slack / Telegram) when a
  scheduled ticker fails to fetch data.

- [ ] **Daily signal digest** — send a summary of impulse signals found each day.

- [ ] **Price alert** — notify when a consolidating stock breaks out.

---

## Backtesting

- [ ] **Backtest runner** — replay historical impulse + consolidation signals
  and compute win rate, average return, max drawdown.

- [ ] **Polars-based performance report** — use Polars lazy API for large
  historical datasets without memory pressure.

---

## Infrastructure

- [ ] **Scheduler** — cron / APScheduler to auto-run `pipeline.py` at market
  close each day (e.g. 15:35 IST).

- [ ] **Config via environment variables** — allow overriding `config.py` values
  via `.env` for deployment flexibility.

- [ ] **Parquet export** — periodic export of DuckDB tables to Parquet for
  long-term archival and external analysis.
