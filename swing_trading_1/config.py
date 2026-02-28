# --- Swing Trading Config ---

IMPULSE_THRESHOLD = 8.0    # pipeline: min daily % change to flag an impulse
INTERVAL          = "1d"
PERIOD            = "1d"   # look back window

# DuckDB file (relative to swing_trading_1/)
DB_PATH = "data/market.duckdb"

# NSE index to fall back to when WATCHLIST is empty.
# Options: NIFTY_50 | NIFTY_100 | NIFTY_200 | NIFTY_500
NSE_INDEX = "NIFTY_500"

# Manual watchlist (Yahoo Finance .NS format).
# Leave empty [] to auto-fetch from NSE_INDEX above.
# WATCHLIST = [
#     "RELIANCE.NS", "TCS.NS", "INFY.NS", "TEJASNET.NS", "ICICIBANK.NS"
# ]

WATCHLIST = []

# --- Funnel / Consolidation Config ---
# Number of stable days required before a candidate moves to WATCHLIST.
CONSOLIDATION_DAYS   = 4

# Stability window anchored on Day 0 HIGH.
#   Max allowed rise  : day0_high * (1 + STABLE_MAX_UP_PCT   / 100)
#   Max allowed drop  : day0_high * (1 - STABLE_MAX_DOWN_PCT / 100)
STABLE_MAX_UP_PCT    = 2.0   # +2%  ceiling
STABLE_MAX_DOWN_PCT  = 2.0   # -2%  floor