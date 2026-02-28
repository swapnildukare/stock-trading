"""
Delta Drive Strategy - Full Working Implementation
=================================================
Nifty Options Intraday Scalping Strategy

Usage:
    # Live mode (uses real system time):
    python delta_drive_strategy.py

    # Simulation mode (override time + inject mock data):
    python delta_drive_strategy.py --simulate --sim-time "2024-01-16 11:30" --vix 15.5 --spot 25334

Features:
    - Pre-flight checks (day, time window, VIX)
    - VWAP-based directional filter
    - ITM option selection
    - Trailing stop-loss engine
    - Simulation / paper-trading mode with mock data injection
    - Colored console P&L dashboard
"""

import time
import argparse
import random
import sys
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Optional
import math

# ──────────────────────────────────────────────────────────────────────────────
# ANSI Colors
# ──────────────────────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
WHITE  = "\033[97m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def color(text, c): return f"{c}{text}{RESET}"

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
LOT_SIZE        = 25          # Nifty lot size
NIFTY_STEP      = 50          # Strike price step
TRADE_WINDOW    = (10.5, 14.25)  # Hours (10:30 AM to 2:15 PM IST)
VIX_MIN, VIX_MAX = 12, 18
MAX_TRADE_SECONDS = 3600      # 60-minute hard exit
POLL_INTERVAL   = 2           # seconds between price checks (live)
SIM_POLL_INTERVAL = 0.3       # seconds between checks (simulation)

# ──────────────────────────────────────────────────────────────────────────────
# Simulation Clock
# ──────────────────────────────────────────────────────────────────────────────
class Clock:
    """
    Provides current time. In simulation mode, time advances at accelerated
    pace or can be stepped manually.
    """
    def __init__(self, sim_time: Optional[datetime] = None, speed: float = 60.0):
        self._sim_mode = sim_time is not None
        self._start_wall = time.time()
        self._sim_start  = sim_time
        self._speed      = speed  # 1 real second = `speed` simulated seconds

    def now(self) -> datetime:
        if not self._sim_mode:
            return datetime.now()
        elapsed_wall = time.time() - self._start_wall
        sim_elapsed  = timedelta(seconds=elapsed_wall * self._speed)
        return self._sim_start + sim_elapsed

    def timestamp(self) -> float:
        return self.now().timestamp()

    @property
    def is_simulation(self): return self._sim_mode


# ──────────────────────────────────────────────────────────────────────────────
# Mock Market Data Engine (Simulation)
# ──────────────────────────────────────────────────────────────────────────────
class MockMarketData:
    """
    Generates realistic-ish mock market data for simulation.
    Prices follow a GBM-like random walk.
    """
    def __init__(self, spot: float, vix: float, direction: str = "AUTO"):
        self.spot      = spot
        self.vix       = vix
        self._direction = direction
        self._option_prices: dict[str, float] = {}
        self._mu    = 0.0
        self._sigma = (vix / 100) / math.sqrt(252 * 375)  # per-tick vol

    def _walk(self, price: float, drift: float = 0.0) -> float:
        """Random walk step."""
        shock = random.gauss(drift, self._sigma * price)
        return max(price + shock, 1.0)

    def tick(self):
        """Advance all prices by one tick."""
        drift = 0.02 if self._direction == "BULLISH" else (-0.02 if self._direction == "BEARISH" else 0.0)
        self.spot = self._walk(self.spot, drift)
        for k in list(self._option_prices):
            self._option_prices[k] = self._walk(self._option_prices[k], drift * 0.8)

    def get_ltp(self, symbol: str) -> float:
        if symbol in ("NIFTY 50", "NIFTY50"):
            return round(self.spot, 2)
        # Option contract
        if symbol not in self._option_prices:
            # Seed option price using simple intrinsic + time value
            parts  = symbol.split("_")   # e.g. NIFTY_25300_CE
            strike = float(parts[1])
            otype  = parts[2]
            intrinsic = max(0.0, self.spot - strike) if otype == "CE" else max(0.0, strike - self.spot)
            time_val  = self.spot * self._sigma * 20   # rough theta proxy
            self._option_prices[symbol] = round(intrinsic + time_val + random.uniform(5, 30), 2)
        return round(self._option_prices[symbol], 2)

    def get_vwap(self, symbol: str) -> float:
        # VWAP slightly below spot in bullish, above in bearish
        if self._direction == "BULLISH":
            return round(self.spot * random.uniform(0.9985, 0.9998), 2)
        elif self._direction == "BEARISH":
            return round(self.spot * random.uniform(1.0002, 1.0015), 2)
        return round(self.spot * random.uniform(0.9990, 1.0010), 2)

    def get_vix(self) -> float:
        return round(self.vix + random.uniform(-0.2, 0.2), 2)

    def get_pcr(self) -> float:
        return round(random.uniform(0.7, 1.4), 2)

    def determine_sentiment(self, pcr: float) -> str:
        if self._direction != "AUTO":
            return self._direction
        return "BULLISH" if pcr > 1.1 else "BEARISH" if pcr < 0.9 else "NEUTRAL"


# ──────────────────────────────────────────────────────────────────────────────
# Live Market Data Stubs (replace with your broker API)
# ──────────────────────────────────────────────────────────────────────────────
class LiveMarketData:
    """
    Replace method bodies with actual broker API calls.
    e.g., Zerodha KiteConnect, Upstox, Angel One SmartAPI, etc.
    """
    def get_ltp(self, symbol: str) -> float:
        raise NotImplementedError(
            "Implement get_ltp() with your broker SDK.\n"
            "Example (KiteConnect): kite.ltp('NSE:NIFTY 50')['NSE:NIFTY 50']['last_price']"
        )

    def get_vwap(self, symbol: str) -> float:
        raise NotImplementedError(
            "Implement get_vwap() — fetch OHLCV candles and compute cumulative (price*vol)/vol."
        )

    def get_vix(self) -> float:
        raise NotImplementedError(
            "Implement get_vix() — fetch India VIX LTP from NSE."
        )

    def get_pcr(self) -> float:
        raise NotImplementedError(
            "Implement get_pcr() — fetch OI-based Put-Call Ratio from NSE option chain."
        )

    def determine_sentiment(self, pcr: float) -> str:
        """Simple rule-based sentiment. Replace with your LLM/NLP engine if desired."""
        if pcr > 1.1:
            return "BULLISH"
        elif pcr < 0.9:
            return "BEARISH"
        return "NEUTRAL"


# ──────────────────────────────────────────────────────────────────────────────
# Trade Result
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class TradeResult:
    contract:     str
    direction:    str
    entry_price:  float
    exit_price:   float
    pnl:          float
    exit_reason:  str
    duration_sec: float
    tsl_activated: bool
    entry_time:   datetime
    exit_time:    datetime

    def summary(self) -> str:
        sign = GREEN if self.pnl >= 0 else RED
        return (
            f"\n{'='*60}\n"
            f"  {BOLD}TRADE CLOSED{RESET}\n"
            f"  Contract   : {CYAN}{self.contract}{RESET}\n"
            f"  Direction  : {self.direction}\n"
            f"  Entry      : ₹{self.entry_price:.2f}  @  {self.entry_time.strftime('%H:%M:%S')}\n"
            f"  Exit       : ₹{self.exit_price:.2f}  @  {self.exit_time.strftime('%H:%M:%S')}\n"
            f"  Duration   : {self.duration_sec:.0f}s\n"
            f"  TSL Hit    : {'Yes' if self.tsl_activated else 'No'}\n"
            f"  Exit Reason: {self.exit_reason}\n"
            f"  P&L        : {sign}₹{self.pnl:+,.2f}{RESET}\n"
            f"{'='*60}"
        )


# ──────────────────────────────────────────────────────────────────────────────
# Core Strategy Engine
# ──────────────────────────────────────────────────────────────────────────────
class DeltaDriveStrategy:
    def __init__(
        self,
        clock: Clock,
        market,
        stop_loss:   int = 1000,
        target:      int = 1500,
        tsl_trigger: int = 700,
        lot_size:    int = LOT_SIZE,
        verbose:     bool = True,
    ):
        self.clock       = clock
        self.market      = market
        self.stop_loss   = stop_loss
        self.target      = target
        self.tsl_trigger = tsl_trigger
        self.lot_size    = lot_size
        self.verbose     = verbose
        self.trades: list[TradeResult] = []

    # ── Helpers ──────────────────────────────────────────────────────────────
    def _log(self, msg: str):
        if self.verbose:
            ts = self.clock.now().strftime("%H:%M:%S")
            print(f"[{ts}] {msg}")

    def _current_minutes(self) -> float:
        t = self.clock.now()
        return t.hour + t.minute / 60 + t.second / 3600

    def _is_monday(self) -> bool:
        return self.clock.now().weekday() == 0

    def _select_strike(self, spot: float, direction: str) -> tuple[int, str]:
        """Return (strike, option_type) for 1st ITM contract."""
        base = int(spot / NIFTY_STEP) * NIFTY_STEP
        if direction == "BULLISH":
            # 1st ITM Call: strike just below spot
            strike = base
            return strike, "CE"
        else:
            # 1st ITM Put: strike just above spot
            strike = base + NIFTY_STEP
            return strike, "PE"

    # ── Pre-flight ────────────────────────────────────────────────────────────
    def pre_flight_checks(self) -> tuple[bool, str]:
        now = self.clock.now()
        day_name = now.strftime("%A")

        if self._is_monday():
            return False, f"Monday ({now.date()}) — skipping (gap-risk day)."

        cur = self._current_minutes()
        lo, hi = TRADE_WINDOW
        if not (lo <= cur <= hi):
            window_str = f"{int(lo)}:{int((lo % 1)*60):02d}–{int(hi)}:{int((hi%1)*60):02d}"
            return False, f"Outside trading window {window_str} IST. Current: {now.strftime('%H:%M')}."

        vix = self.market.get_vix()
        if not (VIX_MIN <= vix <= VIX_MAX):
            return False, f"VIX {vix:.2f} out of range [{VIX_MIN}–{VIX_MAX}]. Aborting."

        return True, f"Pre-flight OK | Day={day_name} | Time={now.strftime('%H:%M')} | VIX={vix:.2f}"

    # ── Main strategy loop ────────────────────────────────────────────────────
    def run(self) -> Optional[TradeResult]:
        ok, msg = self.pre_flight_checks()
        if not ok:
            self._log(color(f"✗ {msg}", YELLOW))
            return None
        self._log(color(f"✓ {msg}", GREEN))

        # Sentiment
        pcr       = self.market.get_pcr()
        direction = self.market.determine_sentiment(pcr)
        self._log(f"PCR={pcr:.2f} → Sentiment: {color(direction, CYAN)}")

        if direction == "NEUTRAL":
            self._log(color("Neutral sentiment — no trade.", YELLOW))
            return None

        # VWAP filter
        spot = self.market.get_ltp("NIFTY 50")
        vwap = self.market.get_vwap("NIFTY 50")
        self._log(f"Spot={spot:.2f}  VWAP={vwap:.2f}")

        if direction == "BULLISH" and spot <= vwap:
            self._log(color("BULLISH but spot ≤ VWAP. No trade.", YELLOW))
            return None
        if direction == "BEARISH" and spot >= vwap:
            self._log(color("BEARISH but spot ≥ VWAP. No trade.", YELLOW))
            return None

        # Contract selection
        strike, otype  = self._select_strike(spot, direction)
        expiry         = self._nearest_expiry()
        contract       = f"NIFTY_{strike}_{otype}_{expiry}"
        self._log(color(f"▶ Entering: {contract}", BOLD))

        # Execute trade
        result = self._execute_trade(contract, direction)
        if result:
            self.trades.append(result)
            print(result.summary())
        return result

    def _nearest_expiry(self) -> str:
        """Return nearest Thursday expiry label (simplified)."""
        now = self.clock.now()
        days_ahead = (3 - now.weekday()) % 7  # Thursday = 3
        expiry = now + timedelta(days=days_ahead)
        return expiry.strftime("%d%b%y").upper()

    # ── Trade executor ────────────────────────────────────────────────────────
    def _execute_trade(self, contract: str, direction: str) -> Optional[TradeResult]:
        entry_price   = self.market.get_ltp(contract)
        entry_time    = self.clock.now()
        start_wall    = time.time()
        current_sl    = self.stop_loss
        tsl_activated = False
        poll          = SIM_POLL_INTERVAL if self.clock.is_simulation else POLL_INTERVAL

        self._log(
            f"  Entry Price : ₹{entry_price:.2f}\n"
            f"  Lot Size    : {self.lot_size}\n"
            f"  Stop-Loss   : ₹{current_sl} | Target: ₹{self.target} | TSL Trigger: ₹{self.tsl_trigger}"
        )

        exit_price  = entry_price
        exit_reason = "TIME_LIMIT"

        while (time.time() - start_wall) < MAX_TRADE_SECONDS:
            # Advance mock prices
            if hasattr(self.market, "tick"):
                self.market.tick()

            ltp         = self.market.get_ltp(contract)
            current_pnl = (ltp - entry_price) * self.lot_size
            elapsed     = time.time() - start_wall

            self._log(
                f"  LTP={ltp:.2f}  P&L={color(f'₹{current_pnl:+,.0f}', GREEN if current_pnl>=0 else RED)}"
                f"  SL={current_sl}  TSL={'ON' if tsl_activated else 'off'}"
                f"  T+{elapsed:.0f}s"
            )

            # TSL: move SL to breakeven
            if not tsl_activated and current_pnl >= self.tsl_trigger:
                current_sl    = 0
                tsl_activated = True
                self._log(color("  ⚡ TSL Activated — SL moved to Breakeven", CYAN))

            # Exit conditions
            if current_pnl <= -current_sl and current_sl > 0:
                exit_price  = ltp
                exit_reason = "STOP_LOSS"
                break
            if tsl_activated and current_pnl < 0:
                exit_price  = ltp
                exit_reason = "TSL_BREAKEVEN"
                break
            if current_pnl >= self.target:
                exit_price  = ltp
                exit_reason = "TARGET"
                break

            # Check end of trading window
            if self._current_minutes() > TRADE_WINDOW[1]:
                exit_price  = ltp
                exit_reason = "EOD_SQUAREOFF"
                break

            time.sleep(poll)

        exit_time = self.clock.now()
        final_pnl = (exit_price - entry_price) * self.lot_size

        return TradeResult(
            contract      = contract,
            direction     = direction,
            entry_price   = entry_price,
            exit_price    = exit_price,
            pnl           = final_pnl,
            exit_reason   = exit_reason,
            duration_sec  = time.time() - start_wall,
            tsl_activated = tsl_activated,
            entry_time    = entry_time,
            exit_time     = exit_time,
        )

    # ── Session summary ───────────────────────────────────────────────────────
    def session_summary(self):
        if not self.trades:
            print(color("\nNo trades taken this session.", YELLOW))
            return
        total_pnl = sum(t.pnl for t in self.trades)
        wins = [t for t in self.trades if t.pnl > 0]
        print(f"\n{BOLD}{'─'*60}")
        print(f"  SESSION SUMMARY  ({len(self.trades)} trade(s))")
        print(f"{'─'*60}{RESET}")
        for i, t in enumerate(self.trades, 1):
            sign = GREEN if t.pnl >= 0 else RED
            print(f"  #{i}  {t.contract:<30}  {color(f'₹{t.pnl:+,.0f}', sign)}  [{t.exit_reason}]")
        print(f"{'─'*60}")
        sign = GREEN if total_pnl >= 0 else RED
        print(f"  Total P&L   : {color(f'₹{total_pnl:+,.0f}', sign)}")
        print(f"  Win Rate    : {len(wins)}/{len(self.trades)} ({100*len(wins)//len(self.trades)}%)")
        print(f"{'─'*60}{RESET}\n")


# ──────────────────────────────────────────────────────────────────────────────
# CLI Entry Point
# ──────────────────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(
        description="Delta Drive Strategy — Live & Simulation Mode",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument("--simulate", action="store_true",
                   help="Run in simulation/paper-trade mode")
    p.add_argument("--sim-time", default=None, metavar="DATETIME",
                   help='Override current time, e.g. "2024-01-16 11:30"')
    p.add_argument("--sim-speed", type=float, default=60.0,
                   help="Simulation clock speed multiplier (default=60 → 1s=1min)")
    p.add_argument("--spot", type=float, default=25334.0,
                   help="Mock Nifty spot price (simulation only)")
    p.add_argument("--vix", type=float, default=14.5,
                   help="Mock VIX value (simulation only)")
    p.add_argument("--direction", choices=["BULLISH", "BEARISH", "AUTO"], default="AUTO",
                   help="Force sentiment direction (simulation only)")
    p.add_argument("--stop-loss", type=int, default=1000, help="Stop-loss in ₹ P&L")
    p.add_argument("--target", type=int, default=1500, help="Target in ₹ P&L")
    p.add_argument("--tsl-trigger", type=int, default=700,
                   help="P&L level to activate trailing SL")
    p.add_argument("--lot-size", type=int, default=LOT_SIZE)
    p.add_argument("--quiet", action="store_true", help="Suppress tick-by-tick logs")
    return p.parse_args()


def main():
    args = parse_args()

    print(f"\n{BOLD}{CYAN}{'═'*60}")
    print("  DELTA DRIVE STRATEGY — Nifty Options Scalper")
    mode = "SIMULATION" if args.simulate else "LIVE"
    print(f"  Mode: {mode}")
    print(f"{'═'*60}{RESET}\n")

    # ── Clock ──────────────────────────────────────────────────────────────
    sim_time = None
    if args.simulate:
        if args.sim_time:
            try:
                sim_time = datetime.strptime(args.sim_time, "%Y-%m-%d %H:%M")
            except ValueError:
                print(color("Invalid --sim-time format. Use: YYYY-MM-DD HH:MM", RED))
                sys.exit(1)
        else:
            # Default sim time: today 11:00 AM
            sim_time = datetime.now().replace(hour=11, minute=0, second=0, microsecond=0)
        print(f"  Simulation Start : {sim_time.strftime('%Y-%m-%d %H:%M')}")
        print(f"  Clock Speed      : {args.sim_speed}x real time")

    clock = Clock(sim_time=sim_time, speed=args.sim_speed)

    # ── Market Data ────────────────────────────────────────────────────────
    if args.simulate:
        market = MockMarketData(
            spot      = args.spot,
            vix       = args.vix,
            direction = args.direction,
        )
        print(f"  Mock Spot        : ₹{args.spot:,.2f}")
        print(f"  Mock VIX         : {args.vix}")
        print(f"  Forced Direction : {args.direction}\n")
    else:
        market = LiveMarketData()
        print(color(
            "  LIVE MODE: Replace LiveMarketData methods with your broker API.\n"
            "  See class LiveMarketData in this file.", YELLOW
        ))

    # ── Strategy ───────────────────────────────────────────────────────────
    strategy = DeltaDriveStrategy(
        clock       = clock,
        market      = market,
        stop_loss   = args.stop_loss,
        target      = args.target,
        tsl_trigger = args.tsl_trigger,
        lot_size    = args.lot_size,
        verbose     = not args.quiet,
    )

    try:
        strategy.run()
    except KeyboardInterrupt:
        print(color("\n\nInterrupted by user.", YELLOW))

    strategy.session_summary()


if __name__ == "__main__":
    main()