"""
Condition Strategy Pattern
==========================

Each Condition is an independent, pluggable rule applied to a candidate
during the Day 1-4 stability window.

Interface
---------
    ok, note = condition.evaluate(ctx, candle)

    ok=True,  note=""         → condition passed, nothing to report
    ok=True,  note="WARN: …"  → passed, but worth surfacing (soft flag)
    ok=False, note="reason"   → failed; candidate moves to FALLOUT state

FunnelContext
-------------
A lightweight, self-contained context object passed into every condition.
It carries only what conditions need — the Day 0 anchor prices and the
current stable_days counter. No dependency on src.models so this file
can be read and unit-tested in complete isolation.

Built-in conditions
-------------------
    StabilityCondition  — core rule: candle must stay within ±% of Day 0 high.
    VolumeCondition     — soft flag: elevated volume vs Day 0 (informational).

Extending the system
--------------------
Add a new condition by:
  1. Subclass Condition, implement name + evaluate()
  2. Append an instance to _CONDITIONS in pipeline.py

No other changes needed. See FUTURE_SCOPE.md for planned additions:
  RSICondition, ATRCondition, VolumeCondition(hard=True).

Design note — why conditions are stateless
------------------------------------------
Each condition receives a FunnelContext snapshot and a single DayCandle.
Conditions never write to the DB, never mutate the context, and never
depend on external state. This means:
  • Any condition can be unit-tested with just two plain objects.
  • Adding / removing a condition never changes pipeline orchestration.
  • The same condition list is reused across every ticker every day.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


# ---------------------------------------------------------------------------
# Shared data objects — no imports from src.models
# ---------------------------------------------------------------------------

@dataclass
class FunnelContext:
    """
    Minimal context passed to each Condition during day-by-day evaluation.

    Fields
    ------
    day0_high    : Day 0 high price — the stability ceiling/floor anchor.
    day0_volume  : Day 0 volume — baseline for relative-volume checks.
    stable_days  : How many days have already passed the stability check
                   before this candle is evaluated. Used in failure messages
                   so the log records "failed on Day 2" rather than a raw index.
    """
    day0_high:   float
    day0_volume: float
    stable_days: int = 0


@dataclass
class DayCandle:
    """Snapshot of a single ticker's OHLCV for one trading day."""
    ticker:     str
    high:       float
    low:        float
    close:      float
    volume:     float
    change_pct: float


# ---------------------------------------------------------------------------
# Base interface
# ---------------------------------------------------------------------------

class Condition(ABC):
    """
    Base interface for all funnel entry/exit conditions.

    Subclass this to add any new rule. The pipeline calls evaluate() on every
    condition in _CONDITIONS for each (candidate, day) pair. First failure
    stops evaluation and marks the candidate FALLOUT.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Short identifier used in failure_reason log messages."""
        ...

    @abstractmethod
    def evaluate(self, ctx: FunnelContext, candle: DayCandle) -> tuple[bool, str]:
        """
        Assess whether this candidate still belongs in the funnel.

        Args:
            ctx    : FunnelContext with Day 0 anchors + stable_days so far.
            candle : Today's OHLCV snapshot for this ticker.

        Returns:
            (True,  "")           — condition passed, no note
            (True,  "WARN: …")    — passed with soft informational flag
            (False, "reason …")   — failed; candidate should become FALLOUT
        """
        ...


# ---------------------------------------------------------------------------
# Core condition — required for the funnel
# ---------------------------------------------------------------------------

class StabilityCondition(Condition):
    """
    Days 1-4: the candle must trade within a tight range around Day 0 high.

    Ceiling : day0_high × (1 + max_up_pct   / 100)   default +1%
    Floor   : day0_high × (1 - max_down_pct / 100)   default -2%

    Any day where candle.high > ceiling or candle.low < floor → FALLOUT.

    Using Day 0 HIGH (not close) as the anchor is intentional:
    the high represents the full extent of the impulse move. A stock
    consolidating below its impulse high is a healthy base; a stock that
    exceeds it is making a new leg up (not consolidation).

    Configuration: STABLE_MAX_UP_PCT and STABLE_MAX_DOWN_PCT in config.py.
    """

    name = "StabilityCondition"

    def __init__(self, max_up_pct: float = 1.0, max_down_pct: float = 2.0) -> None:
        self.max_up_pct   = max_up_pct
        self.max_down_pct = max_down_pct

    def evaluate(self, ctx: FunnelContext, candle: DayCandle) -> tuple[bool, str]:
        anchor  = ctx.day0_high
        floor   = anchor * (1 - self.max_down_pct / 100)
        ceiling = anchor * (1 + self.max_up_pct   / 100)

        if candle.low < floor:
            return (
                False,
                f"Low {candle.low:.2f} broke floor {floor:.2f} "
                f"(-{self.max_down_pct}% of Day0 high {anchor:.2f}) "
                f"on Day {ctx.stable_days + 1}",
            )
        if candle.high > ceiling:
            return (
                False,
                f"High {candle.high:.2f} broke ceiling {ceiling:.2f} "
                f"(+{self.max_up_pct}% of Day0 high {anchor:.2f}) "
                f"on Day {ctx.stable_days + 1}",
            )
        return True, ""


# ---------------------------------------------------------------------------
# Soft / informational condition
# ---------------------------------------------------------------------------

class VolumeCondition(Condition):
    """
    Flags if today's volume exceeds Day 0 volume.

    Healthy consolidation should see declining volume — sellers exhausted.
    Elevated volume during consolidation suggests continued seller pressure
    or the start of a new directional move.

    Currently soft (hard=False): always passes but attaches a warning note
    that surfaces in pipeline output. No candidate is ejected.

    To enforce as a hard gate:
        VolumeCondition(hard=True)
    in _CONDITIONS in pipeline.py — no other changes needed.

    See FUTURE_SCOPE.md for the full plan.
    """

    name = "VolumeCondition"

    def __init__(self, hard: bool = False) -> None:
        """
        Args:
            hard: If True, elevated volume causes FALLOUT. Default False (soft).
        """
        self.hard = hard

    def evaluate(self, ctx: FunnelContext, candle: DayCandle) -> tuple[bool, str]:
        if ctx.day0_volume > 0 and candle.volume > ctx.day0_volume:
            ratio = candle.volume / ctx.day0_volume
            note  = f"Volume {ratio:.1f}x Day0 (elevated — watch for continuation)"
            if self.hard:
                return False, note
            return True, f"WARN: {note}"
        return True, ""
