"""
src/logger.py
=============
Central logging setup for the swing pipeline.

Two handlers per run:
  - File handler  : logs/pipeline_YYYY-MM-DD_HHMMSS.log  — DEBUG+, full detail
  - Stream handler: stderr — WARNING+ only (errors surface in CI output)

Usage
-----
    from src.logger import get_logger
    log = get_logger(__name__)

    log.info("candles ingested: %d", n)
    log.warning("holiday API unreachable, using cache")
    log.error("fetch failed: %s", exc)

The pipeline's main() calls setup_logging(run_date) once at startup.
All subsequent get_logger() calls reuse the already-configured root logger.
"""

from __future__ import annotations

import logging
import sys
from datetime import date, datetime
from pathlib import Path

_LOG_DIR  = Path(__file__).parent.parent / "logs"
_FMT_FILE = "%(asctime)s  %(levelname)-8s  %(name)-28s  %(message)s"
_FMT_CON  = "%(levelname)-8s  %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"

_configured = False   # guard so setup_logging is idempotent


def setup_logging(run_date: date | None = None) -> Path:
    """
    Configure root logger with a file handler (DEBUG+) and a stderr handler
    (WARNING+).  Returns the path of the log file created.

    Safe to call multiple times — only the first call has any effect.
    """
    global _configured
    if _configured:
        return _current_log_file()

    _LOG_DIR.mkdir(parents=True, exist_ok=True)

    ts       = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    date_tag = run_date.isoformat() if run_date else datetime.now().strftime("%Y-%m-%d")
    log_path = _LOG_DIR / f"pipeline_{date_tag}_{ts}.log"

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # ── File handler — full detail ──────────────────────────────────────────
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(_FMT_FILE, datefmt=_DATE_FMT))
    root.addHandler(fh)

    # ── stderr handler — warnings and errors only ───────────────────────────
    sh = logging.StreamHandler(sys.stderr)
    sh.setLevel(logging.WARNING)
    sh.setFormatter(logging.Formatter(_FMT_CON))
    root.addHandler(sh)

    # Silence noisy third-party loggers
    for noisy in ("yfinance", "peewee", "urllib3", "requests", "charset_normalizer"):
        logging.getLogger(noisy).setLevel(logging.ERROR)

    _configured = True
    return log_path


def _current_log_file() -> Path:
    """Return the path of the first FileHandler on the root logger, if any."""
    for h in logging.getLogger().handlers:
        if isinstance(h, logging.FileHandler):
            return Path(h.baseFilename)
    return _LOG_DIR / "pipeline.log"


def get_logger(name: str) -> logging.Logger:
    """Return a named child logger (call setup_logging first)."""
    return logging.getLogger(name)
