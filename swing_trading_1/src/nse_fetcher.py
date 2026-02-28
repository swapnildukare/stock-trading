import io
import json
import requests
import pandas as pd
from datetime import date, datetime
from pathlib import Path

from src.logger import get_logger

log = get_logger(__name__)

# Map friendly index names → NSE CSV URL suffix
NSE_INDEX_MAP = {
    "NIFTY_50":   "ind_nifty50list.csv",
    "NIFTY_100":  "ind_nifty100list.csv",
    "NIFTY_200":  "ind_nifty200list.csv",
    "NIFTY_500":  "ind_nifty500list.csv",
}

_BASE_URL          = "https://niftyindices.com/IndexConstituent/"
_HOLIDAY_API_URL   = "https://www.nseindia.com/api/holiday-master?type=trading"
_HOLIDAY_CACHE     = Path(__file__).parent.parent / "data" / "nse_holidays_cache.json"

# NSE blocks requests without a browser User-Agent
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://niftyindices.com/",
}

_HOLIDAY_HEADERS = {
    **_HEADERS,
    "Accept":  "application/json, text/plain, */*",
    "Referer": "https://www.nseindia.com/",
}


# ──────────────────────────────────────────────────────────────────────────────
# Ticker resolution
# ──────────────────────────────────────────────────────────────────────────────

def resolve_tickers(index_name: str, verbose: bool = False) -> list[str]:
    """Fetch tickers from NSE for a given index name (e.g. 'NIFTY_200').
    The `verbose` parameter is kept for backwards compatibility but has no effect —
    logging is now handled via the standard logger.
    """
    suffix = NSE_INDEX_MAP.get(index_name.upper())
    if not suffix:
        raise ValueError(f"Unknown index '{index_name}'. Choose from: {list(NSE_INDEX_MAP)}")

    url = _BASE_URL + suffix
    log.info("fetching %s tickers from NSE", index_name)
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=15)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))
        tickers = [s.strip() + ".NS" for s in df["Symbol"].tolist()]
        log.info("fetched %d tickers from %s", len(tickers), index_name)
        return tickers
    except Exception as e:
        log.error("failed to fetch %s from NSE: %s", index_name, e)
        raise


# ──────────────────────────────────────────────────────────────────────────────
# Market calendar
# ──────────────────────────────────────────────────────────────────────────────

def _nse_session() -> requests.Session:
    """Create a session with NSE cookies (required by the holiday API)."""
    session = requests.Session()
    session.headers.update(_HOLIDAY_HEADERS)
    try:
        session.get("https://www.nseindia.com", timeout=10)
    except Exception:
        pass
    return session


def get_nse_holidays(year: int | None = None) -> set[date]:
    """
    Return a set of NSE exchange holidays (Capital Market segment) for the given year.

    Fetches from NSE's official API and caches to data/nse_holidays_cache.json.
    Falls back to disk cache if the API is unreachable.
    """
    year = year or date.today().year

    # Load disk cache
    cache: dict = {}
    if _HOLIDAY_CACHE.exists():
        try:
            cache = json.loads(_HOLIDAY_CACHE.read_text())
        except Exception:
            pass

    if str(year) in cache:
        return {date.fromisoformat(d) for d in cache[str(year)]}

    holidays: set[date] = set()
    try:
        session  = _nse_session()
        resp     = session.get(_HOLIDAY_API_URL, timeout=15)
        resp.raise_for_status()
        data     = resp.json()

        # "CM" = Capital Market (equities)
        for entry in data.get("CM", []):
            raw = entry.get("tradingDate", "")
            try:
                d = datetime.strptime(raw, "%d-%b-%Y").date()
                cache.setdefault(str(d.year), [])
                cache[str(d.year)].append(d.isoformat())
                if d.year == year:
                    holidays.add(d)
            except ValueError:
                continue

        # Persist updated cache
        _HOLIDAY_CACHE.parent.mkdir(parents=True, exist_ok=True)
        # Deduplicate and sort each year's entries
        for y in cache:
            cache[y] = sorted(set(cache[y]))
        _HOLIDAY_CACHE.write_text(json.dumps(cache, indent=2))

    except Exception as exc:
        log.warning("could not fetch NSE holidays: %s — using cache/empty", exc)

    return holidays


def is_trading_day(check_date: date | None = None) -> tuple[bool, str]:
    """
    Return (is_open, reason) for the given date (default: today).

    is_open=True  → normal trading day
    is_open=False → weekend or NSE holiday
    """
    check_date = check_date or date.today()
    weekday    = check_date.weekday()   # 0=Mon … 6=Sun

    if weekday == 5:
        return False, f"{check_date} is Saturday — NSE closed"
    if weekday == 6:
        return False, f"{check_date} is Sunday — NSE closed"

    if check_date in get_nse_holidays(check_date.year):
        return False, f"{check_date} is an NSE trading holiday"

    return True, f"{check_date} is a trading day"


def filter_trading_days(dates: list[date]) -> tuple[list[date], list[tuple[date, str]]]:
    """
    Split a list of dates into (trading_days, skipped).
    skipped is a list of (date, reason) tuples.
    """
    trading, skipped = [], []
    for d in dates:
        open_, reason = is_trading_day(d)
        if open_:
            trading.append(d)
        else:
            skipped.append((d, reason))
    return trading, skipped
