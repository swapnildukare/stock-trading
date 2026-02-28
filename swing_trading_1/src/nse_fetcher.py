import io
import requests
import pandas as pd

# Map friendly index names → NSE CSV URL suffix
NSE_INDEX_MAP = {
    "NIFTY_50":   "ind_nifty50list.csv",
    "NIFTY_100":  "ind_nifty100list.csv",
    "NIFTY_200":  "ind_nifty200list.csv",
    "NIFTY_500":  "ind_nifty500list.csv",
}

_BASE_URL = "https://niftyindices.com/IndexConstituent/"

# NSE blocks requests without a browser User-Agent
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://niftyindices.com/",
}


def resolve_tickers(index_name: str, verbose: bool = False) -> list[str]:
    """Fetch tickers from NSE for a given index name (e.g. 'NIFTY_200')."""
    suffix = NSE_INDEX_MAP.get(index_name.upper())
    if not suffix:
        raise ValueError(f"Unknown index '{index_name}'. Choose from: {list(NSE_INDEX_MAP)}")

    url = _BASE_URL + suffix
    if verbose:
        print(f"Fetching {index_name} tickers from NSE...")
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=15)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))
        tickers = [s.strip() + ".NS" for s in df["Symbol"].tolist()]
        if verbose:
            print(f"Fetched {len(tickers)} tickers from {index_name}.")
        return tickers
    except Exception as e:
        if verbose:
            print(f"Failed to fetch {index_name} from NSE: {e}")
        raise
