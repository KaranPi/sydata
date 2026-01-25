from __future__ import annotations  # no installation needed

import time  # no installation needed
from dataclasses import dataclass  # no installation needed
from typing import Any, Dict, Optional, Tuple, List  # no installation needed

import pandas as pd  # already in env — no new install
import requests  # already in env — no new install


@dataclass
class BinanceOptionsClient:
    base_url: str = "https://eapi.binance.com"
    timeout_s: int = 30
    max_retries: int = 5
    backoff_s: float = 0.5

    def __post_init__(self) -> None:
        self._session = requests.Session()

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = self.base_url.rstrip("/") + path
        last_err: Optional[Exception] = None

        for i in range(self.max_retries):
            try:
                r = self._session.get(url, params=params or {}, timeout=self.timeout_s)
                if r.status_code in (429, 418):
                    # rate limit / ban protection: exponential backoff
                    time.sleep(self.backoff_s * (2 ** i))
                    continue
                r.raise_for_status()
                return r.json()
            except Exception as e:
                last_err = e
                time.sleep(self.backoff_s * (2 ** i))

        raise RuntimeError(f"GET {path} failed after retries; params={params}") from last_err

    def fetch_exchange_info(self) -> Dict[str, Any]:
        # https://developers.binance.com/docs/derivatives/options-trading/market-data/Exchange-Information
        return self._get("/eapi/v1/exchangeInfo")

    def fetch_mark(self, symbol: Optional[str] = None) -> Any:
        # https://developers.binance.com/docs/derivatives/options-trading/market-data/Option-Mark-Price
        params: Dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        return self._get("/eapi/v1/mark", params=params)
  

def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


def exchange_info_to_frames(
    payload: Dict[str, Any],
    asof_ms: int,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      - option_contracts_df
      - option_assets_df
      - option_symbols_df (flattened filters)
    """
    option_contracts = payload.get("optionContracts", []) or []
    option_assets = payload.get("optionAssets", []) or []
    option_symbols = payload.get("optionSymbols", []) or []

    cdf = pd.DataFrame(option_contracts)
    adf = pd.DataFrame(option_assets)

    rows: List[Dict[str, Any]] = []
    for s in option_symbols:
        s = dict(s)

        # flatten filters[] into a few canonical columns
        pf = None
        ls = None
        for f in (s.get("filters", []) or []):
            if not isinstance(f, dict):
                continue
            if f.get("filterType") == "PRICE_FILTER":
                pf = f
            elif f.get("filterType") == "LOT_SIZE":
                ls = f

        out: Dict[str, Any] = {
            "asof_ms": asof_ms,
            "symbol": s.get("symbol"),
            "underlying": s.get("underlying"),
            "expiry_ms": _safe_int(s.get("expiryDate")),
            "side": s.get("side"),
            "strike_price": _safe_float(s.get("strikePrice")),
            "unit": _safe_int(s.get("unit")),
            "liquidation_fee_rate": _safe_float(s.get("liquidationFeeRate")),
            "min_qty": _safe_float(s.get("minQty")),
            "max_qty": _safe_float(s.get("maxQty")),
            "initial_margin": _safe_float(s.get("initialMargin")),
            "maintenance_margin": _safe_float(s.get("maintenanceMargin")),
            "min_initial_margin": _safe_float(s.get("minInitialMargin")),
            "min_maintenance_margin": _safe_float(s.get("minMaintenanceMargin")),
            "price_scale": _safe_int(s.get("priceScale")),
            "quantity_scale": _safe_int(s.get("quantityScale")),
            "quote_asset": s.get("quoteAsset"),
            "status": s.get("status"),
            # PRICE_FILTER
            "min_price": _safe_float(pf.get("minPrice")) if pf else None,
            "max_price": _safe_float(pf.get("maxPrice")) if pf else None,
            "tick_size": _safe_float(pf.get("tickSize")) if pf else None,
            # LOT_SIZE
            "min_lot_qty": _safe_float(ls.get("minQty")) if ls else None,
            "max_lot_qty": _safe_float(ls.get("maxQty")) if ls else None,
            "step_size": _safe_float(ls.get("stepSize")) if ls else None,
        }
        rows.append(out)

    sdf = pd.DataFrame(rows)

    # types + derived timestamps
    if not sdf.empty:
        sdf["asof_ts"] = pd.to_datetime(sdf["asof_ms"], unit="ms", utc=True)
        sdf["expiry_ts"] = pd.to_datetime(sdf["expiry_ms"], unit="ms", utc=True, errors="coerce")

    return cdf, adf, sdf

def mark_to_frame(mark_payload: Any, asof_ms: int) -> pd.DataFrame:
    """
    Normalize /eapi/v1/mark payload to a typed dataframe with stable time keys.
    The mark payload does not carry timestamps; we stamp with fetch time.
    """
    df = pd.DataFrame(mark_payload if isinstance(mark_payload, list) else [])

    ts_fetch = pd.to_datetime(asof_ms, unit="ms", utc=True)
    ts_hour = ts_fetch.floor("h")

    df["asof_ms"] = asof_ms
    df["ts_fetch"] = ts_fetch
    df["ts_hour"] = ts_hour

    # numeric columns commonly present in mark payload
    num_cols = [
        "markPrice", "bidIV", "askIV", "markIV",
        "delta", "gamma", "vega", "theta",
        "highPriceLimit", "lowPriceLimit", "riskFreeInterest",
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df
