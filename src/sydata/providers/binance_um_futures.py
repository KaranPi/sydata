from __future__ import annotations  # no installation needed

from dataclasses import dataclass  # no installation needed
from typing import Any, Dict, List, Optional  # no installation needed
import time  # no installation needed

import requests  # already in env — no new install


@dataclass
class BinanceUmFuturesClient:
    """
    Minimal USDⓈ-M Futures REST client for spot-checking premiumIndexKlines.
    Docs: GET /fapi/v1/premiumIndexKlines :contentReference[oaicite:3]{index=3}
    """
    base_url: str = "https://fapi.binance.com"
    timeout_s: int = 30
    retries: int = 4
    backoff_s: float = 0.7

    def _get(self, path: str, params: Dict[str, Any]) -> Any:
        last_err: Optional[Exception] = None
        url = f"{self.base_url}{path}"
        for i in range(self.retries):
            try:
                r = requests.get(url, params=params, timeout=self.timeout_s)
                r.raise_for_status()
                return r.json()
            except Exception as e:
                last_err = e
                time.sleep(self.backoff_s * (2**i))
        raise RuntimeError(f"GET {path} failed after retries; params={params}") from last_err

    def fetch_premium_index_klines(
        self,
        symbol: str,
        interval: str,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        limit: int = 1500,
    ) -> List[List[Any]]:
        params: Dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": int(limit)}
        if start_ms is not None:
            params["startTime"] = int(start_ms)
        if end_ms is not None:
            params["endTime"] = int(end_ms)
        return self._get("/fapi/v1/premiumIndexKlines", params=params)
