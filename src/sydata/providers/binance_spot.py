# Key endpoint semantics used here:
# - GET /api/v3/klines with limit max 1000
# - open time is the unique identifier; startTime/endTime in UTC.
#------------------------------------------------------------------

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

import time
import requests
import pandas as pd


@dataclass(frozen=True)
class BinanceSpotClient:
    """
    Minimal public-market-data client for Spot klines.
    Uses Binance's market-data base endpoint.
    """
    base_url: str = "https://data-api.binance.vision"
    timeout_s: int = 10
    max_retries: int = 6
    backoff_s: float = 0.5  # exponential

    def _get(self, path: str, params: dict) -> list:
        url = f"{self.base_url}{path}"
        last_err: Optional[Exception] = None

        for i in range(self.max_retries):
            try:
                r = requests.get(url, params=params, timeout=self.timeout_s)
                if r.status_code == 200:
                    return r.json()
                # 418/429 are typical rate-limit / ban responses; 5xx transient.
                if r.status_code in (418, 429) or 500 <= r.status_code < 600:
                    time.sleep(self.backoff_s * (2 ** i))
                    continue
                r.raise_for_status()
            except Exception as e:
                last_err = e
                time.sleep(self.backoff_s * (2 ** i))

        raise RuntimeError(f"GET {path} failed after retries; params={params}") from last_err

    @staticmethod
    def to_api_symbol(canonical_symbol: str) -> str:
        import re  # no installation needed
        s = canonical_symbol.strip()
        if re.fullmatch(r"[A-Za-z0-9]+-[A-Za-z0-9]+", s):
            return s.replace("-", "").upper()
        if re.fullmatch(r"[A-Za-z0-9]{6,20}", s):
            return s.upper()
        raise ValueError(f"Bad symbol '{canonical_symbol}'. Expected 'BTC-USDT' or 'BTCUSDT'.")

    def fetch_klines_chunk(
        self,
        symbol: str,
        interval: str,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        limit: int = 1000,
    ) -> list:
        params: dict = {"symbol": self.to_api_symbol(symbol), "interval": interval, "limit": int(limit)}
        if start_ms is not None:
            params["startTime"] = int(start_ms)
        if end_ms is not None:
            params["endTime"] = int(end_ms)
        return self._get("/api/v3/klines", params=params)

    def fetch_klines(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int,
        limit: int = 1000,
    ) -> pd.DataFrame:
        """
        Deterministic pagination using kline open time as cursor.
        Klines are uniquely identified by open time. :contentReference[oaicite:3]{index=3}
        """
        rows: list[list] = []
        cursor = int(start_ms)

        while cursor <= end_ms:
            chunk = self.fetch_klines_chunk(
                symbol=symbol,
                interval=interval,
                start_ms=cursor,
                end_ms=end_ms,
                limit=limit,
            )
            if not chunk:
                break

            rows.extend(chunk)

            last_open = int(chunk[-1][0])
            # advance by 1ms to avoid repeating the last bar (open time is unique)
            next_cursor = last_open + 1
            if next_cursor <= cursor:
                break
            cursor = next_cursor

            # light pacing; avoids hammering (rate limits vary by endpoint weights)
            time.sleep(0.05)

        if not rows:
            return pd.DataFrame(
                columns=[
                    "open_time",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "close_time",
                    "quote_volume",
                    "trades",
                    "taker_buy_base_volume",
                    "taker_buy_quote_volume",
                    "ignore",
                ]
            )

        df = pd.DataFrame(
            rows,
            columns=[
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_volume",
                "trades",
                "taker_buy_base_volume",
                "taker_buy_quote_volume",
                "ignore",
            ],
        )

        # strict dtypes (keep raw but typed)
        df["open_time"] = df["open_time"].astype("int64")
        df["close_time"] = df["close_time"].astype("int64")
        for c in ["open", "high", "low", "close", "volume", "quote_volume", "taker_buy_base_volume", "taker_buy_quote_volume"]:
            df[c] = df[c].astype("float64")
        df["trades"] = df["trades"].astype("int64")

        # attach keys
        df["symbol"] = symbol
        df["interval"] = interval
        df["venue"] = "binance"

        # de-dup on open time (primary identifier)
        df = df.drop_duplicates(subset=["symbol", "interval", "open_time"]).sort_values("open_time").reset_index(drop=True)
        return df
