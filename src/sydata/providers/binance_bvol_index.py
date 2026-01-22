from __future__ import annotations  # no installation needed

import io  # no installation needed
import zipfile  # no installation needed
from dataclasses import dataclass  # no installation needed
from datetime import date  # no installation needed

import pandas as pd  # already in env — no new install
import requests  # already in env — no new install


@dataclass(frozen=True)
class BinanceBVOLIndexClient:
    base_url: str = "https://data.binance.vision"
    timeout_s: int = 60

    def url_for_day(self, symbol: str, d: date) -> str:
        # Confirmed by Binance Data Collection listing: .../option/daily/BVOLIndex/<SYMBOL>/<SYMBOL>-BVOLIndex-YYYY-MM-DD.zip :contentReference[oaicite:2]{index=2}
        ds = d.strftime("%Y-%m-%d")
        return f"{self.base_url}/data/option/daily/BVOLIndex/{symbol}/{symbol}-BVOLIndex-{ds}.zip"

    def fetch_day(self, symbol: str, d: date) -> pd.DataFrame:
        url = self.url_for_day(symbol, d)
        r = requests.get(url, timeout=self.timeout_s)
        if r.status_code == 404:
            return pd.DataFrame()
        r.raise_for_status()

        z = zipfile.ZipFile(io.BytesIO(r.content))
        csv_name = [n for n in z.namelist() if n.lower().endswith(".csv")][0]
        df = pd.read_csv(z.open(csv_name))

        # keep raw, but attach minimal lineage keys
        df["symbol"] = symbol
        df["file_date"] = d.isoformat()
        df["venue"] = "binance"
        df["dataset"] = "bvol_index"
        return df
