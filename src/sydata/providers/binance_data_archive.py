from __future__ import annotations  # no installation needed

from dataclasses import dataclass  # no installation needed
from pathlib import Path  # no installation needed
from typing import Optional  # no installation needed
import shutil  # no installation needed

import requests  # already in env — no new install


@dataclass(frozen=True)
class BinanceDataArchiveClient:
    """
    Thin client for Binance Data Collection (data.binance.vision) public files.

    URL patterns confirmed by directory listings:
      - monthly aggTrades: /data/spot/monthly/aggTrades/<SYM>/<SYM>-aggTrades-YYYY-MM.zip
      - daily   aggTrades: /data/spot/daily/aggTrades/<SYM>/<SYM>-aggTrades-YYYY-MM-DD.zip
    """  # no installation needed

    base_url: str = "https://data.binance.vision"
    timeout_s: int = 120
    chunk_bytes: int = 1024 * 1024

    def spot_aggtrades_monthly_url(self, venue_symbol: str, year: int, month: int) -> str:
        yymm = f"{year:04d}-{month:02d}"
        return (
            f"{self.base_url}/data/spot/monthly/aggTrades/{venue_symbol}/"
            f"{venue_symbol}-aggTrades-{yymm}.zip"
        )

    def spot_aggtrades_daily_url(self, venue_symbol: str, year: int, month: int, day: int) -> str:
        yymmdd = f"{year:04d}-{month:02d}-{day:02d}"
        return (
            f"{self.base_url}/data/spot/daily/aggTrades/{venue_symbol}/"
            f"{venue_symbol}-aggTrades-{yymmdd}.zip"
        )

    def download_zip(self, url: str, dest_zip: Path) -> Path:
        dest_zip.parent.mkdir(parents=True, exist_ok=True)

        tmp = dest_zip.with_suffix(dest_zip.suffix + ".tmp")
        if tmp.exists():
            tmp.unlink()

        with requests.get(url, stream=True, timeout=self.timeout_s) as r:
            if r.status_code == 404:
                raise FileNotFoundError(url)
            r.raise_for_status()
            with tmp.open("wb") as f:
                for chunk in r.iter_content(chunk_size=self.chunk_bytes):
                    if chunk:
                        f.write(chunk)

        tmp.replace(dest_zip)
        return dest_zip

    def safe_rm_tree(self, p: Path) -> None:
        if p.exists():
            shutil.rmtree(p, ignore_errors=True)
