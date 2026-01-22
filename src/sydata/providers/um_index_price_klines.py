from __future__ import annotations  # no installation needed

import io  # no installation needed
import zipfile  # no installation needed
from dataclasses import dataclass  # no installation needed
from datetime import date  # no installation needed
from pathlib import Path  # no installation needed

import pandas as pd  # already in env — no new install

from sydata.providers.binance_data_archive import BinanceDataArchiveClient  # project-local
from sydata.providers.um_mark_price_klines import canon_to_venue_symbol, iter_months  # project-local


@dataclass
class UmIndexPriceKlinesArchive:
    data_root: Path
    client: BinanceDataArchiveClient
    venue: str = "binance"

    def _month_url(self, venue_symbol: str, interval: str, year: int, month: int) -> str:
        # https://data.binance.vision/data/futures/um/monthly/indexPriceKlines/BTCUSDT/1h/BTCUSDT-1h-2024-01.zip
        return (
            f"{self.client.base_url}/data/futures/um/monthly/indexPriceKlines/"
            f"{venue_symbol}/{interval}/{venue_symbol}-{interval}-{year:04d}-{month:02d}.zip"
        )

    def _out_path(self, symbol_canon: str, interval: str, year: int, month: int) -> Path:
        return (
            self.data_root
            / "raw"
            / "binance"
            / "um_index_price_klines"
            / f"symbol={symbol_canon}"
            / f"interval={interval}"
            / f"part-{year:04d}-{month:02d}.parquet"
        )

    def ingest_month(
        self,
        symbol_canon: str,
        interval: str,
        year: int,
        month: int,
        overwrite: bool = False,
    ) -> dict:
        venue_symbol = canon_to_venue_symbol(symbol_canon)
        outp = self._out_path(symbol_canon, interval, year, month)

        if outp.exists() and not overwrite:
            return {
                "symbol": symbol_canon,
                "venue_symbol": venue_symbol,
                "interval": interval,
                "year": year,
                "month": month,
                "status": "already_exists",
                "path": str(outp),
            }

        url = self._month_url(venue_symbol, interval, year, month)

        tmp_dir = self.data_root / "meta" / "tmp" / "um_index_price_klines"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        zip_path = tmp_dir / f"{venue_symbol}-{interval}-{year:04d}-{month:02d}.zip"

        try:
            self.client.download_zip(url=url, dest_zip=zip_path)
        except FileNotFoundError:
            return {
                "symbol": symbol_canon,
                "venue_symbol": venue_symbol,
                "interval": interval,
                "year": year,
                "month": month,
                "status": "missing_archive_file",
                "url": url,
            }

        with zipfile.ZipFile(zip_path, "r") as zf:
            names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not names:
                raise RuntimeError(f"No CSV found in {zip_path}")
            with zf.open(names[0]) as f:
                raw = f.read()

        cols = [
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "ignore_0",
            "close_time",
            "ignore_1",
            "ignore_2",
            "ignore_3",
            "ignore_4",
            "ignore_5",
        ]
        df = pd.read_csv(io.BytesIO(raw), header=None, names=cols)

        for c in ["open_time", "close_time"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna(subset=["open_time", "close_time"])

        df["open_time"] = df["open_time"].astype("int64")
        df["close_time"] = df["close_time"].astype("int64")

        for c in ["open", "high", "low", "close"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        df = (
            df.dropna(subset=["open", "high", "low", "close"])
            .drop_duplicates(subset=["open_time"])
            .sort_values("open_time")
        )

        df["symbol"] = symbol_canon
        df["venue_symbol"] = venue_symbol
        df["interval"] = interval
        df["venue"] = self.venue
        df["dataset"] = "um_index_price_klines"

        outp.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(outp, index=False)

        return {
            "symbol": symbol_canon,
            "venue_symbol": venue_symbol,
            "interval": interval,
            "year": year,
            "month": month,
            "status": "ok",
            "rows": int(len(df)),
            "path": str(outp),
            "url": url,
        }

    def ingest_range(
        self,
        symbols: list[str],
        intervals: list[str],
        start_d: date,
        end_d: date,
        overwrite: bool = False,
    ) -> dict:
        results: list[dict] = []
        months = iter_months(start_d, end_d)

        for s in symbols:
            for itv in intervals:
                for (y, m) in months:
                    results.append(self.ingest_month(s, itv, y, m, overwrite=overwrite))

        summary = {}
        for r in results:
            summary[r["status"]] = summary.get(r["status"], 0) + 1

        return {"summary": summary, "results": results}
