from __future__ import annotations  # no installation needed

from dataclasses import dataclass  # no installation needed
from datetime import datetime, timezone  # no installation needed
from pathlib import Path  # no installation needed
from typing import Dict, Iterable, Iterator, Optional, Tuple  # no installation needed
import io  # no installation needed
import zipfile  # no installation needed

import pandas as pd  # already in env — no new install

from sydata.providers.binance_data_archive import BinanceDataArchiveClient  # project-local


def canon_to_venue_symbol_um(canon: str) -> str:
    # "BTC-USDT" -> "BTCUSDT"
    return canon.replace("-", "").upper()


def _detect_epoch_unit(x: int) -> str:
    # crude but robust: ms vs us vs ns
    # ms ~ 1e12-1e13, us ~ 1e15-1e16, ns ~ 1e18-1e19
    if x > 10**17:
        return "ns"
    if x > 10**14:
        return "us"
    return "ms"


def _month_iter(start_utc: datetime, end_utc: datetime) -> Iterator[Tuple[int, int]]:
    # yields (year, month) covering [start, end)
    cur = datetime(start_utc.year, start_utc.month, 1, tzinfo=timezone.utc)
    endm = datetime(end_utc.year, end_utc.month, 1, tzinfo=timezone.utc)
    while cur <= endm:
        yield cur.year, cur.month
        # add one month
        if cur.month == 12:
            cur = datetime(cur.year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            cur = datetime(cur.year, cur.month + 1, 1, tzinfo=timezone.utc)


@dataclass
class UmPremiumIndexKlinesArchive:
    data_root: Path
    client: BinanceDataArchiveClient

    def _out_path(self, symbol_canon: str, interval: str, year: int, month: int) -> Path:
        return (
            self.data_root
            / "raw"
            / "binance"
            / "um_premium_index_klines"
            / f"symbol={symbol_canon}"
            / f"interval={interval}"
            / f"part-{year:04d}-{month:02d}.parquet"
        )

    def _zip_url(self, venue_symbol: str, interval: str, year: int, month: int) -> str:
        # Binance Data Collection layout: data/futures/um/monthly/premiumIndexKlines/{SYMBOL}/{INTERVAL}/{SYMBOL}-{INTERVAL}-YYYY-MM.zip
        # Naming pattern matches their public examples for monthly zips. :contentReference[oaicite:4]{index=4}
        return (
            "https://data.binance.vision/data/futures/um/monthly/premiumIndexKlines/"
            f"{venue_symbol}/{interval}/{venue_symbol}-{interval}-{year:04d}-{month:02d}.zip"
        )

    def ingest_month(
        self,
        symbol_canon: str,
        interval: str,
        year: int,
        month: int,
        overwrite: bool = False,
    ) -> Optional[Dict]:
        venue_symbol = canon_to_venue_symbol_um(symbol_canon)
        out_path = self._out_path(symbol_canon, interval, year, month)
        if out_path.exists() and not overwrite:
            return None

        url = self._zip_url(venue_symbol, interval, year, month)
        tmp_dir = self.data_root / "meta" / "tmp" / "um_premium_index_klines"
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
            # pick first CSV in zip
            names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not names:
                raise RuntimeError(f"No CSV found in {zip_path}")
            with zf.open(names[0]) as f:
                raw = f.read()

        # archive files have no header
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

        # numeric times
        df["open_time"] = pd.to_numeric(df["open_time"], errors="coerce")
        df["close_time"] = pd.to_numeric(df["close_time"], errors="coerce")

        # drop junk/blank rows before integer cast
        df = df.dropna(subset=["open_time", "close_time"]).copy()
        if df.empty:
            raise RuntimeError(f"open_time parse failed (no numeric rows) for {zip_path}")

        # detect epoch unit from first valid value
        sample = int(df["open_time"].iloc[0])
        unit = _detect_epoch_unit(sample)

        # cast to int64 now that NA rows are gone
        df["open_time"] = df["open_time"].astype("int64")
        df["close_time"] = df["close_time"].astype("int64")

        # canonical timestamp
        df["ts"] = pd.to_datetime(df["open_time"], unit=unit, utc=True)


        # numeric OHLC
        for c in ["open", "high", "low", "close"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        # keep stable minimal columns
        out = df[["ts", "open_time", "close_time", "open", "high", "low", "close"]].copy()
        out["symbol"] = symbol_canon
        out["venue_symbol"] = venue_symbol
        out["interval"] = interval
        out["venue"] = "binance"
        out["dataset"] = "um_premium_index_klines"

        # basic integrity
        out = out.dropna(subset=["ts", "open_time", "open", "high", "low", "close"]).sort_values("ts")
        out = out.drop_duplicates(subset=["open_time"])

        out_path.parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(out_path, index=False)

        return {
            "symbol": symbol_canon,
            "venue_symbol": venue_symbol,
            "interval": interval,
            "year": year,
            "month": month,
            "rows": int(out.shape[0]),
            "ts_min": out["ts"].min().isoformat(),
            "ts_max": out["ts"].max().isoformat(),
            "file": str(out_path),
            "status": "ok",
            "url": url,
            "epoch_unit": unit,
        }

    def ingest_range(
        self,
        symbols: Iterable[str],
        intervals: Iterable[str],
        start: str,
        end: str,
        overwrite: bool = False,
    ) -> Dict:
        start_utc = datetime.fromisoformat(start.replace("Z", "+00:00")).astimezone(timezone.utc)
        end_utc = datetime.fromisoformat(end.replace("Z", "+00:00")).astimezone(timezone.utc)

        items = []
        for s in symbols:
            for itv in intervals:
                for (y, m) in _month_iter(start_utc, end_utc):
                    res = self.ingest_month(s, itv, y, m, overwrite=overwrite)
                    if res is not None:
                        items.append(res)

        ok = sum(1 for x in items if x.get("status") == "ok")
        miss = sum(1 for x in items if x.get("status") == "missing_archive_file")
        return {"summary": {"ok": ok, "missing_archive_file": miss, "total": len(items)}, "items": items}
