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
    # ms ~ 1e12-1e13, us ~ 1e15-1e16, ns ~ 1e18-1e19
    if x > 10**17:
        return "ns"
    if x > 10**14:
        return "us"
    return "ms"


def _month_iter(start_utc: datetime, end_utc: datetime) -> Iterator[Tuple[int, int]]:
    cur = datetime(start_utc.year, start_utc.month, 1, tzinfo=timezone.utc)
    endm = datetime(end_utc.year, end_utc.month, 1, tzinfo=timezone.utc)
    while cur <= endm:
        yield cur.year, cur.month
        if cur.month == 12:
            cur = datetime(cur.year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            cur = datetime(cur.year, cur.month + 1, 1, tzinfo=timezone.utc)


def _normalize_funding_df(raw: pd.DataFrame, symbol_canon: str, venue_symbol: str) -> pd.DataFrame:
    """
    Supports:
      A) Headered: columns like fundingTime/fundingRate/markPrice
      B) Headerless: 2-4 columns, inferred by width
    """
    df = raw.copy()

    # Case A: headered
    rename = {
        "fundingTime": "funding_time",
        "funding_time": "funding_time",
        "fundingRate": "funding_rate",
        "funding_rate": "funding_rate",
        "markPrice": "mark_price",
        "mark_price": "mark_price",
        "symbol": "venue_symbol",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    if "funding_time" not in df.columns or "funding_rate" not in df.columns:
        # Case B: headerless / unknown columns
        w = df.shape[1]
        cols = list(df.columns)

        if w >= 4:
            # likely: symbol, fundingTime, fundingRate, markPrice
            df = df.rename(columns={cols[0]: "venue_symbol", cols[1]: "funding_time", cols[2]: "funding_rate", cols[3]: "mark_price"})
        elif w == 3:
            # likely: fundingTime, fundingRate, markPrice
            df = df.rename(columns={cols[0]: "funding_time", cols[1]: "funding_rate", cols[2]: "mark_price"})
        elif w == 2:
            # likely: fundingTime, fundingRate
            df = df.rename(columns={cols[0]: "funding_time", cols[1]: "funding_rate"})
        else:
            raise ValueError(f"Unexpected fundingRate width={w}; cols={cols}")

    # coerce numeric
    df["funding_time"] = pd.to_numeric(df["funding_time"], errors="coerce")
    df["funding_rate"] = pd.to_numeric(df["funding_rate"], errors="coerce")
    if "mark_price" in df.columns:
        df["mark_price"] = pd.to_numeric(df["mark_price"], errors="coerce")
    else:
        df["mark_price"] = pd.NA


    # If funding_rate is actually an interval column (typically 8), fix mapping.
    # Heuristic: interval is a small integer with very few unique values.
    if "funding_rate" in df.columns:
        fr = df["funding_rate"].dropna()
        if not fr.empty:
            uniq = fr.unique()
            if len(uniq) <= 5 and fr.max() <= 24 and (fr.astype("int64") == fr).all():
                df["funding_interval_hours"] = df["funding_rate"].astype("Int64")

                # In your outputs, the "mark_price" column is actually the funding rate.
                # Safe for core_major: funding rates are typically |rate| < 0.1.
                if "mark_price" in df.columns:
                    mp = pd.to_numeric(df["mark_price"], errors="coerce")
                    if mp.notna().any() and mp.abs().max() < 0.1:
                        df["funding_rate"] = mp
                        df["mark_price"] = pd.NA


    df = df.dropna(subset=["funding_time", "funding_rate"]).copy()
    if df.empty:
        raise RuntimeError("No valid rows after parsing funding_time/funding_rate")

    # drop junk
    df = df.dropna(subset=["funding_time"]).copy()
    if df.empty:
        raise RuntimeError("No valid rows after parsing funding_time")

    # normalize funding_time to milliseconds
    ft = pd.to_numeric(df["funding_time"], errors="coerce")
    df = df.dropna(subset=["funding_time"]).copy()
    ft = df["funding_time"].astype("int64")

    sample = int(ft.iloc[0])
    unit = _detect_epoch_unit(sample)
    if unit == "ns":
        ft = ft // 1_000_000
    elif unit == "us":
        ft = ft // 1_000

    # snap to exact millisecond (kills 0.001s drift)
    ft = (ft // 1000) * 1000
    df["funding_time"] = ft
    df["ts"] = pd.to_datetime(df["funding_time"], unit="ms", utc=True)

    # coerce numeric (after time fix)
    df["funding_rate"] = pd.to_numeric(df["funding_rate"], errors="coerce") if "funding_rate" in df.columns else pd.NA
    df["mark_price"]   = pd.to_numeric(df["mark_price"], errors="coerce") if "mark_price" in df.columns else pd.NA

    # If funding_rate is actually the interval column (8), swap rate from mark_price
    fr = df["funding_rate"].dropna()
    if not fr.empty:
        uniq = fr.unique()
        if len(uniq) <= 5 and fr.max() <= 24 and (fr.astype("int64") == fr).all():
            df["funding_interval_hours"] = df["funding_rate"].astype("Int64")
            mp = pd.to_numeric(df["mark_price"], errors="coerce")
            if mp.notna().any() and mp.abs().max() < 0.1:
                df["funding_rate"] = mp
                df["mark_price"] = pd.NA

    # require funding_rate now
    df = df.dropna(subset=["funding_rate"]).copy()
    if df.empty:
        raise RuntimeError("No valid rows after resolving funding_rate")

    # identity
    df["symbol"] = symbol_canon
    df["venue_symbol"] = venue_symbol

    # stable output
    keep = ["ts", "funding_time", "funding_rate", "mark_price", "symbol", "venue_symbol"]
    if "funding_interval_hours" in df.columns:
        keep.insert(3, "funding_interval_hours")

    out = df[keep].drop_duplicates(subset=["symbol", "funding_time"]).sort_values("ts").reset_index(drop=True)
    out["venue"] = "binance"
    out["dataset"] = "um_funding_rate"
    return out



@dataclass
class UmFundingRateArchive:
    data_root: Path
    client: BinanceDataArchiveClient

    def _out_path(self, symbol_canon: str, year: int, month: int) -> Path:
        return (
            self.data_root
            / "raw"
            / "binance"
            / "um_funding_rate"
            / f"symbol={symbol_canon}"
            / f"part-{year:04d}-{month:02d}.parquet"
        )

    def _zip_url(self, venue_symbol: str, year: int, month: int) -> str:
        return (
            "https://data.binance.vision/data/futures/um/monthly/fundingRate/"
            f"{venue_symbol}/{venue_symbol}-fundingRate-{year:04d}-{month:02d}.zip"
        )

    def ingest_month(self, symbol_canon: str, year: int, month: int, overwrite: bool = False) -> Dict:
        venue_symbol = canon_to_venue_symbol_um(symbol_canon)
        out_path = self._out_path(symbol_canon, year, month)
        if out_path.exists() and not overwrite:
            return {
                "symbol": symbol_canon,
                "venue_symbol": venue_symbol,
                "year": year,
                "month": month,
                "status": "already_exists",
                "file": str(out_path),
            }

        url = self._zip_url(venue_symbol, year, month)

        cache_dir = self.data_root / "meta" / "cache" / "binance_zips" / "um_monthly_fundingRate" / venue_symbol
        cache_dir.mkdir(parents=True, exist_ok=True)
        zip_path = cache_dir / f"{venue_symbol}-fundingRate-{year:04d}-{month:02d}.zip"

        try:
            if not zip_path.exists():
                self.client.download_zip(url=url, dest_zip=zip_path)
        except FileNotFoundError:
            return {
                "symbol": symbol_canon,
                "venue_symbol": venue_symbol,
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
                raw_bytes = f.read()

        # Try headered first; if it looks wrong, fall back to headerless
        try:
            raw_df = pd.read_csv(io.BytesIO(raw_bytes))
            if raw_df.shape[1] < 2:
                raise ValueError("too few columns")
        except Exception:
            raw_df = pd.read_csv(io.BytesIO(raw_bytes), header=None)

        out = _normalize_funding_df(raw_df, symbol_canon=symbol_canon, venue_symbol=venue_symbol)

        out_path.parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(out_path, index=False)

        return {
            "symbol": symbol_canon,
            "venue_symbol": venue_symbol,
            "year": year,
            "month": month,
            "rows": int(out.shape[0]),
            "ts_min": out["ts"].min().isoformat(),
            "ts_max": out["ts"].max().isoformat(),
            "file": str(out_path),
            "status": "ok",
            "url": url,
        }

    def ingest_range(
        self,
        symbols: Iterable[str],
        start: str,
        end: str,
        overwrite: bool = False,
    ) -> Dict:
        start_utc = datetime.fromisoformat(start.replace("Z", "+00:00")).astimezone(timezone.utc)
        end_utc = datetime.fromisoformat(end.replace("Z", "+00:00")).astimezone(timezone.utc)

        items = []
        for s in symbols:
            for (y, m) in _month_iter(start_utc, end_utc):
                items.append(self.ingest_month(s, y, m, overwrite=overwrite))

        ok = sum(1 for x in items if x.get("status") == "ok")
        miss = sum(1 for x in items if x.get("status") == "missing_archive_file")
        exists = sum(1 for x in items if x.get("status") == "already_exists")
        return {"summary": {"ok": ok, "missing_archive_file": miss, "already_exists": exists, "total": len(items)}, "items": items}
