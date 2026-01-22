from __future__ import annotations  # no installation needed

from dataclasses import dataclass  # no installation needed
from datetime import date  # no installation needed
from pathlib import Path  # no installation needed
from typing import Iterable, Optional  # no installation needed
import io  # no installation needed
import zipfile  # no installation needed

import pandas as pd  # already in env — no new install

from sydata.providers.binance_data_archive import BinanceDataArchiveClient  # project-local


def canon_to_venue_symbol_spot(canon: str) -> str:
    # "BTC-USDT" -> "BTCUSDT"
    return canon.replace("-", "").upper()


def _detect_epoch_unit(x: int) -> str:
    """
    Binance public spot data timestamps:
      - <= 2024: milliseconds
      - >= 2025-01-01: microseconds (documented)
    Detect by magnitude (robust even if mixed). :contentReference[oaicite:1]{index=1}
    """
    if x >= 10**15:
        return "us"
    if x >= 10**12:
        return "ms"
    if x >= 10**9:
        return "s"
    return "ms"


def _coerce_bool(s: pd.Series) -> pd.Series:
    if s.dtype == bool:
        return s
    # common forms: True/False, 0/1, "true"/"false"
    return s.astype(str).str.lower().map({"true": True, "false": False, "1": True, "0": False})


def normalize_aggtrades_df(raw: pd.DataFrame, symbol_canon: str) -> pd.DataFrame:
    """
    Handles two common formats:
      A) headered CSV with semantic names
      B) headerless / generic columns (positional)
    Expected columns per Binance public-data doc:
      [aggTradeId, price, qty, firstTradeId, lastTradeId, timestamp, isBuyerMaker, isBestMatch]
    """  # no installation needed

    df = raw.copy()

    # If the file is positional (no expected column names), map by position.
    # Typical positional order per Binance public-data doc. :contentReference[oaicite:2]{index=2}
    expected_pos = [
        "agg_trade_id",
        "price",
        "qty",
        "first_trade_id",
        "last_trade_id",
        "ts_raw",
        "is_buyer_maker",
        "is_best_match",
    ]

    # Heuristic: if none of the known headers exist, treat as positional.
    known_headers = {"aggTradeId", "agg_trade_id", "price", "qty", "quantity", "timestamp", "transact_time", "isBuyerMaker", "m"}
    if len(known_headers.intersection(set(df.columns))) == 0:
        # Map first 8 cols only
        cols = list(df.columns)[:8]
        rename_map = {c: expected_pos[i] for i, c in enumerate(cols)}
        df = df.rename(columns=rename_map)

    # Map header variants
    rename_variants = {
        "aggTradeId": "agg_trade_id",
        "a": "agg_trade_id",
        "price": "price",
        "p": "price",
        "qty": "qty",
        "q": "qty",
        "quantity": "qty",
        "firstTradeId": "first_trade_id",
        "f": "first_trade_id",
        "lastTradeId": "last_trade_id",
        "l": "last_trade_id",
        "timestamp": "ts_raw",
        "T": "ts_raw",
        "transact_time": "ts_raw",
        "transactTime": "ts_raw",
        "isBuyerMaker": "is_buyer_maker",
        "m": "is_buyer_maker",
        "isBestMatch": "is_best_match",
        "M": "is_best_match",
    }
    df = df.rename(columns={k: v for k, v in rename_variants.items() if k in df.columns})

    needed = {"price", "qty", "ts_raw", "is_buyer_maker"}
    missing = needed.difference(df.columns)
    if missing:
        raise ValueError(f"aggTrades missing required columns {sorted(missing)}; have={list(df.columns)}")

    # numeric
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce")

    # time
    ts_int = pd.to_numeric(df["ts_raw"], errors="coerce").astype("Int64")
    # pick a unit using the first non-null sample
    sample = ts_int.dropna().iloc[0]
    unit = _detect_epoch_unit(int(sample))
    df["ts"] = pd.to_datetime(ts_int.astype("int64"), unit=unit, utc=True)

    # bool
    df["is_buyer_maker"] = _coerce_bool(df["is_buyer_maker"]).astype("boolean")

    # ids if present
    if "agg_trade_id" in df.columns:
        df["agg_trade_id"] = pd.to_numeric(df["agg_trade_id"], errors="coerce").astype("Int64")

    # lineage
    df["symbol"] = symbol_canon
    df["venue"] = "binance"
    df["dataset"] = "spot_aggtrades"

    # keep minimal stable columns (+ id if present)
    cols = ["ts", "price", "qty", "is_buyer_maker", "symbol", "venue", "dataset"]
    if "agg_trade_id" in df.columns:
        cols.insert(1, "agg_trade_id")

    df = df[cols].dropna(subset=["ts", "price", "qty", "is_buyer_maker"])

    # dedup
    if "agg_trade_id" in df.columns:
        df = df.drop_duplicates(subset=["symbol", "agg_trade_id"])
    else:
        df = df.drop_duplicates(subset=["symbol", "ts", "price", "qty", "is_buyer_maker"])

    df = df.sort_values("ts").reset_index(drop=True)
    return df


@dataclass
class SpotAggTradesIngestor:
    data_root: Path
    client: BinanceDataArchiveClient

    def out_path_month(self, symbol_canon: str, year: int, month: int) -> Path:
        return (
            self.data_root
            / "raw"
            / "binance"
            / "spot_aggtrades"
            / f"symbol={symbol_canon}"
            / f"year={year:04d}"
            / f"month={month:02d}"
            / f"part-{year:04d}-{month:02d}.parquet"
        )

    def ingest_month(self, symbol_canon: str, year: int, month: int, cache_dir: Optional[Path] = None) -> dict:
        venue_symbol = canon_to_venue_symbol_spot(symbol_canon)
        url = self.client.spot_aggtrades_monthly_url(venue_symbol, year, month)

        cache_dir = cache_dir or (self.data_root / "meta" / "cache" / "binance_zips")
        zip_path = cache_dir / "spot_monthly_aggTrades" / venue_symbol / f"{venue_symbol}-aggTrades-{year:04d}-{month:02d}.zip"

        out_path = self.out_path_month(symbol_canon, year, month)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        # download (idempotent if already present)
        if not zip_path.exists():
            self.client.download_zip(url, zip_path)

        # read CSV from zip
        with zipfile.ZipFile(zip_path) as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                raise ValueError(f"No CSV inside {zip_path}")
            with zf.open(csv_names[0]) as f:
                raw = pd.read_csv(f)
            # if headerless, re-read correctly
            expected_any = {"aggTradeId", "price", "qty", "timestamp", "isBuyerMaker"}
            if len(expected_any.intersection(set(raw.columns))) == 0:
                with zf.open(csv_names[0]) as f:
                    raw = pd.read_csv(f, header=None)

        df = normalize_aggtrades_df(raw, symbol_canon)

        # validations
        if not (df["price"] > 0).all():
            raise ValueError("Found non-positive prices")
        if not (df["qty"] >= 0).all():
            raise ValueError("Found negative qty")

        # write deterministic (overwrite OK)
        df.to_parquet(out_path, index=False)

        return {
            "symbol": symbol_canon,
            "venue_symbol": venue_symbol,
            "year": year,
            "month": month,
            "rows": int(df.shape[0]),
            "ts_min": df["ts"].min().isoformat(),
            "ts_max": df["ts"].max().isoformat(),
            "file": str(out_path),
        }
