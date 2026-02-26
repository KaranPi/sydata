from __future__ import annotations  

from dataclasses import dataclass  
from pathlib import Path  
from typing import Any  

import pandas as pd  

from sydata.datasets.master_join_aggtrades import (  
    MasterAggJoinCfg,
    ensure_ts_utc,
    iter_year_months,
    month_part_path,
    resolve_symbols,
)


def year_part_path(root: Path, interval: str, year: int) -> Path:
    return root / f"interval={interval}" / f"year={year}" / f"part-{year}.parquet"


@dataclass(frozen=True)
class BuildInfo:
    ok: bool
    year: int
    rows: int
    symbols: int
    ts_unique: bool
    out: str
    details: dict[str, Any]


def _read_joined_partition(cfg: MasterAggJoinCfg, symbol: str, year: int, month: int) -> pd.DataFrame:
    p = month_part_path(cfg.out_root, cfg.interval, year, month, symbol)
    df = pd.read_parquet(p)
    df = ensure_ts_utc(df, "ts")
    # enforce symbol presence (join code usually ensures this, but keep it hard)
    if "symbol" not in df.columns:
        df["symbol"] = symbol
    return df


def _qc_long(df: pd.DataFrame) -> dict[str, Any]:
    # core keys
    req = {"ts", "symbol"}
    missing = sorted(req - set(df.columns))
    if missing:
        return {"ok": False, "reason": f"missing required columns: {missing}"}

    dup = int(df[["ts", "symbol"]].duplicated().sum())
    ts_unique = bool(df.groupby("symbol")["ts"].nunique().min() == df.groupby("symbol")["ts"].nunique().max())

    return {
        "ok": dup == 0,
        "dups_ts_symbol": dup,
        "symbols": int(df["symbol"].nunique()),
        "ts_unique_per_symbol": ts_unique,
        "rows": int(len(df)),
        "cols": int(df.shape[1]),
    }


def build_long_year(cfg: MasterAggJoinCfg, year: int, out_long_root: Path) -> BuildInfo:
    """
    Builds a year-level long parquet by concatenating monthly joined partitions from cfg.out_root.
    Uses cfg.start/end_excl to decide which months to include for that year.
    """
    start_utc = pd.Timestamp(cfg.start, tz="UTC")
    end_excl_utc = pd.Timestamp(cfg.end_excl, tz="UTC")

    symbols = resolve_symbols(cfg)
    ym = [(y, m) for (y, m) in iter_year_months(start_utc, end_excl_utc) if y == year]

    frames: list[pd.DataFrame] = []
    for sym in symbols:
        for (y, m) in ym:
            frames.append(_read_joined_partition(cfg, sym, y, m))

    if not frames:
        out = year_part_path(out_long_root, cfg.interval, year)
        return BuildInfo(
            ok=False,
            year=year,
            rows=0,
            symbols=0,
            ts_unique=False,
            out=str(out),
            details={"reason": "no partitions selected for this year under cfg.start/end_excl"},
        )

    df = pd.concat(frames, ignore_index=True)
    df = ensure_ts_utc(df, "ts")

    # stable ordering for downstream reproducibility
    df = df.sort_values(["ts", "symbol"], kind="mergesort").reset_index(drop=True)

    qc = _qc_long(df)

    out = year_part_path(out_long_root, cfg.interval, year)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)

    return BuildInfo(
        ok=bool(qc["ok"]),
        year=year,
        rows=int(qc["rows"]),
        symbols=int(qc["symbols"]),
        ts_unique=bool(qc["ts_unique_per_symbol"]),
        out=str(out),
        details=qc,
    )


def build_wide_year_from_long(long_path: Path, out_wide_root: Path, interval: str, year: int) -> BuildInfo:
    """
    Reads the year-level long parquet and pivots it into a wide frame:
      index: ts
      columns: <feature>__<symbol>
    """
    df = pd.read_parquet(long_path)
    df = ensure_ts_utc(df, "ts")

    qc = _qc_long(df)
    if not qc["ok"]:
        out = year_part_path(out_wide_root, interval, year)
        return BuildInfo(
            ok=False,
            year=year,
            rows=int(len(df)),
            symbols=int(df["symbol"].nunique()) if "symbol" in df.columns else 0,
            ts_unique=bool(qc.get("ts_unique_per_symbol", False)),
            out=str(out),
            details={"reason": "long QC failed; refusing to build wide", "long_qc": qc},
        )

    value_cols = [c for c in df.columns if c not in ("ts", "symbol")]
    base = df.set_index(["ts", "symbol"])[value_cols]

    wide = base.unstack("symbol")  # columns MultiIndex: (feature, symbol)
    wide.columns = [f"{feat}__{sym}" for (feat, sym) in wide.columns.to_list()]
    wide = wide.sort_index()

    out = year_part_path(out_wide_root, interval, year)
    out.parent.mkdir(parents=True, exist_ok=True)
    wide.to_parquet(out, index=True)

    return BuildInfo(
        ok=True,
        year=year,
        rows=int(wide.shape[0]),
        symbols=int(df["symbol"].nunique()),
        ts_unique=True,
        out=str(out),
        details={
            "wide_rows": int(wide.shape[0]),
            "wide_cols": int(wide.shape[1]),
            "value_cols": int(len(value_cols)),
        },
    )
