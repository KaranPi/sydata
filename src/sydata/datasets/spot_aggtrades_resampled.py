"""Spot aggTrades resampling helpers.

Goal
- Convert trade-level Binance spot aggTrades into bar-aligned aggregates (15m/1h/…)
  without ever concatenating the full trade tape.

Input layout (raw aggTrades parquet)
  {raw_root}/symbol=BTC-USDT/year=YYYY/month=MM/part-*.parquet

Output layout (resampled parquet)
  {norm_root}/interval=15m/year=YYYY/month=MM/symbol=BTC-USDT/part-YYYY-MM.parquet

Resampled columns (per symbol, per bar)
- sum_qty: total traded qty in bar
- trades: number of trades in bar
- cvd_qty: taker_buy_qty - taker_sell_qty (using is_buyer_maker)
- vwap: sum(price*qty) / sum(qty)
- last_trade_id: max agg_trade_id observed in bar
"""

from __future__ import annotations  

from dataclasses import dataclass  
from pathlib import Path  
from typing import Iterable  

import pandas as pd  
import numpy as np  

from sydata.io.symbols import load_basket, load_manifest  


@dataclass(frozen=True)
class AggResampleCfg:
    data_root: Path
    manifest_path: Path
    basket: str
    interval: str
    start: str
    end_excl: str

    # optional overrides
    symbols_override: list[str] | None = None
    raw_root: Path | None = None
    norm_root: Path | None = None


def interval_to_floor_str(interval: str) -> str:
    """
    '15m' -> '15min'
    '1h'  -> '1H'
    '1d'  -> '1D'
    """
    s = interval.strip().lower()
    if s.endswith("m"):
        n = int(s[:-1])
        return f"{n}min"
    if s.endswith("h"):
        n = int(s[:-1])
        return f"{n}H"
    if s.endswith("d"):
        n = int(s[:-1])
        return f"{n}D"
    raise ValueError(f"Unsupported interval: {interval!r}")


def parse_utc(ts_like: str | pd.Timestamp) -> pd.Timestamp:
    if isinstance(ts_like, pd.Timestamp):
        return ts_like.tz_convert("UTC") if ts_like.tzinfo else ts_like.tz_localize("UTC")
    return pd.Timestamp(ts_like, tz="UTC")


def resolve_symbols(cfg: AggResampleCfg) -> list[str]:
    if cfg.symbols_override:
        return list(cfg.symbols_override)
    spec = load_manifest(cfg.manifest_path)
    return load_basket(spec, cfg.basket)


def iter_year_months(start_utc: pd.Timestamp, end_excl_utc: pd.Timestamp) -> list[tuple[int, int]]:
    """
    Months that intersect [start_utc, end_excl_utc).
    Robust across pandas versions (no Period.to_timestamp(tz=...)).
    """
    s = start_utc.tz_convert("UTC")
    e = end_excl_utc.tz_convert("UTC")

    cur = pd.Timestamp(year=s.year, month=s.month, day=1, tz="UTC")
    last_inclusive = e - pd.Timedelta(seconds=1)
    last_month = pd.Timestamp(year=last_inclusive.year, month=last_inclusive.month, day=1, tz="UTC")

    out: list[tuple[int, int]] = []
    while cur <= last_month:
        out.append((cur.year, cur.month))
        cur = (cur + pd.offsets.MonthBegin(1)).tz_convert("UTC")
    return out


def _month_window(year: int, month: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    start = pd.Timestamp(year=year, month=month, day=1, tz="UTC")
    end_excl = (start + pd.offsets.MonthBegin(1)).tz_convert("UTC")
    return start, end_excl


def _raw_files_for_month(raw_root: Path, symbol: str, year: int, month: int) -> list[Path]:
    p = raw_root / f"symbol={symbol}" / f"year={year}" / f"month={month:02d}"
    return sorted(p.glob("part-*.parquet"))


def resampled_out_path(
    norm_root: Path, *, interval: str, symbol: str, year: int, month: int
) -> Path:
    """Canonical path for a (symbol, year, month) resampled parquet."""
    return (
        norm_root
        / f"interval={interval}"
        / f"year={year}"
        / f"month={month:02d}"
        / f"symbol={symbol}"
        / f"part-{year}-{month:02d}.parquet"
    )


def load_resampled_month(cfg: "AggResampleCfg", symbol: str, year: int, month: int) -> pd.DataFrame:
    """Convenience loader for notebooks."""
    outp = resampled_out_path(cfg.norm_root, interval=cfg.interval, symbol=symbol, year=year, month=month)
    return pd.read_parquet(outp)


def _agg_one_file(df: pd.DataFrame, floor_str: str) -> pd.DataFrame:
    """Aggregate raw aggTrades rows into bar-level microstructure features."""
    df = df.copy()

    # Ensure expected dtypes
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df["agg_trade_id"] = pd.to_numeric(df["agg_trade_id"], errors="coerce").astype("int64")
    df["price"] = pd.to_numeric(df["price"], errors="coerce").astype("float64")
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").astype("float64")
    df["is_buyer_maker"] = df["is_buyer_maker"].astype(bool)

    # Bucket
    df["bar_ts"] = df["ts"].dt.floor(floor_str)

    # Binance semantics:
    # isBuyerMaker == False => taker buy
    # isBuyerMaker == True  => taker sell
    is_sell = df["is_buyer_maker"].to_numpy()
    is_buy = ~is_sell
    qty = df["qty"].to_numpy(dtype="float64", copy=False)
    price = df["price"].to_numpy(dtype="float64", copy=False)

    buy_qty = np.where(is_buy, qty, 0.0)
    sell_qty = np.where(is_sell, qty, 0.0)
    buy_trades = is_buy.astype("int64")
    sell_trades = is_sell.astype("int64")

    buy_notional = np.where(is_buy, price * qty, 0.0)
    sell_notional = np.where(is_sell, price * qty, 0.0)

    df["_buy_qty"] = buy_qty
    df["_sell_qty"] = sell_qty
    df["_buy_trades"] = buy_trades
    df["_sell_trades"] = sell_trades
    df["_buy_notional"] = buy_notional
    df["_sell_notional"] = sell_notional

    gb = df.groupby("bar_ts", sort=True)

    agg = gb.agg(
        sum_qty=("qty", "sum"),
        trades=("qty", "size"),
        taker_buy_qty=("_buy_qty", "sum"),
        taker_sell_qty=("_sell_qty", "sum"),
        taker_buy_trades=("_buy_trades", "sum"),
        taker_sell_trades=("_sell_trades", "sum"),
        buy_notional=("_buy_notional", "sum"),
        sell_notional=("_sell_notional", "sum"),
    )

    # First/last by trade id (preferred ordering)
    idx_first = gb["agg_trade_id"].idxmin()
    idx_last = gb["agg_trade_id"].idxmax()

    first_rows = (
        df.loc[idx_first, ["bar_ts", "agg_trade_id", "price"]]
        .set_index("bar_ts")
        .rename(columns={"agg_trade_id": "first_trade_id", "price": "first_price"})
    )
    last_rows = (
        df.loc[idx_last, ["bar_ts", "agg_trade_id", "price"]]
        .set_index("bar_ts")
        .rename(columns={"agg_trade_id": "last_trade_id", "price": "last_price"})
    )

    agg = agg.join(first_rows, how="left").join(last_rows, how="left")

    # Derived identities
    agg["cvd_qty"] = agg["taker_buy_qty"] - agg["taker_sell_qty"]

    tot_notional = agg["buy_notional"] + agg["sell_notional"]
    agg["vwap"] = tot_notional / agg["sum_qty"]
    agg["buy_vwap"] = agg["buy_notional"] / agg["taker_buy_qty"]
    agg["sell_vwap"] = agg["sell_notional"] / agg["taker_sell_qty"]

    # Clean division edge cases
    for c in ("vwap", "buy_vwap", "sell_vwap"):
        agg[c] = agg[c].replace([np.inf, -np.inf], np.nan)

    out = agg.reset_index().rename(columns={"bar_ts": "ts"})
    return out


def _resample_files(
    files: list[Path],
    start_utc: pd.Timestamp,
    end_excl_utc: pd.Timestamp,
    floor_str: str,
) -> pd.DataFrame:
    # Read + aggregate each file, then combine by union(ts) with stable semantics.
    usecols = ["ts", "agg_trade_id", "price", "qty", "is_buyer_maker"]
    if not files:
        return pd.DataFrame()

    agg = None

    for p in files:
        df = pd.read_parquet(p, columns=usecols)
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
        df = df[(df["ts"] >= start_utc) & (df["ts"] < end_excl_utc)]
        if df.empty:
            continue

        g = _agg_one_file(df, floor_str).set_index("ts").sort_index()

        if agg is None:
            agg = g
            continue

        # Align on union index
        idx = agg.index.union(g.index)
        agg = agg.reindex(idx)
        g = g.reindex(idx)

        # Additive fields (safe with fillna)
        add_cols = [
            "sum_qty",
            "taker_buy_qty",
            "taker_sell_qty",
            "taker_buy_trades",
            "taker_sell_trades",
            "buy_notional",
            "sell_notional",
        ]
        for c in add_cols:
            agg[c] = agg[c].fillna(0.0) + g[c].fillna(0.0)

        # First by min(first_trade_id), carry its price
        a_first = agg["first_trade_id"]
        g_first = g["first_trade_id"]
        pick_first = g_first.notna() & (a_first.isna() | (g_first < a_first))
        agg.loc[pick_first, "first_trade_id"] = g_first[pick_first]
        agg.loc[pick_first, "first_price"] = g.loc[pick_first, "first_price"]

        # Last by max(last_trade_id), carry its price
        a_last = agg["last_trade_id"]
        g_last = g["last_trade_id"]
        pick_last = g_last.notna() & (a_last.isna() | (g_last > a_last))
        agg.loc[pick_last, "last_trade_id"] = g_last[pick_last]
        agg.loc[pick_last, "last_price"] = g.loc[pick_last, "last_price"]

    if agg is None or agg.empty:
        return pd.DataFrame()

    # Final derived identities (recompute so they’re internally consistent)
    agg["trades"] = (agg["taker_buy_trades"].fillna(0.0) + agg["taker_sell_trades"].fillna(0.0)).astype("int64")
    agg["cvd_qty"] = agg["taker_buy_qty"].fillna(0.0) - agg["taker_sell_qty"].fillna(0.0)

    tot_notional = agg["buy_notional"].fillna(0.0) + agg["sell_notional"].fillna(0.0)
    agg["vwap"] = tot_notional / agg["sum_qty"]
    agg["buy_vwap"] = agg["buy_notional"] / agg["taker_buy_qty"]
    agg["sell_vwap"] = agg["sell_notional"] / agg["taker_sell_qty"]
    for c in ("vwap", "buy_vwap", "sell_vwap"):
        agg[c] = agg[c].replace([np.inf, -np.inf], np.nan)

    # dtypes
    for c in ("sum_qty", "taker_buy_qty", "taker_sell_qty", "buy_notional", "sell_notional", "cvd_qty", "vwap", "buy_vwap", "sell_vwap", "first_price", "last_price"):
        if c in agg.columns:
            agg[c] = agg[c].astype("float64")

    for c in ("taker_buy_trades", "taker_sell_trades"):
        if c in agg.columns:
            agg[c] = agg[c].fillna(0.0).astype("int64")

    for c in ("first_trade_id", "last_trade_id"):
        if c in agg.columns:
            if agg[c].isna().any():
                agg[c] = agg[c].astype("Int64")
            else:
                agg[c] = agg[c].astype("int64")

    agg = agg.sort_index()
    return agg.reset_index()

def resample_month(cfg: AggResampleCfg, symbol: str, year: int, month: int) -> dict:
    start_utc = parse_utc(cfg.start)
    end_excl_utc = parse_utc(cfg.end_excl)
    floor_str = interval_to_floor_str(cfg.interval)

    raw_root = cfg.raw_root or (cfg.data_root / "raw" / "binance" / "spot_aggtrades")
    norm_root = cfg.norm_root or (cfg.data_root / "norm" / "spot_aggtrades_resampled")

    # set back into cfg-like locals (for notebook convenience)
    object.__setattr__(cfg, "raw_root", raw_root)   # type: ignore[misc]
    object.__setattr__(cfg, "norm_root", norm_root) # type: ignore[misc]

    m_start, m_end_excl = _month_window(year, month)
    w_start = max(start_utc, m_start)
    w_end_excl = min(end_excl_utc, m_end_excl)

    files = _raw_files_for_month(raw_root, symbol, year, month)
    outp = resampled_out_path(norm_root, interval=cfg.interval, symbol=symbol, year=year, month=month)

    if not files:
        outp.parent.mkdir(parents=True, exist_ok=True)
        empty = pd.DataFrame(
            columns=[
                "ts",
                "sum_qty",
                "trades",
                "taker_buy_qty",
                "taker_sell_qty",
                "taker_buy_trades",
                "taker_sell_trades",
                "buy_notional",
                "sell_notional",
                "cvd_qty",
                "vwap",
                "buy_vwap",
                "sell_vwap",
                "first_trade_id",
                "first_price",
                "last_trade_id",
                "last_price",
                "symbol",
            ]
        )
        empty.to_parquet(outp, index=False)
        return {"ok": True, "symbol": symbol, "year": year, "month": month, "out": str(outp), "rows": 0, "note": "no raw files"}

    df = _resample_files(files, floor_str=floor_str, start_utc=w_start, end_excl_utc=w_end_excl)
    df["symbol"] = symbol

    outp.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(outp, index=False)

    return {
        "ok": True,
        "symbol": symbol,
        "year": year,
        "month": month,
        "out": str(outp),
        "rows": int(len(df)),
        "min_ts": (df["ts"].min().isoformat() if len(df) else None),
        "max_ts": (df["ts"].max().isoformat() if len(df) else None),
    }


def run_resample(cfg: AggResampleCfg) -> dict[tuple[str, int, int], dict]:
    start_utc = parse_utc(cfg.start)
    end_excl_utc = parse_utc(cfg.end_excl)

    symbols = resolve_symbols(cfg)
    months = iter_year_months(start_utc, end_excl_utc)

    results: dict[tuple[str, int, int], dict] = {}
    for sym in symbols:
        for (y, m) in months:
            results[(sym, y, m)] = resample_month(cfg, sym, y, m)
    return results
