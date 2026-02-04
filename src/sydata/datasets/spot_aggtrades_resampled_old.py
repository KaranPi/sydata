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


def _resample_files(
    files: Iterable[Path],
    *,
    floor_str: str,
    start_utc: pd.Timestamp,
    end_excl_utc: pd.Timestamp,
) -> pd.DataFrame:
    """Incrementally aggregate each parquet part -> bar aggregates, then sum across parts."""
    agg: pd.DataFrame | None = None
    usecols = ["ts", "agg_trade_id", "price", "qty", "is_buyer_maker", "symbol"]

    for fp in files:
        df = pd.read_parquet(fp, columns=usecols)

        # defensive ts typing (some writers store as int ms)
        if not pd.api.types.is_datetime64_any_dtype(df["ts"]):
            df["ts"] = pd.to_datetime(df["ts"], utc=True)
        else:
            try:
                df["ts"] = df["ts"].dt.tz_convert("UTC")
            except Exception:
                df["ts"] = df["ts"].dt.tz_localize("UTC")

        df = df[(df["ts"] >= start_utc) & (df["ts"] < end_excl_utc)]
        if df.empty:
            continue

        df["bar_ts"] = df["ts"].dt.floor(floor_str)

        # CVD sign convention:
        # is_buyer_maker=True  -> buyer is maker -> taker sell -> negative
        # is_buyer_maker=False -> taker buy -> positive
        df["signed_qty"] = df["qty"].where(~df["is_buyer_maker"], -df["qty"])
        df["notional"] = df["price"] * df["qty"]

        g = (
            df.groupby("bar_ts", sort=False)
            .agg(
                sum_qty=("qty", "sum"),
                trades=("agg_trade_id", "size"),
                cvd_qty=("signed_qty", "sum"),
                sum_notional=("notional", "sum"),
                last_trade_id=("agg_trade_id", "max"),
            )
            .reset_index()
            .rename(columns={"bar_ts": "ts"})
        )
        g["vwap"] = g["sum_notional"] / g["sum_qty"]
        g = g.drop(columns=["sum_notional"])

        if agg is None:
            agg = g
        else:
            # align on ts; sum additive columns; max last_trade_id; recompute vwap via qty-weight later
            agg = agg.set_index("ts")
            g = g.set_index("ts")

            if "sum_qty" not in agg.columns:
                raise RuntimeError("internal aggregation state corrupted")

            # additive columns
            for c in ["sum_qty", "trades", "cvd_qty"]:
                agg[c] = agg[c].add(g[c], fill_value=0)

            # vwap: convert both to notional, add, then divide
            agg_notional = agg["vwap"] * agg["sum_qty"]
            g_notional = g["vwap"] * g["sum_qty"]
            notional = agg_notional.add(g_notional, fill_value=0)
            agg["vwap"] = notional / agg["sum_qty"]

            # last_trade_id: max
            agg["last_trade_id"] = agg["last_trade_id"].combine(g["last_trade_id"], max)

            agg = agg.reset_index()

    if agg is None:
        return pd.DataFrame(columns=["ts", "sum_qty", "trades", "cvd_qty", "vwap", "last_trade_id"])

    return agg.sort_values("ts").reset_index(drop=True)


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
        empty = pd.DataFrame(columns=["ts", "sum_qty", "trades", "cvd_qty", "vwap", "last_trade_id", "symbol"])
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
