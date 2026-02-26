from __future__ import annotations  

from dataclasses import dataclass  
from pathlib import Path  
from typing import Any, Iterable  

import pandas as pd  

from sydata.datasets.microagg_schema import (  
    AGG_COLS,
    AGG_RENAME,
    micro_keep_cols,
)

from sydata.io.symbols import load_basket, load_manifest  


# ---------- config ----------

@dataclass(frozen=True)
class MasterAggJoinCfg:
    # root folder (your marketdata root)
    data_root: Path

    # optional explicit norm root (marketdata/norm)
    norm_root: Path | None = None

    # if left as defaults, these will be re-anchored in __post_init__
    master_root: Path = Path("norm/master")
    agg_root: Path = Path("norm/spot_aggtrades_resampled")
    out_root: Path = Path("norm/master_plus_microaggtrades")
    manifest_path: Path = Path("norm/manifest.json")

    # basket can be either:
    # - str: basket name in manifest
    # - list[str]: explicit symbol list
    basket: str | list[str] | None = None

    interval: str = "15m"
    start: str = "2025-01-01"
    end_excl: str = "2026-01-01"

    # explicit override always wins
    symbols_override: list[str] | None = None

    def __post_init__(self) -> None:
        # Determine base norm dir
        base_norm = self.norm_root if self.norm_root is not None else (self.data_root / "norm")
    
        # Manifest auto-discovery:
        # Many setups store it under {data_root}/meta instead of {data_root}/norm
        base_meta = self.data_root / "meta"

        # Only auto-resolve if user left the default OR the current path doesn't exist
        if (self.manifest_path == Path("norm/manifest.json")) or (not Path(self.manifest_path).exists()):
            candidates = [
                base_norm / "manifest.json",
                base_norm / "manifest.yaml",
                base_norm / "manifest.yml",
                base_meta / "manifest.json",
                base_meta / "manifest.yaml",
                base_meta / "manifest.yml",
            ]
            for p in candidates:
                if p.exists():
                    object.__setattr__(self, "manifest_path", p)
                    break
    

        # Only rewrite if user left defaults (keeps explicit paths untouched)
        if self.master_root == Path("norm/master"):
            object.__setattr__(self, "master_root", base_norm / "master")
        if self.agg_root == Path("norm/spot_aggtrades_resampled"):
            object.__setattr__(self, "agg_root", base_norm / "spot_aggtrades_resampled")
        if self.out_root == Path("norm/master_plus_microaggtrades"):
            object.__setattr__(self, "out_root", base_norm / "master_plus_microaggtrades")
        if self.manifest_path == Path("norm/manifest.json"):
            object.__setattr__(self, "manifest_path", base_norm / "manifest.json")


# ---------- small utilities ----------

def iter_year_months(start_utc: pd.Timestamp, end_excl_utc: pd.Timestamp) -> list[tuple[int, int]]:
    """
    Months that intersect [start_utc, end_excl_utc).
    """
    if start_utc.tz is None or end_excl_utc.tz is None:
        raise ValueError("start_utc/end_excl_utc must be timezone-aware (UTC).")

    # inclusive month range
    cursor = pd.Timestamp(start_utc.year, start_utc.month, 1, tz="UTC")
    end_last = end_excl_utc - pd.Timedelta(microseconds=1)
    end_month = pd.Timestamp(end_last.year, end_last.month, 1, tz="UTC")

    out: list[tuple[int, int]] = []
    while cursor <= end_month:
        out.append((int(cursor.year), int(cursor.month)))
        cursor = cursor + pd.offsets.MonthBegin(1)
    return out


def month_part_path(root: Path, interval: str, year: int, month: int, symbol: str) -> Path:
    return (
        root
        / f"interval={interval}"
        / f"year={year}"
        / f"month={month:02d}"
        / f"symbol={symbol}"
        / f"part-{year}-{month:02d}.parquet"
    )


def ensure_ts_utc(df: pd.DataFrame, col: str = "ts") -> pd.DataFrame:
    if col not in df.columns:
        raise ValueError(f"Missing '{col}' in df. cols={list(df.columns)}")

    s = df[col]
    if not pd.api.types.is_datetime64_any_dtype(s):
        df[col] = pd.to_datetime(df[col], utc=True)
    else:
        # handle tz-naive datetime64[ns]
        if getattr(s.dt, "tz", None) is None:
            df[col] = df[col].dt.tz_localize("UTC")
        else:
            df[col] = df[col].dt.tz_convert("UTC")
    return df


def assert_unique_key(df: pd.DataFrame, key_cols: list[str], name: str) -> None:
    dup = df.duplicated(key_cols).any()
    if dup:
        n = int(df.duplicated(key_cols).sum())
        raise ValueError(f"{name}: duplicated keys on {key_cols} (n={n})")


def resolve_symbols(cfg: MasterAggJoinCfg) -> list[str]:
    # 1) explicit override always wins
    if cfg.symbols_override:
        return list(dict.fromkeys(cfg.symbols_override))

    # 2) basket can be an explicit list/tuple of symbols -> no manifest needed
    if cfg.basket is not None:
        if isinstance(cfg.basket, (list, tuple)):
            return list(dict.fromkeys([str(x) for x in cfg.basket]))

        # basket name in manifest
        if isinstance(cfg.basket, str):
            spec = load_manifest(cfg.manifest_path)
            return load_basket(spec, cfg.basket)

        raise TypeError(f"cfg.basket must be str | list[str] | None, got {type(cfg.basket)}")

    # 3) fallback: all symbols from manifest
    spec = load_manifest(cfg.manifest_path)
    symbols = spec.get("symbols", {})
    if isinstance(symbols, dict):
        return sorted(symbols.keys())

    raise ValueError("manifest 'symbols' must be a dict")


# ---------- core join ----------

#AGG_RENAME = {
#    # existing bar aggregates
#    "sum_qty": "agg_sum_qty",
#    "trades": "agg_trades",
#    "cvd_qty": "agg_cvd_qty",
#    "vwap": "agg_vwap",
#    "last_trade_id": "agg_last_trade_id",

    # taker-side microstructure
#    "taker_buy_qty": "agg_taker_buy_qty",
#    "taker_sell_qty": "agg_taker_sell_qty",
#    "taker_buy_trades": "agg_taker_buy_trades",
#    "taker_sell_trades": "agg_taker_sell_trades",
#    "buy_notional": "agg_buy_notional",
#    "sell_notional": "agg_sell_notional",
#    "buy_vwap": "agg_buy_vwap",
#    "sell_vwap": "agg_sell_vwap",
#    "first_price": "agg_first_price",
#    "last_price": "agg_last_price",
#    "first_trade_id": "agg_first_trade_id",
#}


def join_master_with_aggtrades(master_df: pd.DataFrame, agg_df: pd.DataFrame) -> pd.DataFrame:
    """
    Left-join aggtrades resampled onto master on (ts, symbol).

    master required: ts, symbol
    agg required: ts, symbol, [sum_qty, trades, cvd_qty, vwap, last_trade_id] (any subset allowed)
    """
    m = master_df.copy()
    a = agg_df.copy()

    m = ensure_ts_utc(m, "ts")
    a = ensure_ts_utc(a, "ts")

    # --- allow per-symbol master slices that omit 'symbol' (symbol is in partition path) ---
    if "symbol" not in m.columns:
        if "symbol" in a.columns and a["symbol"].nunique(dropna=True) == 1:
            m["symbol"] = a["symbol"].iloc[0]
        else:
            raise ValueError("master_df missing 'symbol' and cannot infer from agg_df")

    if "symbol" not in a.columns:
        if "symbol" in m.columns and m["symbol"].nunique(dropna=True) == 1:
            a["symbol"] = m["symbol"].iloc[0]
        else:
            raise ValueError("agg_df missing 'symbol' and cannot infer from master_df")

    # if both are single-symbol, enforce equality (prevents silent all-NA joins)
    if (
        "symbol" in m.columns
        and "symbol" in a.columns
        and m["symbol"].nunique(dropna=True) == 1
        and a["symbol"].nunique(dropna=True) == 1
    ):
        ms = m["symbol"].iloc[0]
        as_ = a["symbol"].iloc[0]
        if ms != as_:
            raise ValueError(f"symbol mismatch: master={ms} agg={as_}")


    assert_unique_key(m, ["ts", "symbol"], "master")
    assert_unique_key(a, ["ts", "symbol"], "agg")

    # rename agg cols to avoid collision with master volume/trades/etc.
    keep = micro_keep_cols(include_symbol=("symbol" in a.columns))
    a = a.loc[:, [c for c in keep if c in a.columns]]
    a = a[keep].rename(columns={k: v for k, v in AGG_RENAME.items() if k in a.columns})

    out = m.merge(a, on=["ts", "symbol"], how="left", validate="one_to_one")
    return out


def qc_joined(df: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {
        "rows": int(len(df)),
        "ts_unique": bool(df[["ts", "symbol"]].duplicated().sum() == 0),
    }
    for c in AGG_RENAME.values():
        out[f"{c}_na_frac"] = float(df[c].isna().mean()) if c in df.columns else 1.0
    return out


# ---------- batch runner (month-by-month, symbol-by-symbol) ----------

def run_monthly_join(cfg: MasterAggJoinCfg) -> dict[tuple[str, int, int], dict[str, Any]]:
    start_utc = pd.Timestamp(cfg.start, tz="UTC")
    end_excl_utc = pd.Timestamp(cfg.end_excl, tz="UTC")

    symbols = resolve_symbols(cfg)
    ym = iter_year_months(start_utc, end_excl_utc)

    results: dict[tuple[str, int, int], dict[str, Any]] = {}

    for symbol in symbols:
        for (year, month) in ym:
            master_path = month_part_path(cfg.master_root, cfg.interval, year, month, symbol)
            agg_path = month_part_path(cfg.agg_root, cfg.interval, year, month, symbol)
            out_path = month_part_path(cfg.out_root, cfg.interval, year, month, symbol)

            if not master_path.exists():
                results[(symbol, year, month)] = {"ok": False, "reason": "missing_master", "master": str(master_path)}
                continue

            m = pd.read_parquet(master_path)
            m = ensure_ts_utc(m, "ts")
            if "symbol" not in m.columns:
                m["symbol"] = symbol

            if agg_path.exists():
                a = pd.read_parquet(agg_path)
                a = ensure_ts_utc(a, "ts")
                if "symbol" not in a.columns:
                     a["symbol"] = symbol
                joined = join_master_with_aggtrades(m, a)
            else:
                # keep master rows; add empty agg columns
                joined = m.copy()
                for k in AGG_RENAME.values():
                    if k not in joined.columns:
                        joined[k] = pd.NA

            out_path.parent.mkdir(parents=True, exist_ok=True)
            joined.to_parquet(out_path, index=False)

            meta = qc_joined(joined)
            meta.update(
                {
                    "ok": True,
                    "out": str(out_path),
                    "master": str(master_path),
                    "agg": str(agg_path),
                    "min_ts": str(joined["ts"].min()),
                    "max_ts": str(joined["ts"].max()),
                }
            )
            results[(symbol, year, month)] = meta

    return results
