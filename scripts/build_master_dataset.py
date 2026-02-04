from __future__ import annotations  

import argparse  
import json  
import sys  
from dataclasses import dataclass  
from pathlib import Path  
from typing import Any, Dict, List, Optional, Tuple  

import numpy as np  
import pandas as pd  


# --- bootstrap imports from src/ so this script runs via subprocess ---
ROOT = Path(__file__).resolve().parents[1]  
SRC = ROOT / "src"  
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))  

# project-local (already in your repo)
import sydata.io.symbols as sym   (project-local)


# ----------------------------
# small utilities
# ----------------------------
def _expected_step_ms(interval: str) -> int:
    if interval.endswith("h"):
        return int(interval[:-1]) * 3600_000
    if interval.endswith("m"):
        return int(interval[:-1]) * 60_000
    if interval.endswith("d"):
        return int(interval[:-1]) * 86_400_000
    raise ValueError(f"Unsupported interval: {interval}")


def audit_time_key(
    df: pd.DataFrame,
    time_col: str,
    symbol_col: str,
    interval: str,
) -> Dict[str, Any]:
    if df.empty:
        return {
            "rows": 0,
            "key_is_unique": True,
            "monotonic": True,
            "step_ok": True,
            "min": None,
            "max": None,
            "bad_steps": 0,
        }

    step = _expected_step_ms(interval)
    t = df[time_col].astype("int64", copy=False)

    keys = pd.MultiIndex.from_arrays(
        [df[symbol_col].astype(str), t],
        names=[symbol_col, time_col],
    )
    key_is_unique = bool(keys.is_unique)

    bad_steps = 0
    monotonic = True
    step_ok = True

    gdf = df[[symbol_col, time_col]].sort_values([symbol_col, time_col])
    for _, g in gdf.groupby(symbol_col, sort=False):
        gt = g[time_col].astype("int64", copy=False).to_numpy()
        if len(gt) >= 2:
            diffs = np.diff(gt)
            if np.any(diffs < 0):
                monotonic = False
            bad = int(np.sum(diffs != step))
            bad_steps += bad
            if bad > 0:
                step_ok = False

    return {
        "rows": int(len(df)),
        "key_is_unique": key_is_unique,
        "monotonic": bool(monotonic),
        "step_ok": bool(step_ok),
        "min": int(t.min()),
        "max": int(t.max()),
        "bad_steps": int(bad_steps),
    }


def _read_parquets(parts: List[Path]) -> pd.DataFrame:
    if not parts:
        return pd.DataFrame()
    dfs = [pd.read_parquet(p) for p in parts]
    return pd.concat(dfs, ignore_index=True)


def _ensure_ts_from_open_time(df: pd.DataFrame, open_time_col: str = "open_time") -> pd.DataFrame:
    if "ts" not in df.columns and open_time_col in df.columns:
        df = df.copy()
        df["ts"] = pd.to_datetime(df[open_time_col], unit="ms", utc=True)
    else:
        if "ts" in df.columns:
            df = df.copy()
            df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df


def _filter_ts(df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    if df.empty:
        return df
    m = (df["ts"] >= start) & (df["ts"] < end)
    return df.loc[m].copy()


def _dedup_on_key(df: pd.DataFrame, key_cols: List[str]) -> Tuple[pd.DataFrame, int]:
    if df.empty:
        return df, 0
    before = len(df)
    df2 = df.sort_values(key_cols).drop_duplicates(key_cols, keep="last")
    return df2, int(before - len(df2))


def _candidate_roots(data_root: Path, dataset: str, venue: str, symbol: str, interval: str) -> List[Path]:
    """
    Tries multiple common layouts so the script works even if a dataset
    doesn't have venue partitioning (or if it's in raw/ vs norm/).
    """
    if dataset == "spot_klines_ohlcv":
        return [
        # ---- preferred normalized layout (if you have it)
        data_root / "norm" / "ohlcv" / f"venue={venue}" / f"symbol={symbol}" / f"interval={interval}",
        data_root / "norm" / "ohlcv" / f"symbol={symbol}" / f"interval={interval}",

        # ---- raw fallback (what you clearly have today)
        data_root / "raw" / "binance" / "klines" / f"symbol={symbol}" / f"interval={interval}",
        data_root / "raw" / "binance" / "klines" / f"venue={venue}" / f"symbol={symbol}" / f"interval={interval}",
        ]
    if dataset == "bvol_resampled":
        return [
            data_root / "norm" / "bvol_resampled" / f"venue={venue}" / f"symbol={symbol}" / f"interval={interval}",
            data_root / "norm" / "bvol_resampled" / f"symbol={symbol}" / f"interval={interval}",
            data_root / "norm" / "bvol_resampled" / f"symbol={symbol}",
        ]
    if dataset == "um_funding_rate":
        return [
            data_root / "raw" / "binance" / "um_funding_rate" / f"symbol={symbol}",
            data_root / "raw" / "binance" / "um_funding_rate" / f"venue={venue}" / f"symbol={symbol}",
        ]
    if dataset == "um_mark_price_klines":
        return [
            data_root / "raw" / "binance" / "um_mark_price_klines" / f"symbol={symbol}" / f"interval={interval}",
            data_root / "raw" / "binance" / "um_mark_price_klines" / f"symbol={symbol}",
        ]
    if dataset == "um_index_price_klines":
        return [
            data_root / "raw" / "binance" / "um_index_price_klines" / f"symbol={symbol}" / f"interval={interval}",
            data_root / "raw" / "binance" / "um_index_price_klines" / f"symbol={symbol}",
        ]
    raise ValueError(f"Unknown dataset: {dataset}")


def _load_symbol_dataset(
    data_root: Path,
    dataset: str,
    venue: str,
    symbol: str,
    interval: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    roots = _candidate_roots(data_root, dataset, venue, symbol, interval)
    parts: List[Path] = []
    used_root: Optional[Path] = None
    for r in roots:
        if r.exists():
            cand = sorted(r.rglob("part-*.parquet"))
            if cand:
                parts = cand
                used_root = r
                break

    df = _read_parquets(parts)
    meta: Dict[str, Any] = {
        "dataset": dataset,
        "symbol": symbol,
        "roots_tried": [str(x) for x in roots],
        "root_used": str(used_root) if used_root else None,
        "parts": int(len(parts)),
        "rows_read": int(len(df)),
    }

    if df.empty:
        return df, meta

    # Ensure ts exists
    df = _ensure_ts_from_open_time(df, open_time_col="open_time")

    # Some sources may have symbol column missing or named differently; enforce.
    if "symbol" not in df.columns:
        df = df.copy()
        df["symbol"] = symbol

    df = _filter_ts(df, start, end)
    meta["rows_in_window"] = int(len(df))

    # Dedup on expected key
    if dataset in ("spot_klines_ohlcv", "um_mark_price_klines", "um_index_price_klines"):
        key = ["symbol", "ts"]  # hour bars; ts derived from open_time
    else:
        key = ["symbol", "ts"]
    df, dropped = _dedup_on_key(df, key)
    meta["dedup_dropped"] = dropped
    meta["rows_post_dedup"] = int(len(df))

    return df, meta


def _merge_exact(spine: pd.DataFrame, other: pd.DataFrame, cols_map: Dict[str, str]) -> pd.DataFrame:
    if other.empty:
        for _, out_col in cols_map.items():
            if out_col not in spine.columns:
                spine[out_col] = np.nan
        return spine

    use_cols = ["ts", "symbol"] + list(cols_map.keys())
    o = other[use_cols].copy()

    # rename payload columns into master namespace
    o = o.rename(columns=cols_map)

    return spine.merge(o, on=["ts", "symbol"], how="left")


def _merge_asof_per_symbol(spine: pd.DataFrame, other: pd.DataFrame, cols_map: Dict[str, str]) -> pd.DataFrame:
    # Asof merge: last known value at or before ts
    if other.empty:
        for _, out_col in cols_map.items():
            if out_col not in spine.columns:
                spine[out_col] = np.nan
        return spine

    o = other[["ts", "symbol"] + list(cols_map.keys())].copy().rename(columns=cols_map)

    spine = spine.sort_values(["symbol", "ts"])
    o = o.sort_values(["symbol", "ts"])

    out_chunks: List[pd.DataFrame] = []
    for sym_, s in spine.groupby("symbol", sort=False):
        og = o[o["symbol"] == sym_]
        if og.empty:
            # add NaNs for this symbol
            for c in cols_map.values():
                if c not in s.columns:
                    s[c] = np.nan
            out_chunks.append(s)
            continue

        merged = pd.merge_asof(
            s.sort_values("ts"),
            og.sort_values("ts")[["ts"] + list(cols_map.values())],
            on="ts",
            direction="backward",
        )
        out_chunks.append(merged)

    out = pd.concat(out_chunks, ignore_index=True).sort_values(["symbol", "ts"])
    return out


def _coverage(spine: pd.DataFrame, col: str) -> Dict[str, float]:
    if spine.empty or col not in spine.columns:
        return {"nonnull_frac": 0.0, "null_frac": 1.0}
    null_frac = float(spine[col].isna().mean())
    return {"nonnull_frac": float(1.0 - null_frac), "null_frac": null_frac}


# ----------------------------
# main build
# ----------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", required=True, type=Path)
    ap.add_argument("--spec", required=True, type=Path)
    ap.add_argument("--manifest", required=False, type=Path, default=None)
    ap.add_argument("--venue", required=False, type=str, default="binance")
    args = ap.parse_args()

    data_root: Path = args.data_root
    spec_path: Path = args.spec
    venue: str = args.venue
    manifest_path: Path = args.manifest if args.manifest else (data_root / "meta" / "symbols.yml")

    spec = json.loads(spec_path.read_text())

    interval = str(spec["interval"])
    start = pd.to_datetime(spec["start"], utc=True)
    end = pd.to_datetime(spec["end"], utc=True)
    basket_name = str(spec["basket_name"])
    datasets_enabled = list(spec["datasets_enabled"])

    out_long_csv = Path(spec["output"]["master_long_csv"])
    out_wide_csv = Path(spec["output"]["master_wide_csv"])
    out_long_parq = Path(spec["output"]["master_long_parq"])
    out_wide_parq = Path(spec["output"]["master_wide_parq"])
    out_report = Path(spec["output"]["run_report"])

    for p in [out_long_csv, out_wide_csv, out_long_parq, out_wide_parq, out_report]:
        p.parent.mkdir(parents=True, exist_ok=True)

    manifest = sym.load_manifest(manifest_path)
    basket = None
    for k in ("baskets", "basket", "universes", "universe", "groups"):
        if k in manifest and isinstance(manifest[k], dict) and basket_name in manifest[k]:
            basket = manifest[k][basket_name]
            break
    if basket is None:
        raise ValueError(f"Could not find basket '{basket_name}' inside manifest at {manifest_path}")

    symbols = [str(x) for x in basket]
    if not symbols:
        raise ValueError(f"Basket '{basket_name}' is empty")

    run_report: Dict[str, Any] = {
        "ok": False,
        "spec_path": str(spec_path),
        "data_root": str(data_root),
        "manifest_path": str(manifest_path),
        "venue": venue,
        "basket_name": basket_name,
        "interval": interval,
        "start": str(start),
        "end": str(end),
        "datasets_enabled": datasets_enabled,
        "symbols_n": int(len(symbols)),
        "symbols": symbols,
        "datasets": {},
    }

    # --- 1) spine = spot OHLCV close ---
    if "spot_klines_ohlcv" not in datasets_enabled:
        raise ValueError("This builder expects 'spot_klines_ohlcv' enabled as the spine dataset.")

    spine_chunks: List[pd.DataFrame] = []
    meta_spine: List[Dict[str, Any]] = []

    for s in symbols:
        df, meta = _load_symbol_dataset(data_root, "spot_klines_ohlcv", venue, s, interval, start, end)
        meta_spine.append(meta)
        if df.empty:
            continue

        # keep only what we need for master build
        # assumes norm/ohlcv already has close; ts derived from open_time
        keep = ["ts", "symbol", "close"]
        if "open_time" in df.columns:
            keep.insert(0, "open_time")

        d = df[keep].copy()
        d = d.rename(columns={"close": "spot_close"})
        spine_chunks.append(d)

    spine = pd.concat(spine_chunks, ignore_index=True) if spine_chunks else pd.DataFrame(columns=["ts", "symbol", "spot_close"])

    # enforce hourly grid check on spine if open_time exists
    if "open_time" in spine.columns:
        run_report["spine_audit"] = audit_time_key(spine, time_col="open_time", symbol_col="symbol", interval=interval)
    else:
        # audit on ts converted to ms
        tmp = spine.copy()
        tmp["open_time"] = (tmp["ts"].astype("int64") // 1_000_000).astype("int64")
        run_report["spine_audit"] = audit_time_key(tmp, time_col="open_time", symbol_col="symbol", interval=interval)

    run_report["datasets"]["spot_klines_ohlcv"] = {
        "per_symbol": meta_spine,
        "rows_spine": int(len(spine)),
    }

    # --- 2) merge other enabled datasets onto spine ---
    master = spine.copy()
    if "ts" not in master.columns:
        master["ts"] = pd.to_datetime(master["open_time"], unit="ms", utc=True)

    # exact-join datasets (hourly bars)
    exact_join_map: Dict[str, Dict[str, str]] = {
        "um_mark_price_klines": {"close": "mark_close"},
        "um_index_price_klines": {"close": "index_close"},
        "bvol_resampled": {"bvol": "bvol"},
    }

    # asof datasets (sparse relative to 1h spine)
    asof_join_map: Dict[str, Dict[str, str]] = {
        "um_funding_rate": {"funding_rate": "funding_rate"},
    }

    for ds, cols_map in exact_join_map.items():
        if ds not in datasets_enabled:
            continue

        chunks: List[pd.DataFrame] = []
        metas: List[Dict[str, Any]] = []
        for s in symbols:
            df, meta = _load_symbol_dataset(data_root, ds, venue, s, interval, start, end)
            metas.append(meta)
            if df.empty:
                continue

            if ds == "bvol_resampled" and ("bvol" not in df.columns) and ("index_value" in df.columns):
                df = df.rename(columns={"index_value": "bvol"})

            # normalize to ts,symbol + needed cols
            base_cols = ["ts", "symbol"] + list(cols_map.keys())
            d = df[base_cols].copy()

            # (optional) coerce numeric
            for c in cols_map.keys():
                d[c] = pd.to_numeric(d[c], errors="coerce")

            chunks.append(d)

        other = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=["ts", "symbol"] + list(cols_map.keys()))
        other, dropped = _dedup_on_key(other, ["ts", "symbol"])
        run_report["datasets"][ds] = {
            "per_symbol": metas,
            "rows_concat": int(len(other)),
            "dedup_dropped": int(dropped),
        }

        master = _merge_exact(master, other, cols_map)
        # coverage stats
        for _, out_col in cols_map.items():
            run_report["datasets"][ds][f"coverage_{out_col}"] = _coverage(master, out_col)

    for ds, cols_map in asof_join_map.items():
        if ds not in datasets_enabled:
            continue

        chunks = []
        metas = []
        for s in symbols:
            df, meta = _load_symbol_dataset(data_root, ds, venue, s, interval, start, end)
            metas.append(meta)
            if df.empty:
                continue

            base_cols = ["ts", "symbol"] + list(cols_map.keys())
            d = df[base_cols].copy()

            for c in cols_map.keys():
                d[c] = pd.to_numeric(d[c], errors="coerce")

            chunks.append(d)

        other = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=["ts", "symbol"] + list(cols_map.keys()))
        other, dropped = _dedup_on_key(other, ["ts", "symbol"])
        run_report["datasets"][ds] = {
            "per_symbol": metas,
            "rows_concat": int(len(other)),
            "dedup_dropped": int(dropped),
        }

        master = _merge_asof_per_symbol(master, other, cols_map)
        for _, out_col in cols_map.items():
            run_report["datasets"][ds][f"coverage_{out_col}"] = _coverage(master, out_col)

    # --- 3) finalize long ---
    master = master.sort_values(["symbol", "ts"]).reset_index(drop=True)

    # enforce ts first for downstream CSV/Mongo friendliness
    ordered_cols = ["ts", "symbol"]
    rest = [c for c in master.columns if c not in ordered_cols]
    master_long = master[ordered_cols + rest]

    # --- 4) wide ---
    idx_cols = ["ts"]
    value_cols = [c for c in master_long.columns if c not in ("ts", "symbol")]
    wide = (
        master_long.set_index(["ts", "symbol"])[value_cols]
        .sort_index()
        .unstack("symbol")
    )
    # flatten columns: <symbol>__<field>
    wide.columns = [f"{sym}__{field}" for field, sym in wide.columns.to_list()]
    master_wide = wide.reset_index()

    run_report["rows"] = {
        "master_long": int(len(master_long)),
        "master_wide": int(len(master_wide)),
        "symbols": int(len(symbols)),
    }
    run_report["ok"] = True

    # --- 5) write artifacts ---
    master_long.to_csv(out_long_csv, index=False)
    master_wide.to_csv(out_wide_csv, index=False)
    master_long.to_parquet(out_long_parq, index=False)
    master_wide.to_parquet(out_wide_parq, index=False)

    out_report.write_text(json.dumps(run_report, indent=2, default=str))
    print(json.dumps({"ok": True, "report": str(out_report), "rows": run_report["rows"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
