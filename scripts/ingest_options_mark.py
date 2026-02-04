from __future__ import annotations  

import argparse  
import json  
import sys  
import time  
from datetime import datetime, timezone  
from pathlib import Path  

import pandas as pd  

# Make src importable without editable installs
ROOT = Path(__file__).resolve().parents[1]  # repo root (…/sydata)
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from sydata.providers.binance_options import (  # project-local
    BinanceOptionsClient,
    mark_to_frame,
)


def utc_now_ms() -> int:
    return int(time.time() * 1000)


def asof_tag(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H%M%SZ")  # windows-safe


def latest_exchange_info_dir(data_root: Path) -> Path:
    meta_root = data_root / "meta" / "binance" / "options_exchange_info"
    dirs = sorted(meta_root.glob("asof=*"))
    if not dirs:
        raise FileNotFoundError(f"No exchange info dirs found under {meta_root}")
    return max(dirs, key=lambda p: p.stat().st_mtime)


def parse_underlyings(s: str | None) -> set[str] | None:
    if not s:
        return None
    toks = [t.strip() for t in s.split(",") if t.strip()]
    return set(toks) if toks else None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", required=True)
    ap.add_argument("--base-url", default="https://eapi.binance.com")
    ap.add_argument("--underlyings", default=None, help="comma-separated, e.g. BTCUSDT,ETHUSDT (default: all)")
    args = ap.parse_args()

    data_root = Path(args.data_root)
    asof_ms = utc_now_ms()
    tag = asof_tag(asof_ms)

    # Load latest option universe snapshot
    uni_dir = latest_exchange_info_dir(data_root)
    uni = pd.read_parquet(uni_dir / "option_symbols.parquet")

    under_filter = parse_underlyings(args.underlyings)
    if under_filter is not None:
        uni = uni[uni["underlying"].isin(sorted(under_filter))].copy()

    # Keep only tradable symbols from the snapshot
    if "status" in uni.columns:
        uni = uni[uni["status"] == "TRADING"].copy()

    # Fetch one mark snapshot (endpoint returns all by default)
    client = BinanceOptionsClient(base_url=args.base_url)
    mark_payload = client.fetch_mark()

    dfm = mark_to_frame(mark_payload, asof_ms=asof_ms)

    # Normalize symbol column naming (Binance uses 'symbol')
    if "symbol" not in dfm.columns:
        raise RuntimeError("Mark payload missing 'symbol' field; cannot join to universe")

    # Join to universe metadata (expiry/strike/side/underlying)
    meta_cols = [
        "symbol", "underlying", "expiry_ms", "expiry_ts", "side", "strike_price",
        "quote_asset", "tick_size", "step_size",
    ]
    meta_cols = [c for c in meta_cols if c in uni.columns]
    uni_meta = uni[meta_cols].drop_duplicates(subset=["symbol"])
    allowed = set(uni_meta["symbol"].astype(str).tolist())

    # filter the mark snapshot down to the selected universe before joining
    dfm = dfm[dfm["symbol"].astype(str).isin(allowed)].copy()

    df = dfm.merge(uni_meta, on="symbol", how="inner", validate="many_to_one")

    # Attach identifiers
    df["venue"] = "binance"
    df["dataset"] = "options_mark"

    # Coverage / diagnostics
    joined_frac = float(df["underlying"].notna().mean()) if len(df) else 0.0
    rows_total = int(len(df))

    # Partition path by fetch hour (stable join key for 1h spot)
    ts_fetch = pd.to_datetime(asof_ms, unit="ms", utc=True)
    date_str = ts_fetch.strftime("%Y-%m-%d")
    hour_str = ts_fetch.strftime("%H")

    raw_root = data_root / "raw" / "binance" / "options_mark"
    wrote = []

    # Write one file per underlying to keep files small and join-friendly
    if "underlying" in df.columns:
        for under, g in df.groupby("underlying"):
            out_dir = raw_root / f"underlying={under}" / f"date={date_str}" / f"hour={hour_str}"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"part-{asof_ms}.parquet"
            g.to_parquet(out_path, index=False)
            wrote.append(str(out_path))

    else:
        out_dir = raw_root / f"date={date_str}" / f"hour={hour_str}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"part-{asof_ms}.parquet"
        df.to_parquet(out_path, index=False)
        wrote.append(str(out_path))

    report = {
        "asof_ms": asof_ms,
        "asof_tag": tag,
        "base_url": args.base_url,
        "universe_dir": str(uni_dir),
        "universe_symbols": int(uni["symbol"].nunique()) if "symbol" in uni.columns else None,
        "rows_total": rows_total,
        "joined_frac": joined_frac,
        "rows_by_underlying": (df["underlying"].fillna("UNKNOWN").value_counts().to_dict()
                               if "underlying" in df.columns else {}),
        "null_fracs": {
            k: float(df[k].isna().mean())
            for k in ["markIV", "bidIV", "askIV", "delta", "gamma", "vega", "theta", "markPrice"]
            if k in df.columns
        },
        "paths_written": wrote,
    }

    runs_dir = data_root / "meta" / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    (runs_dir / f"{tag}_options_mark_report.json").write_text(json.dumps(report, indent=2))

    print(json.dumps({"ok": True, "rows_total": rows_total, "joined_frac": joined_frac}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
