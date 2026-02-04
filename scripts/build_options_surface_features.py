from __future__ import annotations  

import argparse  
import json  
import sys  
from datetime import datetime, timezone  
from pathlib import Path  

import pandas as pd  

ROOT = Path(__file__).resolve().parents[1]  
SRC = ROOT / "src"  
sys.path.insert(0, str(SRC))  

from sydata.features.options_surface import (  # project-local
    build_surface_expiry_slice,
    load_options_mark_contracts_month,
    load_spot_klines_range,
    month_iter,
    underlying_to_spot_symbol,
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", required=True)
    ap.add_argument("--underlyings", required=True, help="comma-separated, e.g. BTCUSDT,ETHUSDT")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--interval", default="1h")
    ap.add_argument("--overwrite", action="store_true")
    args = ap.parse_args()

    data_root = Path(args.data_root)
    underlyings = [u.strip() for u in args.underlyings.split(",") if u.strip()]
    start = pd.to_datetime(args.start, utc=True)
    end = pd.to_datetime(args.end, utc=True)
    interval = args.interval

    tag = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")

    total_rows = 0
    written_paths: list[str] = []

    for u in underlyings:
        spot_symbol = underlying_to_spot_symbol(u)
        spot = load_spot_klines_range(data_root, spot_symbol, interval, start, end)
        if spot.empty:
            continue

        for y, m in month_iter(start, end):
            contracts = load_options_mark_contracts_month(data_root, u, interval, y, m)
            if contracts.empty:
                continue
            # window
            contracts = contracts[(contracts["ts_hour"] >= start) & (contracts["ts_hour"] < end)]
            if contracts.empty:
                continue

            out = build_surface_expiry_slice(contracts, spot)
            if out.empty:
                continue

            out_dir = (
                data_root
                / "norm"
                / "options_surface_expiry"
                / "venue=binance"
                / f"underlying={u}"
                / f"interval={interval}"
                / f"year={y:04d}"
                / f"month={m:02d}"
            )
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"part-{y:04d}-{m:02d}.parquet"
            if out_path.exists() and (not args.overwrite):
                pass
            else:
                out.to_parquet(out_path, index=False)
            total_rows += int(len(out))
            written_paths.append(str(out_path))

    runs_dir = data_root / "meta" / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    report_path = runs_dir / f"{tag}_options_surface_expiry_report.json"
    report = {
        "ok": True,
        "asof_tag": tag,
        "interval": interval,
        "start": str(start),
        "end": str(end),
        "underlyings": underlyings,
        "rows_written": total_rows,
        "paths": written_paths,
    }
    report_path.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
