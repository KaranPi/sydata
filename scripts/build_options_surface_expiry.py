from __future__ import annotations  

import argparse  
import json  
from pathlib import Path  
import sys
from datetime import datetime, timezone  

import pandas as pd  

ROOT = Path(__file__).resolve().parents[1]  
SRC = ROOT / "src"  
sys.path.insert(0, str(SRC))  

from sydata.features.options_surface_expiry import build_surface_expiry_range  # project-local


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", required=True)
    ap.add_argument("--underlyings", required=True, help="Comma-separated, e.g. BTCUSDT,ETHUSDT")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD or ISO8601")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD or ISO8601")
    ap.add_argument("--interval", default="1h")
    ap.add_argument("--overwrite", action="store_true")
    args = ap.parse_args()

    data_root = Path(args.data_root)
    underlyings = [u.strip().upper() for u in args.underlyings.split(",") if u.strip()]
    start = pd.to_datetime(args.start, utc=True)
    end = pd.to_datetime(args.end, utc=True)

    report = build_surface_expiry_range(
        data_root=data_root,
        underlyings=underlyings,
        start=start,
        end=end,
        interval=args.interval,
        overwrite=bool(args.overwrite),
    )

    runs_dir = data_root / "meta" / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    tag = pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H%M%SZ")
    out = runs_dir / f"{tag}_options_surface_expiry_report.json"
    out.write_text(json.dumps(report, indent=2))

    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
