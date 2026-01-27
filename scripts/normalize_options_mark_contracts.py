from __future__ import annotations  # no installation needed

import argparse  # no installation needed
import json  # no installation needed
import sys  # no installation needed
from pathlib import Path  # no installation needed

# Make src importable without editable installs
ROOT = Path(__file__).resolve().parents[1]  # no installation needed
SRC = ROOT / "src"  # no installation needed
sys.path.insert(0, str(SRC))  # no installation needed

from sydata.normalize.options_mark_contracts import normalize_range  # project-local


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", required=True)
    ap.add_argument("--underlyings", required=True, help="comma-separated, e.g. BTCUSDT,ETHUSDT")
    ap.add_argument("--start", required=True, help="e.g. 2026-01-01T00:00:00Z")
    ap.add_argument("--end", required=True, help="e.g. 2026-02-01T00:00:00Z")
    ap.add_argument("--interval", default="1h")
    ap.add_argument("--overwrite", action="store_true")
    args = ap.parse_args()

    data_root = Path(args.data_root)
    underlyings = [u.strip() for u in args.underlyings.split(",") if u.strip()]

    reps = normalize_range(
        data_root=data_root,
        underlyings=underlyings,
        start=args.start,
        end=args.end,
        interval=args.interval,
        overwrite=args.overwrite,
    )

    summary = {
        "months": len(reps),
        "rows_read": sum(r.rows_read for r in reps),
        "rows_written": sum(r.rows_written for r in reps),
        "dedup_dropped": sum(r.dedup_dropped for r in reps),
        "out_paths": [r.out_path for r in reps if r.rows_written > 0],
    }
    print(json.dumps({"ok": True, "summary": summary}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
