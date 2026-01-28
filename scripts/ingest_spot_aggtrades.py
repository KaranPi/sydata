from __future__ import annotations  # no installation needed

import argparse  # no installation needed
import json  # no installation needed
from dataclasses import dataclass  # no installation needed
from pathlib import Path  # no installation needed
from typing import List  # no installation needed

import pandas as pd  # already in env — no new install
import yaml  # already in env — no new install

import sys

ROOT = Path(__file__).resolve().parents[1]  # no installation needed
SRC = ROOT / "src"  # no installation needed
sys.path.insert(0, str(SRC))  # no installation needed


from sydata.providers.binance_data_archive import BinanceDataArchiveClient  # project-local
from sydata.datasets.spot_aggtrades import SpotAggTradesIngestor  # project-local


def utc_run_id() -> str:
    return pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H%M%SZ")


def month_range(start: str, end: str) -> List[tuple[int, int]]:
    # start/end are ISO dates or datetimes; months inclusive of start, inclusive of end month
    s = pd.Timestamp(start).to_period("M")
    e = pd.Timestamp(end).to_period("M")
    out = []
    cur = s
    while cur <= e:
        out.append((cur.year, cur.month))
        cur = cur + 1
    return out


def load_basket(spec: dict, basket_name: str) -> list[str]:
    b = spec["baskets"][basket_name]["symbols"]
    if not isinstance(b, list) or not all(isinstance(x, str) for x in b):
        raise ValueError(f"Basket '{basket_name}' must be list[str]")
    return b


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", required=True)
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--basket", required=True)
    ap.add_argument("--start", required=True, help="e.g. 2024-01-01")
    ap.add_argument("--end", required=True, help="e.g. 2024-02-01")
    ap.add_argument("--run-id", default=None)
    args = ap.parse_args()

    data_root = Path(args.data_root)
    spec = yaml.safe_load(Path(args.manifest).read_text())
    symbols = load_basket(spec, args.basket)

    run_id = args.run_id or utc_run_id()
    months = month_range(args.start, args.end)

    client = BinanceDataArchiveClient()
    ing = SpotAggTradesIngestor(data_root=data_root, client=client)

    results = []
    for sym in symbols:
        for (y, m) in months:
            rec = {"symbol": sym, "year": y, "month": m, "status": "ok", "error": None}
            try:
                info = ing.ingest_month(sym, y, m)
                rec.update(info)
            except Exception as e:
                rec["status"] = "error"
                rec["error"] = repr(e)
            results.append(rec)

    report = {
        "run_id": run_id,
        "dataset": "spot_aggtrades",
        "basket": args.basket,
        "symbols": symbols,
        "start": args.start,
        "end": args.end,
        "results": results,
        "summary": {
            "ok": sum(1 for r in results if r["status"] == "ok"),
            "error": sum(1 for r in results if r["status"] == "error"),
            "rows_written": sum(int(r.get("rows", 0) or 0) for r in results if r["status"] == "ok"),
        },
    }

    runs_dir = data_root / "meta" / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    out = runs_dir / f"{run_id}_spot_aggtrades_report.json"
    out.write_text(json.dumps(report, indent=2))
    print(str(out))


if __name__ == "__main__":
    main()
