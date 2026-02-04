from __future__ import annotations  

import argparse  
import json  
import sys  
from datetime import datetime, timezone  
from pathlib import Path  

import yaml  

# Make script runnable from anywhere (adds <repo>/src)
PROJECT_ROOT = Path(__file__).resolve().parents[1]  
SRC = PROJECT_ROOT / "src"  
if str(SRC) not in sys.path:  
    sys.path.insert(0, str(SRC))  

from sydata.providers.binance_data_archive import BinanceDataArchiveClient  # project-local
from sydata.providers.um_funding_rate import UmFundingRateArchive  # project-local


def load_basket(manifest_path: Path, basket_name: str) -> list[str]:
    spec = yaml.safe_load(manifest_path.read_text())
    node = spec["baskets"][basket_name]

    if isinstance(node, dict):
        symbols = node.get("symbols", None)
    elif isinstance(node, list):
        symbols = node
    else:
        symbols = None

    if not isinstance(symbols, list) or not symbols:
        raise ValueError(f"Basket '{basket_name}' invalid/empty")

    return [str(x) for x in symbols]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", required=True)
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--basket", required=True)
    ap.add_argument("--start", required=True, help="e.g. 2024-01-01 or 2024-01-01T00:00:00Z")
    ap.add_argument("--end", required=True)
    ap.add_argument("--overwrite", action="store_true")
    args = ap.parse_args()

    data_root = Path(args.data_root)
    manifest = Path(args.manifest)
    symbols = load_basket(manifest, args.basket)

    start = args.start if "T" in args.start else f"{args.start}T00:00:00Z"
    end = args.end if "T" in args.end else f"{args.end}T00:00:00Z"

    client = BinanceDataArchiveClient()
    arch = UmFundingRateArchive(data_root=data_root, client=client)

    report = arch.ingest_range(symbols=symbols, start=start, end=end, overwrite=bool(args.overwrite))

    runs = data_root / "meta" / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    out = runs / f"{ts}_um_funding_rate_report.json"
    out.write_text(json.dumps(report, indent=2))

    print(json.dumps(report["summary"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
