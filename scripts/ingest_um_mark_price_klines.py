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
from sydata.providers.um_mark_price_klines import UmMarkPriceKlinesArchive  # project-local


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
        raise ValueError(f"Basket '{basket_name}' has no symbols list")

    return [str(s) for s in symbols]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", required=True)
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--basket", required=True)
    ap.add_argument("--intervals", default="1h")
    ap.add_argument("--start", required=True)  # YYYY-MM-DD
    ap.add_argument("--end", required=True)    # YYYY-MM-DD (exclusive)
    ap.add_argument("--overwrite", action="store_true")
    args = ap.parse_args()

    data_root = Path(args.data_root)
    manifest = Path(args.manifest)

    symbols = load_basket(manifest, args.basket)
    intervals = [s.strip() for s in args.intervals.split(",") if s.strip()]

    start_d = datetime.fromisoformat(args.start).date()
    end_d = datetime.fromisoformat(args.end).date()

    client = BinanceDataArchiveClient()
    ing = UmMarkPriceKlinesArchive(data_root=data_root, client=client)
    report = ing.ingest_range(
        symbols=symbols,
        intervals=intervals,
        start_d=start_d,
        end_d=end_d,
        overwrite=bool(args.overwrite),
    )

    runs_dir = data_root / "meta" / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    out = runs_dir / f"{stamp}_um_mark_price_klines_report.json"
    out.write_text(json.dumps(report, indent=2))

    print(json.dumps(report["summary"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
