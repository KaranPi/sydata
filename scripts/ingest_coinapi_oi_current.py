from __future__ import annotations

import sys  # no installation needed
from pathlib import Path  # no installation needed

import argparse  # no installation needed
import json  # no installation needed
from datetime import datetime, timezone  # no installation needed
from pathlib import Path  # no installation needed

import pandas as pd  # already in env — no new install
import yaml  # already in env — no new install

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC = REPO_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from sydata.providers.coinapi_client import CoinAPIClient  # project-local
from sydata.providers.coinapi_metrics import CoinAPIMetricsV1  # project-local


def utc_now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", type=str, required=True)
    ap.add_argument("--coinapi-map", type=str, required=True)
    ap.add_argument("--metric-id", type=str, default="DERIVATIVES_OPEN_INTEREST")
    args = ap.parse_args()

    data_root = Path(args.data_root)
    mp = yaml.safe_load(Path(args.coinapi_map).read_text(encoding="utf-8"))
    exchange_id = mp["exchange_id"]
    mapping = mp["map"]

    client = CoinAPIClient()
    mx = CoinAPIMetricsV1(client=client)

    rows = []
    for canonical, info in mapping.items():
        symbol_id = info["symbol_id"]
        if not symbol_id:
            continue
        cur = mx.current_symbol_metrics(symbol_id=symbol_id, exchange_id=exchange_id, metric_id=args.metric_id)
        for r in cur:
            r2 = dict(r)
            r2["canonical_symbol"] = canonical
            r2["exchange_id"] = exchange_id
            r2["symbol_id"] = symbol_id
            rows.append(r2)

    df = pd.DataFrame(rows)
    out_dir = data_root / "raw" / "coinapi" / "oi_current" / f"exchange_id={exchange_id}" / f"metric_id={args.metric_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"part-{utc_now_tag()}.parquet"
    df.to_parquet(out_path, index=False)

    rep = {
        "exchange_id": exchange_id,
        "metric_id": args.metric_id,
        "rows": int(df.shape[0]),
        "symbols": int(df["canonical_symbol"].nunique()) if not df.empty else 0,
        "out": str(out_path),
    }
    runs = data_root / "meta" / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    rep_path = runs / f"{utc_now_tag()}_coinapi_oi_current_report.json"
    rep_path.write_text(json.dumps(rep, indent=2), encoding="utf-8")
    print(json.dumps(rep, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
