from __future__ import annotations  # no installation needed

import argparse  # no installation needed
import json  # no installation needed
import sys  # no installation needed
import time  # no installation needed
from datetime import datetime, timezone  # no installation needed
from pathlib import Path  # no installation needed

import pandas as pd  # already in env — no new install

# Make src importable without editable installs
ROOT = Path(__file__).resolve().parents[1]  # repo root (…/sydata)
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from sydata.providers.binance_options import (  # project-local
    BinanceOptionsClient,
    exchange_info_to_frames,
)


def utc_now_ms() -> int:
    return int(time.time() * 1000)


def asof_tag(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H%M%SZ")  # windows-safe


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", required=True)
    ap.add_argument("--base-url", default="https://eapi.binance.com")
    args = ap.parse_args()

    data_root = Path(args.data_root)
    meta_dir = data_root / "meta" / "binance" / "options_exchange_info"

    asof_ms = utc_now_ms()
    tag = asof_tag(asof_ms)
    out_dir = meta_dir / f"asof={tag}"
    out_dir.mkdir(parents=True, exist_ok=True)

    client = BinanceOptionsClient(base_url=args.base_url)

    payload = client.fetch_exchange_info()
    (out_dir / "exchangeInfo.json").write_text(json.dumps(payload, indent=2))

    cdf, adf, sdf = exchange_info_to_frames(payload, asof_ms=asof_ms)

    # Persist parquet (schema-stable artifacts)
    if not cdf.empty:
        cdf.to_parquet(out_dir / "option_contracts.parquet", index=False)
    if not adf.empty:
        adf.to_parquet(out_dir / "option_assets.parquet", index=False)
    if not sdf.empty:
        sdf.to_parquet(out_dir / "option_symbols.parquet", index=False)

    report = {
        "asof_ms": asof_ms,
        "asof_tag": tag,
        "base_url": args.base_url,
        "counts": {
            "option_contracts": int(len(cdf)),
            "option_assets": int(len(adf)),
            "option_symbols": int(len(sdf)),
        },
        "paths": {
            "dir": str(out_dir),
            "exchangeInfo_json": str(out_dir / "exchangeInfo.json"),
            "option_contracts_parquet": str(out_dir / "option_contracts.parquet"),
            "option_assets_parquet": str(out_dir / "option_assets.parquet"),
            "option_symbols_parquet": str(out_dir / "option_symbols.parquet"),
        },
    }

    runs_dir = data_root / "meta" / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    (runs_dir / f"{tag}_options_exchange_info_report.json").write_text(json.dumps(report, indent=2))

    print(json.dumps({"ok": True, "asof_tag": tag, "counts": report["counts"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
