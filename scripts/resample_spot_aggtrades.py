"""Resample Binance spot aggTrades into bar-aligned aggregates.

This script is intentionally thin; core logic lives in:
`sydata.datasets.spot_aggtrades_resampled`.
"""

from __future__ import annotations  # no installation needed

import argparse  # no installation needed
import json  # no installation needed
from pathlib import Path  # no installation needed

from sydata.datasets.spot_aggtrades_resampled import AggResampleCfg, run_resample  # no installation needed


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", type=Path, required=True)
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--basket", type=str, required=True)
    ap.add_argument("--interval", type=str, required=True)
    ap.add_argument("--start", type=str, required=True)
    ap.add_argument("--end", dest="end_excl", type=str, required=True)
    ap.add_argument("--symbols", nargs="*", default=None)
    ap.add_argument("--raw-root", type=Path, default=None)
    ap.add_argument("--norm-root", type=Path, default=None)
    args = ap.parse_args()

    cfg = AggResampleCfg(
        data_root=args.data_root,
        manifest_path=args.manifest,
        basket=args.basket,
        interval=args.interval,
        start=args.start,
        end_excl=args.end_excl,
        symbols_override=args.symbols,
        raw_root=args.raw_root,
        norm_root=args.norm_root,
    )

    out = run_resample(cfg)
    ok = sum(1 for v in out.values() if v.get("ok"))

    print(json.dumps({"ok": ok, "n": len(out)}, indent=2))


if __name__ == "__main__":
    main()
