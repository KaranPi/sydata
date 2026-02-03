from __future__ import annotations  

import argparse  
import json  
from pathlib import Path  

from sydata.datasets.master_join_aggtrades import (  # project-local
    MasterAggJoinCfg,
    run_monthly_join,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Join spot aggtrades-resampled onto monthly master parquet slices.")
    p.add_argument("--data-root", required=True)
    p.add_argument("--manifest", required=True)
    p.add_argument("--basket", default=None)
    p.add_argument("--interval", required=True)
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True, dest="end_excl")

    p.add_argument("--master-root", default=None, help="Default: <data-root>/norm/master")
    p.add_argument("--agg-root", default=None, help="Default: <data-root>/norm/spot_aggtrades_resampled")
    p.add_argument("--out-root", default=None, help="Default: <data-root>/norm/master_plus_aggtrades")

    return p.parse_args()


def main() -> None:
    args = parse_args()

    data_root = Path(args.data_root)
    master_root = Path(args.master_root) if args.master_root else data_root / "norm" / "master"
    agg_root = Path(args.agg_root) if args.agg_root else data_root / "norm" / "spot_aggtrades_resampled"
    out_root = Path(args.out_root) if args.out_root else data_root / "norm" / "master_plus_aggtrades"

    cfg = MasterAggJoinCfg(
        data_root=data_root,
        master_root=master_root,
        agg_root=agg_root,
        out_root=out_root,
        manifest_path=Path(args.manifest),
        basket=args.basket,
        interval=args.interval,
        start=args.start,
        end_excl=args.end_excl,
    )

    res = run_monthly_join(cfg)

    ok = sum(1 for v in res.values() if v.get("ok"))
    fail = sum(1 for v in res.values() if not v.get("ok"))

    print(
        json.dumps(
            {
                "ok": True,
                "joined": ok,
                "failed": fail,
                "out_root": str(out_root),
                "sample": dict(list(res.items())[:5]),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
