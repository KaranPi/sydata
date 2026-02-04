from __future__ import annotations

import argparse
from pathlib import Path
import sys
import json  

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]  
SRC = ROOT / "src"  
sys.path.insert(0, str(SRC))  


from sydata.providers.binance_spot import BinanceSpotClient
from sydata.io.symbols import load_manifest, load_basket  # project-local


def parse_utc_ms(ts: str) -> int:
    # Treat naive inputs as UTC; Binance interprets startTime/endTime in UTC. :contentReference[oaicite:5]{index=5}
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")
    else:
        t = t.tz_convert("UTC")
    return int(t.value // 1_000_000)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", required=True)  # C:\Users\quantbase\Desktop\marketdata

    sym_mode = ap.add_mutually_exclusive_group(required=True)
    sym_mode.add_argument("--symbol", help="BTC-USDT (canonical)")
    sym_mode.add_argument("--basket", help="basket name in symbols.yml (e.g. core_major)")

    ap.add_argument("--manifest", help=r"path to symbols.yml (required if --basket)")
    ap.add_argument("--interval", required=True)   # 1m/15m/1h/4h
    ap.add_argument("--start", required=True)      # e.g. 2026-01-01T00:00:00Z
    ap.add_argument("--end", required=True)        # e.g. 2026-01-02T00:00:00Z
    args = ap.parse_args()

    data_root = Path(args.data_root)
    start_ms = parse_utc_ms(args.start)
    end_ms = parse_utc_ms(args.end)

    client = BinanceSpotClient()
    symbols: list[str]
    if args.basket:
        if not args.manifest:
            ap.error("--manifest is required when using --basket")
        spec = load_manifest(Path(args.manifest))
        symbols = load_basket(spec, args.basket)
    else:
        symbols = [args.symbol]

    out_paths: list[str] = []

    for sym in symbols:
        df = client.fetch_klines(symbol=sym, interval=args.interval, start_ms=start_ms, end_ms=end_ms)

        out_dir = data_root / "raw" / "binance" / "klines" / f"symbol={sym}" / f"interval={args.interval}"
        out_dir.mkdir(parents=True, exist_ok=True)

        if df.empty:
            continue

        first_open = int(df["open_time"].iloc[0])
        last_open = int(df["open_time"].iloc[-1])
        out_path = out_dir / f"part-{first_open}-{last_open}.parquet"

        df.to_parquet(out_path, index=False)
        out_paths.append(str(out_path))

    print(json.dumps({"ok": len(out_paths), "out_paths": out_paths}, indent=2))


if __name__ == "__main__":
    main()
