from __future__ import annotations

import argparse
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]  # no installation needed
SRC = ROOT / "src"  # no installation needed
sys.path.insert(0, str(SRC))  # no installation needed


from sydata.providers.binance_spot import BinanceSpotClient


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
    ap.add_argument("--symbol", required=True)     # BTC-USDT
    ap.add_argument("--interval", required=True)   # 1m
    ap.add_argument("--start", required=True)      # e.g. 2026-01-01T00:00:00Z
    ap.add_argument("--end", required=True)        # e.g. 2026-01-02T00:00:00Z
    args = ap.parse_args()

    data_root = Path(args.data_root)
    start_ms = parse_utc_ms(args.start)
    end_ms = parse_utc_ms(args.end)

    client = BinanceSpotClient()
    df = client.fetch_klines(symbol=args.symbol, interval=args.interval, start_ms=start_ms, end_ms=end_ms)

    out_dir = data_root / "raw" / "binance" / "klines" / f"symbol={args.symbol}" / f"interval={args.interval}"
    out_dir.mkdir(parents=True, exist_ok=True)

    if df.empty:
        return

    first_open = int(df["open_time"].iloc[0])
    last_open = int(df["open_time"].iloc[-1])
    out_path = out_dir / f"part-{first_open}-{last_open}.parquet"

    df.to_parquet(out_path, index=False)
    print(str(out_path))


if __name__ == "__main__":
    main()
