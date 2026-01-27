from __future__ import annotations

import sys  # no installation needed
from pathlib import Path  # no installation needed

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC = REPO_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import argparse  # no installation needed
from pathlib import Path  # no installation needed

import yaml  # already in env — no new install

from sydata.io.symbols import load_manifest, load_basket  # project-local
from sydata.providers.coinapi_client import CoinAPIClient  # project-local
from sydata.providers.coinapi_metadata import CoinAPIMetadata  # project-local


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", type=str, required=True)
    ap.add_argument("--manifest", type=str, required=True)
    ap.add_argument("--basket", type=str, required=True)
    ap.add_argument("--exchange-id", type=str, default="BINANCEFTS")  # Binance Futures (USDT-M) per CoinAPI
    args = ap.parse_args()

    data_root = Path(args.data_root)
    spec = load_manifest(Path(args.manifest))
    basket = load_basket(spec, args.basket)  # canonical list like BTC-USDT

    client = CoinAPIClient()
    md = CoinAPIMetadata(client=client)

    mapping = md.map_canonical_to_symbol_id(
        canonical_symbols=basket,
        exchange_id=args.exchange_id,
    )

    out_path = data_root / "meta" / "coinapi_symbol_map.yml"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "exchange_id": args.exchange_id,
        "basket": args.basket,
        "map": mapping,
    }
    out_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    print(f"Wrote {out_path} ({len(mapping)} mapped symbols)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
