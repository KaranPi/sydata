from __future__ import annotations

from pathlib import Path  # no installation needed
from typing import Any  # no installation needed

import re  # no installation needed
import yaml  # already in env — no new install


_CANON_RE = re.compile(r"^[A-Z0-9]+(?:-[A-Z0-9]+)?$")  # allows BTC-USDT and BTCBVOLUSDT


def load_manifest(path: Path) -> dict[str, Any]:
    spec = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(spec, dict):
        raise ValueError("Manifest must parse to a dict")
    return spec


def load_basket(spec: dict[str, Any], basket_name: str) -> list[str]:
    baskets = spec.get("baskets", {})
    if not isinstance(baskets, dict) or basket_name not in baskets:
        raise ValueError(f"Basket '{basket_name}' not found in manifest")

    node = baskets[basket_name]

    # Support both:
    # 1) baskets: { name: {symbols: [...] } }
    # 2) baskets: { name: [ ... ] }
    if isinstance(node, dict):
        symbols = node.get("symbols", None)
    elif isinstance(node, list):
        symbols = node
    else:
        symbols = None

    if not isinstance(symbols, list) or not symbols:
        raise ValueError(f"Basket '{basket_name}' invalid/empty")

    out: list[str] = []
    for s in symbols:
        if not isinstance(s, str):
            raise ValueError(f"Basket '{basket_name}' must be list[str]")
        sym = s.strip().upper()
        if not _CANON_RE.match(sym):
            raise ValueError(f"Basket '{basket_name}' must be a list of canonical symbols")
        out.append(sym)

    return out
