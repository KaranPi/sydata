from __future__ import annotations

from pathlib import Path  # no installation needed
from typing import Any  # no installation needed

import re  # no installation needed
import yaml  # already in env — no new install


_CANON_RE = re.compile(r"[A-Za-z0-9]+-[A-Za-z0-9]+")


def load_manifest(path: Path) -> dict[str, Any]:
    spec = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(spec, dict):
        raise ValueError(f"Manifest must be a mapping/dict: {path}")
    return spec


def load_basket(spec: dict[str, Any], basket_name: str) -> list[str]:
    """
    Expected simplest manifest shape:
      baskets:
        core_major: ["BTC-USDT", ...]
    Also supports nested shapes by scanning common keys.
    """
    # 1) direct
    baskets = spec.get("baskets")
    if isinstance(baskets, dict) and basket_name in baskets:
        basket = baskets[basket_name]
    else:
        # 2) common alternates
        basket = None
        for k in ("universe", "universes", "meta"):
            node = spec.get(k)
            if isinstance(node, dict):
                b = node.get("baskets")
                if isinstance(b, dict) and basket_name in b:
                    basket = b[basket_name]
                    break
        if basket is None:
            raise KeyError(f"Basket '{basket_name}' not found in manifest")

    if not isinstance(basket, list):
        raise ValueError(f"Basket '{basket_name}' must be a list of canonical symbols")

    # validate canonical symbols like BTC-USDT
    bad = [s for s in basket if (not isinstance(s, str)) or (_CANON_RE.fullmatch(s) is None)]
    if bad:
        raise ValueError(
            f"Invalid canonical symbols in basket '{basket_name}': {bad} "
            "(expected like 'BTC-USDT')"
        )
    return basket
