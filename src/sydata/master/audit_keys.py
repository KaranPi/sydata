from __future__ import annotations  # no installation needed

from dataclasses import dataclass  # no installation needed
from typing import Any, Optional  # no installation needed
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # no installation needed
SRC = ROOT / "src"  # no installation needed
sys.path.insert(0, str(SRC))  # no installation needed

import numpy as np  # already in env — no new install
import pandas as pd  # already in env — no new install


def expected_step_ms(interval: str) -> int:
    # Extend as needed.
    if interval.endswith("h"):
        return int(interval[:-1]) * 3600_000
    if interval.endswith("m"):
        return int(interval[:-1]) * 60_000
    if interval.endswith("d"):
        return int(interval[:-1]) * 86_400_000
    raise ValueError(f"Unsupported interval: {interval}")


def audit_time_key(
    df: pd.DataFrame,
    time_col: str = "open_time",
    symbol_col: Optional[str] = "symbol",
    interval: Optional[str] = None,
) -> dict[str, Any]:
    if df.empty:
        return {
            "rows": 0,
            "key_is_unique": True,
            "monotonic": True,
            "step_ok": True,
            "min": None,
            "max": None,
            "bad_steps": 0,
        }

    t = df[time_col].astype("int64", copy=False)

    if symbol_col and symbol_col in df.columns:
        keys = pd.MultiIndex.from_arrays([df[symbol_col].astype(str), t], names=[symbol_col, time_col])
        key_is_unique = keys.is_unique
        # monotonic/step checks per symbol
        bad_steps = 0
        monotonic = True
        step_ok = True
        step = expected_step_ms(interval) if interval else None

        for _, g in df[[symbol_col, time_col]].sort_values([symbol_col, time_col]).groupby(symbol_col, sort=False):
            gt = g[time_col].astype("int64", copy=False).to_numpy()
            if len(gt) >= 2:
                diffs = np.diff(gt)
                if np.any(diffs < 0):
                    monotonic = False
                if step is not None:
                    bad = np.sum(diffs != step)
                    bad_steps += int(bad)
                    if bad > 0:
                        step_ok = False
        return {
            "rows": int(len(df)),
            "key_is_unique": bool(key_is_unique),
            "monotonic": bool(monotonic),
            "step_ok": bool(step_ok) if interval else None,
            "min": int(t.min()),
            "max": int(t.max()),
            "bad_steps": int(bad_steps),
        }

    # No symbol column: treat as single series
    t_sorted = np.sort(t.to_numpy())
    diffs = np.diff(t_sorted) if len(t_sorted) >= 2 else np.array([], dtype="int64")
    monotonic = bool(np.all(diffs >= 0))
    if interval:
        step = expected_step_ms(interval)
        bad_steps = int(np.sum(diffs != step))
        step_ok = bad_steps == 0
    else:
        bad_steps = 0
        step_ok = None

    return {
        "rows": int(len(df)),
        "key_is_unique": bool(pd.Index(t_sorted).is_unique),
        "monotonic": monotonic,
        "step_ok": step_ok,
        "min": int(t.min()),
        "max": int(t.max()),
        "bad_steps": bad_steps,
    }


def audit_join_coverage(
    spine: pd.DataFrame,
    other: pd.DataFrame,
    on: list[str],
) -> dict[str, float]:
    if spine.empty:
        return {"other_missing_frac": 1.0, "both_present_frac": 0.0}

    spine_keys = spine[on].drop_duplicates()
    if other.empty:
        return {"other_missing_frac": 1.0, "both_present_frac": 0.0}

    other_keys = other[on].drop_duplicates()
    merged = spine_keys.merge(other_keys.assign(_present=1), on=on, how="left")
    present = merged["_present"].fillna(0).astype(int)
    return {
        "other_missing_frac": float((present == 0).mean()),
        "both_present_frac": float((present == 1).mean()),
    }
