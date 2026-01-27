from __future__ import annotations  # no installation needed

from dataclasses import dataclass  # no installation needed
from pathlib import Path  # no installation needed
import sys
from typing import Iterable, Optional  # no installation needed

ROOT = Path(__file__).resolve().parents[1]  # no installation needed
SRC = ROOT / "src"  # no installation needed
sys.path.insert(0, str(SRC))  # no installation needed

import pandas as pd  # already in env — no new install


@dataclass(frozen=True)
class TimeRange:
    start: pd.Timestamp
    end: pd.Timestamp


def parse_utc(ts: str | pd.Timestamp) -> pd.Timestamp:
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")
    else:
        t = t.tz_convert("UTC")
    return t


def month_starts(tr: TimeRange) -> list[pd.Timestamp]:
    start = tr.start.floor("D").replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = tr.end.floor("D").replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    out = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur = (cur + pd.offsets.MonthBegin(1)).to_pydatetime()
        cur = pd.Timestamp(cur, tz="UTC")
    return out


def find_month_parts(root: Path) -> list[Path]:
    # Works for both raw and norm layouts as long as they use part-*.parquet somewhere below.
    return sorted(root.rglob("part-*.parquet"))


def read_parts(paths: Iterable[Path], columns: Optional[list[str]] = None) -> pd.DataFrame:
    dfs: list[pd.DataFrame] = []
    for p in paths:
        dfs.append(pd.read_parquet(p, columns=columns))
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def load_dataset_range(
    data_root: Path,
    root_rel: str,
    folder_parts: dict[str, str],
    tr: TimeRange,
    columns: Optional[list[str]] = None,
) -> pd.DataFrame:
    """
    Loads all parquet parts under:
      data_root / root_rel / "<k>=<v>/"...  filtered by time range AFTER read.

    This is intentionally dumb-but-correct for Phase 1.
    Phase 2 can add smarter partition pruning (year/month folders).
    """
    root = data_root / root_rel
    for k, v in folder_parts.items():
        root = root / f"{k}={v}"

    parts = find_month_parts(root)
    df = read_parts(parts, columns=columns)
    if df.empty:
        return df
    return df


def add_ts_from_open_time_ms(df: pd.DataFrame, open_time_col: str = "open_time") -> pd.DataFrame:
    out = df.copy()
    if open_time_col in out.columns and "ts" not in out.columns:
        out["ts"] = pd.to_datetime(out[open_time_col], unit="ms", utc=True)
    return out
