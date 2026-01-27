from __future__ import annotations  # no installation needed

from pathlib import Path 
import sys
from dataclasses import dataclass  # no installation needed
from typing import Callable, Dict, Iterable, Optional  # no installation needed

ROOT = Path(__file__).resolve().parents[1]  # no installation needed
SRC = ROOT / "src"  # no installation needed
sys.path.insert(0, str(SRC))  # no installation needed


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    # Root relative to DATA_ROOT, e.g. "norm/ohlcv" or "raw/binance/um_funding_rate"
    root_rel: str

    # Partition keys encoded in folders, e.g. ("symbol", "interval") or ("underlying", "interval")
    folder_keys: tuple[str, ...]

    # Required columns in the parquet file (pre-rename)
    required_cols: tuple[str, ...]

    # Canonical time columns expected after standardization
    # - time_key_ms: int64 milliseconds (preferred join key at ingestion/master-build time)
    # - time_key_ts: datetime64[ns, UTC] convenience
    time_key_ms: str
    time_key_ts: str

    # Rename map applied after reading (kept minimal; master builder can do feature renames later)
    rename: dict[str, str]

    # Column whitelist after rename (None = keep all)
    keep_cols: Optional[tuple[str, ...]] = None


# Registry for “stable-history / resample-integrity” datasets.
# Keep this tight; add sparse/short-retention later (OI/liqs/L/S).
DATASETS: Dict[str, DatasetSpec] = {
    "ohlcv": DatasetSpec(
        name="ohlcv",
        root_rel="raw/binance/klines",
        folder_keys=("symbol", "interval"),
        required_cols=("open_time", "close", "volume"),
        time_key_ms="open_time",
        time_key_ts="ts",
        rename={},  # keep raw names; master builder will standardize output names
        keep_cols=None,
    ),
    "bvol_resampled": DatasetSpec(
        name="bvol_resampled",
        root_rel="norm/bvol_resampled",
        folder_keys=("symbol", "interval"),
        required_cols=("open_time", "bvol"),
        time_key_ms="open_time",
        time_key_ts="ts",
        rename={},
        keep_cols=None,
    ),
    "um_mark_price_klines": DatasetSpec(
        name="um_mark_price_klines",
        root_rel="raw/binance/um_mark_price_klines",
        folder_keys=("symbol", "interval"),
        required_cols=("open_time", "close"),
        time_key_ms="open_time",
        time_key_ts="ts",
        rename={"close": "mark_close"},
        keep_cols=None,
    ),
    "um_index_price_klines": DatasetSpec(
        name="um_index_price_klines",
        root_rel="raw/binance/um_index_price_klines",
        folder_keys=("symbol", "interval"),
        required_cols=("open_time", "close"),
        time_key_ms="open_time",
        time_key_ts="ts",
        rename={"close": "index_close"},
        keep_cols=None,
    ),
    "um_funding_rate": DatasetSpec(
        name="um_funding_rate",
        root_rel="raw/binance/um_funding_rate",
        folder_keys=("symbol",),
        required_cols=("funding_time", "funding_rate", "funding_interval_hours"),
        time_key_ms="funding_time",
        time_key_ts="ts",
        rename={},  # master builder aligns to hours
        keep_cols=None,
    ),
}
