from __future__ import annotations  

from typing import Final  

# ---------------------------------------------------------------------
# Canonical schema for resampled spot aggTrades (15m buckets)
# This is the *only* place that defines the micro columns and mappings.
# ---------------------------------------------------------------------

AGG_PREFIX: Final[str] = "agg_"

# Columns that exist in the resampled aggTrades parquet *without* the agg_ prefix.
# NOTE: "ts" and optionally "symbol" are handled separately.
MICRO_RAW_COLS: Final[list[str]] = [
    # base quantities / counts
    "sum_qty",
    "trades",
    "cvd_qty",
    # ids / price aggregates
    "vwap",
    "last_trade_id",
    # microstructure decompositions
    "taker_buy_qty",
    "taker_sell_qty",
    "taker_buy_trades",
    "taker_sell_trades",
    "buy_notional",
    "sell_notional",
    "buy_vwap",
    "sell_vwap",
    "first_price",
    "last_price",
    "first_trade_id",
]

# Rename map: micro parquet columns -> master_long columns
AGG_RENAME: Final[dict[str, str]] = {
    "sum_qty": "agg_sum_qty",
    "trades": "agg_trades",
    "cvd_qty": "agg_cvd_qty",
    "vwap": "agg_vwap",
    "last_trade_id": "agg_last_trade_id",
    "taker_buy_qty": "agg_taker_buy_qty",
    "taker_sell_qty": "agg_taker_sell_qty",
    "taker_buy_trades": "agg_taker_buy_trades",
    "taker_sell_trades": "agg_taker_sell_trades",
    "buy_notional": "agg_buy_notional",
    "sell_notional": "agg_sell_notional",
    "buy_vwap": "agg_buy_vwap",
    "sell_vwap": "agg_sell_vwap",
    "first_price": "agg_first_price",
    "last_price": "agg_last_price",
    "first_trade_id": "agg_first_trade_id",
}

# Target columns expected on master_long after join
AGG_COLS: Final[list[str]] = [AGG_RENAME[c] for c in MICRO_RAW_COLS]


def micro_keep_cols(*, include_symbol: bool = True) -> list[str]:
    """
    Column list to request when reading resampled aggTrades parquet.
    Keep this small to make joins cheap.
    """
    cols = ["ts", *MICRO_RAW_COLS]
    if include_symbol:
        cols.append("symbol")
    return cols
