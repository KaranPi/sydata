from __future__ import annotations  # no installation needed

from dataclasses import dataclass  # no installation needed
from pathlib import Path  # no installation needed
from typing import Iterable  # no installation needed

import numpy as np  # already in env — no new install
import pandas as pd  # already in env — no new install


# ------------------------
# Helpers
# ------------------------
def underlying_to_spot_symbol(underlying: str) -> str:
    # BTCUSDT -> BTC-USDT (matches your spot canonical)
    if underlying.endswith("USDT"):
        base = underlying[:-4]
        return f"{base}-USDT"
    # fallback: insert hyphen before last 4 chars
    return f"{underlying[:-4]}-{underlying[-4:]}"


def month_iter(start: pd.Timestamp, end: pd.Timestamp) -> Iterable[tuple[int, int]]:
    cur = pd.Timestamp(year=start.year, month=start.month, day=1, tz="UTC")
    end_m = pd.Timestamp(year=end.year, month=end.month, day=1, tz="UTC")
    while cur <= end_m:
        yield int(cur.year), int(cur.month)
        cur = (cur + pd.offsets.MonthBegin(1)).tz_convert("UTC")


def _clean_iv_cols(df: pd.DataFrame) -> pd.DataFrame:
    # keep raw columns; produce cleaned columns + flags
    for c in ["bidIV", "askIV", "markIV"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    def bad_iv(x: pd.Series) -> pd.Series:
        return (
            x.isna()
            | (x <= 0)
            | (x < 1e-4)         # near-zero artifact
            | (x > 5.0)          # 500% cap (wide)
            | (x == -1.0)
        )

    if "bidIV" in df.columns and "askIV" in df.columns:
        bid_bad = bad_iv(df["bidIV"])
        ask_bad = bad_iv(df["askIV"])
        spread_bad = (~bid_bad & ~ask_bad) & (df["askIV"] < df["bidIV"])

        df["bidIV_clean"] = df["bidIV"].mask(bid_bad | spread_bad)
        df["askIV_clean"] = df["askIV"].mask(ask_bad | spread_bad)
        df["quote_ok"] = (~df["bidIV_clean"].isna()) & (~df["askIV_clean"].isna())
        df["iv_mid"] = np.where(df["quote_ok"], (df["bidIV_clean"] + df["askIV_clean"]) / 2.0, np.nan)
        df["iv_spread"] = np.where(df["quote_ok"], (df["askIV_clean"] - df["bidIV_clean"]), np.nan)
    else:
        df["quote_ok"] = False
        df["iv_mid"] = np.nan
        df["iv_spread"] = np.nan

    if "markIV" in df.columns:
        df["markIV_clean"] = df["markIV"].mask(bad_iv(df["markIV"]))
    else:
        df["markIV_clean"] = np.nan

    return df


def load_options_mark_contracts_month(
    data_root: Path,
    underlying: str,
    interval: str,
    year: int,
    month: int,
) -> pd.DataFrame:
    p = (
        data_root
        / "norm"
        / "options_mark_contracts"
        / "venue=binance"
        / f"underlying={underlying}"
        / f"interval={interval}"
        / f"year={year:04d}"
        / f"month={month:02d}"
        / f"part-{year:04d}-{month:02d}.parquet"
    )
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_parquet(p)
    # ensure dtypes
    df["ts_hour"] = pd.to_datetime(df["ts_hour"], utc=True)
    df["expiry_ts"] = pd.to_datetime(df["expiry_ts"], utc=True)
    df["strike_price"] = pd.to_numeric(df["strike_price"], errors="coerce")
    df["delta"] = pd.to_numeric(df["delta"], errors="coerce")
    return _clean_iv_cols(df)


def load_spot_klines_range(
    data_root: Path,
    spot_symbol: str,
    interval: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    # raw spot klines: raw/binance/klines/symbol=BTC-USDT/interval=1h/part-*.parquet
    base = (
        data_root
        / "raw"
        / "binance"
        / "klines"
        / f"symbol={spot_symbol}"
        / f"interval={interval}"
    )
    files = sorted(base.glob("part-*.parquet"))
    if not files:
        return pd.DataFrame(columns=["ts_hour", "spot_close"])

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df["ts_hour"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df = df.sort_values("ts_hour")
    df = df[(df["ts_hour"] >= start) & (df["ts_hour"] < end)]
    out = df[["ts_hour", "close"]].rename(columns={"close": "spot_close"})
    out["spot_close"] = pd.to_numeric(out["spot_close"], errors="coerce")
    return out.dropna(subset=["spot_close"])


def build_surface_expiry_slice(
    contracts: pd.DataFrame,
    spot: pd.DataFrame,
) -> pd.DataFrame:
    if contracts.empty or spot.empty:
        return pd.DataFrame()

    df = contracts.merge(spot, on="ts_hour", how="inner")
    df = df.dropna(subset=["spot_close", "strike_price", "expiry_ts", "markIV_clean"])

    # time-to-expiry
    T = (df["expiry_ts"] - df["ts_hour"]).dt.total_seconds() / (365.25 * 24 * 3600)
    df["T_years"] = T
    df = df[df["T_years"] > 0]

    # moneyness
    df["moneyness"] = np.log(df["strike_price"] / df["spot_close"])

    def agg_one(g: pd.DataFrame) -> pd.Series:
        # Diagnostics
        n = int(len(g))
        calls = g[g["side"] == "CALL"]
        puts = g[g["side"] == "PUT"]
        n_calls = int(len(calls))
        n_puts = int(len(puts))
        quote_ok_frac = float(g["quote_ok"].mean()) if "quote_ok" in g.columns else 0.0

        # ATM by minimum abs moneyness (use markIV_clean)
        atm_idx = (g["moneyness"].abs()).idxmin()
        atm_row = g.loc[atm_idx]
        atm_iv = float(atm_row["markIV_clean"]) if pd.notna(atm_row["markIV_clean"]) else np.nan
        atm_strike = float(atm_row["strike_price"]) if pd.notna(atm_row["strike_price"]) else np.nan

        # 25d proxies by delta-nearest (use markIV_clean)
        call25_iv = np.nan
        put25_iv = np.nan

        c = calls.dropna(subset=["delta", "markIV_clean"])
        if not c.empty:
            c = c[(c["delta"] > 0) & (c["delta"] < 1)]
            if not c.empty:
                call25_iv = float(c.loc[(c["delta"] - 0.25).abs().idxmin(), "markIV_clean"])

        p = puts.dropna(subset=["delta", "markIV_clean"])
        if not p.empty:
            p = p[(p["delta"] < 0) & (p["delta"] > -1)]
            if not p.empty:
                put25_iv = float(p.loc[(p["delta"] - (-0.25)).abs().idxmin(), "markIV_clean"])

        rr25 = call25_iv - put25_iv if (np.isfinite(call25_iv) and np.isfinite(put25_iv)) else np.nan
        bf25 = (0.5 * (call25_iv + put25_iv) - atm_iv) if (np.isfinite(call25_iv) and np.isfinite(put25_iv) and np.isfinite(atm_iv)) else np.nan

        return pd.Series(
            {
                "spot_close": float(g["spot_close"].iloc[0]),
                "T_years": float(g["T_years"].iloc[0]),
                "atm_strike": atm_strike,
                "atm_iv": atm_iv,
                "call25_iv": call25_iv,
                "put25_iv": put25_iv,
                "rr25": rr25,
                "bf25": bf25,
                "n_contracts": n,
                "n_calls": n_calls,
                "n_puts": n_puts,
                "quote_ok_frac": quote_ok_frac,
                "has_atm": int(np.isfinite(atm_iv)),
                "has_25d": int(np.isfinite(call25_iv) and np.isfinite(put25_iv)),
            }
        )

    out = (
        df.groupby(["ts_hour", "underlying", "expiry_ts"], as_index=False, sort=True)
          .apply(lambda g: agg_one(g), include_groups=False)
          .reset_index()
    )

    # groupby/apply produces an 'index' column sometimes; clean it
    if "index" in out.columns:
        out = out.drop(columns=["index"])

    return out.sort_values(["ts_hour", "expiry_ts"])
