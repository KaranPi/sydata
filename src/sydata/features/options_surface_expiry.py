from __future__ import annotations  # no installation needed

from dataclasses import dataclass  # no installation needed
from pathlib import Path  # no installation needed
from typing import Iterable  # no installation needed

import numpy as np  # already in env — no new install
import pandas as pd  # already in env — no new install


SECONDS_IN_YEAR = 365.25 * 24 * 3600.0


def _to_utc_dt(x: pd.Series) -> pd.Series:
    # Accept either ms-int open_time OR datetime already
    if np.issubdtype(x.dtype, np.integer):
        return pd.to_datetime(x, unit="ms", utc=True)
    return pd.to_datetime(x, utc=True)


def _canonical_spot_symbol_from_underlying(underlying: str) -> str:
    # BTCUSDT -> BTC-USDT
    u = underlying.strip().upper()
    if u.endswith("USDT") and len(u) > 4:
        return f"{u[:-4]}-USDT"
    # fallback: already canonical-ish
    return u.replace("/", "-").replace("_", "-")


def load_spot_close_hourly(
    data_root: Path,
    spot_symbol: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    interval: str = "1h",
) -> pd.Series:
    """
    Returns Series indexed by ts_hour (UTC) with spot_close for the requested window.
    Tries norm/ohlcv first, then raw klines fallback.
    """
    # ---- 1) Try norm/ohlcv (preferred)
    norm_root = data_root / "norm" / "ohlcv"
    parts: list[Path] = []
    if norm_root.exists():
        # expected-ish: norm/ohlcv/**/symbol=BTC-USDT/interval=1h/year=YYYY/month=MM/part-*.parquet
        yms = pd.period_range(start=start, end=end - pd.Timedelta(seconds=1), freq="M")
        for per in yms:
            y = f"{per.year:04d}"
            m = f"{per.month:02d}"
            parts += list(
                norm_root.rglob(
                    f"symbol={spot_symbol}/interval={interval}/year={y}/month={m}/part-*.parquet"
                )
            )

    if parts:
        df = pd.concat([pd.read_parquet(p) for p in sorted(set(parts))], ignore_index=True)
    else:
        # ---- 2) Raw klines fallback
        raw_root = data_root / "raw" / "binance" / "klines" / f"symbol={spot_symbol}" / f"interval={interval}"
        raw_parts = sorted(raw_root.glob("part-*.parquet")) if raw_root.exists() else []
        if not raw_parts:
            # One-file style (e.g., part-<start>-<end>.parquet)
            raw_parts = sorted(raw_root.glob("*.parquet")) if raw_root.exists() else []
        if not raw_parts:
            return pd.Series(dtype="float64")

        df = pd.concat([pd.read_parquet(p) for p in raw_parts], ignore_index=True)

    # normalize to ts_hour
    if "ts" in df.columns:
        ts = pd.to_datetime(df["ts"], utc=True)
    elif "open_time" in df.columns:
        ts = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    else:
        raise ValueError("Spot dataframe missing ts/open_time")

    df = df.assign(ts=ts)
    df = df[(df["ts"] >= start) & (df["ts"] < end)]
    if df.empty:
        return pd.Series(dtype="float64")

    # ensure hour buckets
    df["ts_hour"] = df["ts"].dt.floor("h")
    # prefer close column
    if "close" not in df.columns:
        raise ValueError("Spot dataframe missing 'close' column")

    out = df.groupby("ts_hour", as_index=True)["close"].last().astype("float64").sort_index()
    return out


def _quote_ok_row(row: pd.Series) -> bool:
    # Binance options mark sometimes has bidIV/askIV = -1 or tiny placeholders.
    bid = row.get("bidIV", np.nan)
    ask = row.get("askIV", np.nan)
    if pd.isna(bid) or pd.isna(ask):
        return False
    if bid <= 0 or ask <= 0:
        return False
    if bid == -1 or ask == -1:
        return False
    return True


def _pick_atm(group: pd.DataFrame, spot_close: float) -> tuple[float | None, float | None]:
    if group.empty or not np.isfinite(spot_close):
        return (None, None)
    idx = (group["strike_price"] - float(spot_close)).abs().idxmin()
    row = group.loc[idx]
    return (float(row["strike_price"]), float(row["markIV"]))


def _pick_25d(group: pd.DataFrame) -> tuple[float | None, float | None]:
    """
    Returns (call25_iv, put25_iv) based on delta closeness.
    call: delta ~ +0.25
    put:  delta ~ -0.25
    """
    call = group[group["side"].str.upper().eq("CALL")]
    put = group[group["side"].str.upper().eq("PUT")]

    call25 = None
    put25 = None

    if not call.empty and call["delta"].notna().any():
        idx = (call["delta"] - 0.25).abs().idxmin()
        call25 = float(call.loc[idx, "markIV"])

    if not put.empty and put["delta"].notna().any():
        idx = (put["delta"] + 0.25).abs().idxmin()
        put25 = float(put.loc[idx, "markIV"])

    return (call25, put25)


def build_surface_expiry_for_hour(
    df_hour: pd.DataFrame,
    ts_hour: pd.Timestamp,
    underlying: str,
    spot_close: float,
) -> pd.DataFrame:
    """
    Input df_hour: contracts for one underlying + one ts_hour, with at least:
      expiry_ts, side, strike_price, markIV, delta, bidIV, askIV
    Output: rows per expiry_ts with surface stats.
    """
    if df_hour.empty:
        return pd.DataFrame()

    d = df_hour.copy()

    # hygiene
    d["side"] = d["side"].astype(str)
    d["expiry_ts"] = pd.to_datetime(d["expiry_ts"], utc=True)
    d["markIV"] = pd.to_numeric(d["markIV"], errors="coerce")
    d["delta"] = pd.to_numeric(d["delta"], errors="coerce")
    d["strike_price"] = pd.to_numeric(d["strike_price"], errors="coerce")

    d = d.dropna(subset=["expiry_ts", "side", "strike_price", "markIV"])
    if d.empty:
        return pd.DataFrame()

    d["quote_ok"] = d.apply(_quote_ok_row, axis=1)

    rows = []
    for expiry_ts, g in d.groupby("expiry_ts", sort=True):
        # time-to-expiry
        T = (expiry_ts - ts_hour).total_seconds() / SECONDS_IN_YEAR
        if not np.isfinite(T) or T <= 0:
            continue

        atm_strike, atm_iv = _pick_atm(g, spot_close)
        call25_iv, put25_iv = _pick_25d(g)

        has_atm = float(atm_iv is not None and np.isfinite(atm_iv))
        has_25d = float(
            call25_iv is not None
            and put25_iv is not None
            and np.isfinite(call25_iv)
            and np.isfinite(put25_iv)
        )

        rr25 = (call25_iv - put25_iv) if has_25d else np.nan
        bf25 = (0.5 * (call25_iv + put25_iv) - atm_iv) if (has_25d and has_atm) else np.nan

        rows.append(
            {
                "ts_hour": ts_hour,
                "underlying": underlying,
                "expiry_ts": expiry_ts,
                "spot_close": float(spot_close) if np.isfinite(spot_close) else np.nan,
                "T_years": float(T),
                "atm_strike": atm_strike,
                "atm_iv": atm_iv,
                "call25_iv": call25_iv,
                "put25_iv": put25_iv,
                "rr25": rr25,
                "bf25": bf25,
                "n_contracts": float(len(g)),
                "n_calls": float((g["side"].str.upper() == "CALL").sum()),
                "n_puts": float((g["side"].str.upper() == "PUT").sum()),
                "quote_ok_frac": float(g["quote_ok"].mean()) if len(g) else np.nan,
                "has_atm": has_atm,
                "has_25d": has_25d,
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    # stable ordering
    out = out.sort_values(["ts_hour", "expiry_ts"]).reset_index(drop=True)
    return out


def _month_path_surface(
    data_root: Path, underlying: str, interval: str, year: int, month: int
) -> Path:
    return (
        data_root
        / "norm"
        / "options_surface_expiry"
        / "venue=binance"
        / f"underlying={underlying}"
        / f"interval={interval}"
        / f"year={year:04d}"
        / f"month={month:02d}"
        / f"part-{year:04d}-{month:02d}.parquet"
    )


def _month_path_contracts(
    data_root: Path, underlying: str, interval: str, year: int, month: int
) -> Path:
    return (
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


def build_surface_expiry_range(
    data_root: Path,
    underlyings: Iterable[str],
    start: pd.Timestamp,
    end: pd.Timestamp,
    interval: str = "1h",
    overwrite: bool = False,
) -> dict:
    """
    Builds monthly parquet outputs for each underlying.
    """
    start = pd.to_datetime(start, utc=True)
    end = pd.to_datetime(end, utc=True)

    months = pd.period_range(start=start, end=end - pd.Timedelta(seconds=1), freq="M")

    report = {
        "ok": True,
        "interval": interval,
        "start": str(start),
        "end": str(end),
        "underlyings": list(underlyings),
        "months": int(len(months)),
        "rows_written": 0,
        "paths": [],
        "missing_inputs": [],
    }

    for underlying in underlyings:
        spot_symbol = _canonical_spot_symbol_from_underlying(underlying)

        for per in months:
            y, m = int(per.year), int(per.month)
            in_path = _month_path_contracts(data_root, underlying, interval, y, m)
            out_path = _month_path_surface(data_root, underlying, interval, y, m)

            if out_path.exists() and not overwrite:
                report["paths"].append(str(out_path))
                continue

            if not in_path.exists():
                report["missing_inputs"].append(str(in_path))
                continue

            dfc = pd.read_parquet(in_path)
            if dfc.empty:
                continue

            if "ts_hour" not in dfc.columns:
                # last resort: derive from ts_fetch if needed
                if "ts_fetch" in dfc.columns:
                    dfc["ts_hour"] = pd.to_datetime(dfc["ts_fetch"], utc=True).dt.floor("h")
                else:
                    raise ValueError("contracts missing ts_hour/ts_fetch")

            dfc["ts_hour"] = pd.to_datetime(dfc["ts_hour"], utc=True)

            # window filter
            dfc = dfc[(dfc["ts_hour"] >= start) & (dfc["ts_hour"] < end)]
            if dfc.empty:
                continue

            # load spot close series for this month window (small enough)
            month_start = max(start, pd.Timestamp(f"{y:04d}-{m:02d}-01", tz="UTC"))
            month_end = min(
                end,
                (pd.Timestamp(f"{y:04d}-{m:02d}-01", tz="UTC") + pd.offsets.MonthBegin(1)),
            )
            spot_close = load_spot_close_hourly(
                data_root=data_root,
                spot_symbol=spot_symbol,
                start=month_start,
                end=month_end,
                interval=interval,
            )

            if spot_close.empty:
                # cannot compute ATM-based features without spot
                continue

            rows = []
            for ts_hour, g in dfc.groupby("ts_hour", sort=True):
                sc = spot_close.get(ts_hour, np.nan)
                if not np.isfinite(sc):
                    continue
                out = build_surface_expiry_for_hour(
                    df_hour=g,
                    ts_hour=ts_hour,
                    underlying=underlying,
                    spot_close=float(sc),
                )
                if not out.empty:
                    rows.append(out)

            if not rows:
                continue

            out_df = pd.concat(rows, ignore_index=True)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_df.to_parquet(out_path, index=False)

            report["rows_written"] += int(len(out_df))
            report["paths"].append(str(out_path))

    return report
