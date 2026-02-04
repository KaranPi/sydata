from __future__ import annotations  

from dataclasses import dataclass  
from pathlib import Path  
from typing import Iterable  

import pandas as pd  


@dataclass(frozen=True)
class NormalizeReport:
    underlying: str
    year: int
    month: int
    files_read: int
    rows_read: int
    rows_written: int
    dedup_dropped: int
    min_ts_hour: str | None
    max_ts_hour: str | None
    out_path: str


def _month_iter(start: pd.Timestamp, end: pd.Timestamp) -> Iterable[tuple[int, int]]:
    cur = pd.Timestamp(year=start.year, month=start.month, day=1, tz="UTC")
    end_m = pd.Timestamp(year=end.year, month=end.month, day=1, tz="UTC")
    while cur <= end_m:
        yield int(cur.year), int(cur.month)
        cur = (cur + pd.offsets.MonthBegin(1)).tz_convert("UTC")


def _read_files(files: list[Path]) -> pd.DataFrame:
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def normalize_month(
    data_root: Path,
    underlying: str,
    year: int,
    month: int,
    start: pd.Timestamp,
    end: pd.Timestamp,
    interval: str = "1h",
    overwrite: bool = False,
) -> NormalizeReport:
    raw_root = data_root / "raw" / "binance" / "options_mark" / f"underlying={underlying}"
    if not raw_root.exists():
        raise FileNotFoundError(f"Raw options_mark not found: {raw_root}")

    # Collect files by partition folders date=YYYY-MM-DD/hour=HH/part-*.parquet
    # Cheap filter: only walk date folders for the month
    month_prefix = f"{year:04d}-{month:02d}-"
    files: list[Path] = []
    for date_dir in raw_root.glob("date=*"):
        if not date_dir.name.startswith("date=" + month_prefix):
            continue
        for hour_dir in date_dir.glob("hour=*"):
            files.extend(sorted(hour_dir.glob("part-*.parquet")))

    df = _read_files(files)

    if df.empty:
        out_path = (
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
        return NormalizeReport(
            underlying=underlying,
            year=year,
            month=month,
            files_read=len(files),
            rows_read=0,
            rows_written=0,
            dedup_dropped=0,
            min_ts_hour=None,
            max_ts_hour=None,
            out_path=str(out_path),
        )

    # Ensure timestamps are proper (should already be)
    if "ts_fetch" in df.columns:
        df["ts_fetch"] = pd.to_datetime(df["ts_fetch"], utc=True)
    if "ts_hour" in df.columns:
        df["ts_hour"] = pd.to_datetime(df["ts_hour"], utc=True)

    # Window filter
    df = df[(df["ts_hour"] >= start) & (df["ts_hour"] < end)].copy()

    rows_read = int(len(df))
    if rows_read == 0:
        out_path = (
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
        return NormalizeReport(
            underlying=underlying,
            year=year,
            month=month,
            files_read=len(files),
            rows_read=0,
            rows_written=0,
            dedup_dropped=0,
            min_ts_hour=None,
            max_ts_hour=None,
            out_path=str(out_path),
        )

    # Dedupe rule: keep last snapshot within hour per contract
    df = df.sort_values(["ts_hour", "symbol", "ts_fetch"])
    before = int(len(df))
    df = df.drop_duplicates(subset=["ts_hour", "symbol"], keep="last")
    after = int(len(df))
    dedup_dropped = before - after

    # Output partition
    out_dir = (
        data_root
        / "norm"
        / "options_mark_contracts"
        / "venue=binance"
        / f"underlying={underlying}"
        / f"interval={interval}"
        / f"year={year:04d}"
        / f"month={month:02d}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"part-{year:04d}-{month:02d}.parquet"

    if out_path.exists() and not overwrite:
        # no write, still report
        pass
    else:
        df.to_parquet(out_path, index=False)

    min_ts = str(df["ts_hour"].min()) if "ts_hour" in df.columns else None
    max_ts = str(df["ts_hour"].max()) if "ts_hour" in df.columns else None

    return NormalizeReport(
        underlying=underlying,
        year=year,
        month=month,
        files_read=len(files),
        rows_read=rows_read,
        rows_written=int(len(df)),
        dedup_dropped=dedup_dropped,
        min_ts_hour=min_ts,
        max_ts_hour=max_ts,
        out_path=str(out_path),
    )


def normalize_range(
    data_root: Path,
    underlyings: list[str],
    start: str,
    end: str,
    interval: str = "1h",
    overwrite: bool = False,
) -> list[NormalizeReport]:
    start_ts = pd.to_datetime(start, utc=True)
    end_ts = pd.to_datetime(end, utc=True)

    reports: list[NormalizeReport] = []
    for u in underlyings:
        for y, m in _month_iter(start_ts, end_ts):
            rep = normalize_month(
                data_root=data_root,
                underlying=u,
                year=y,
                month=m,
                start=start_ts,
                end=end_ts,
                interval=interval,
                overwrite=overwrite,
            )
            reports.append(rep)
    return reports
