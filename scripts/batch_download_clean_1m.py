#!/usr/bin/env python3
from __future__ import annotations

import argparse
import calendar
import concurrent.futures
import json
import logging
import os
import re
import random
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd
import numpy as np
from thetadata import ThetaClient

# The SDK logs authentication responses at INFO; never emit these.
logging.getLogger("thetadata").setLevel(logging.CRITICAL)


NY_TZ = "America/New_York"
DEFAULT_INTERVAL = "1m"
DEFAULT_VENUE = "utp_cta"
DEFAULT_ASSET_TYPE = "stock"
ASSET_TYPES = ("stock", "index")
DEFAULT_START_DATE = date(1990, 1, 1)
DEFAULT_SYMBOLS = [
    "JPM",
    "BAC",
    "AMD",
    "NVDA",
    "AVGO",
    "QCOM",
    "SPY",
    "QQQ",
    "TQQQ",
    "V",
    "MA",
    "TSLA",
    "TSM",
    "INTC",
    "AAPL",
    "GOOGL",
    "AMZN",
    "MSFT",
    "ORCL",
    "ARM",
    "GOOG",
    "PLTR",
    "XOM",
    "CVX",
    "GS",
    "MS",
    "MCD",
    "YUM",
    "WMT",
    "TGT",
    "COST",
    "HD",
    "LOW",
]


def parse_yyyymmdd(value: str) -> date:
    return datetime.strptime(value.replace("-", ""), "%Y%m%d").date()


def yyyymmdd(value: date) -> str:
    return value.strftime("%Y%m%d")


def last_weekday(value: date) -> date:
    while value.weekday() >= 5:
        value -= timedelta(days=1)
    return value


def dedupe_symbols(symbols: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for symbol in symbols:
        normalized = symbol.strip().upper()
        if not re.fullmatch(r"[A-Z][A-Z0-9._-]{0,19}", normalized):
            raise ValueError("Invalid symbol")
        if normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def month_chunks(start_date: date, end_date: date) -> Iterable[tuple[date, date]]:
    cursor = start_date
    while cursor <= end_date:
        month_end = date(cursor.year, cursor.month, calendar.monthrange(cursor.year, cursor.month)[1])
        chunk_end = min(month_end, end_date)
        yield cursor, chunk_end
        cursor = chunk_end + timedelta(days=1)


def ensure_timestamp_column(df: pd.DataFrame) -> pd.DataFrame:
    if "timestamp" in df.columns:
        return df
    for candidate in ("time", "datetime", "date", "last_trade", "created"):
        if candidate in df.columns:
            return df.rename(columns={candidate: "timestamp"})
    raise ValueError(f"Could not find a timestamp column in columns: {list(df.columns)}")


def clean_bars(raw_bars: pd.DataFrame) -> pd.DataFrame:
    required = ["open", "high", "low", "close"]
    missing = [column for column in required if column not in raw_bars.columns]
    if missing:
        raise ValueError(f"Missing required OHLC columns: {missing}")
    result = raw_bars.dropna(subset=required).copy()
    finite = np.isfinite(result[required].to_numpy(dtype=float)).all(axis=1)
    positive = (result[required] > 0).all(axis=1)
    result = result.loc[finite & positive].reset_index(drop=True)
    if (result["high"] < result[["open", "close", "low"]].max(axis=1)).any() or (result["low"] > result[["open", "close"]].min(axis=1)).any():
        raise ValueError("Invalid OHLC bounds")
    if "volume" in result and ((result["volume"] < 0).any() or not np.isfinite(result["volume"]).all()):
        raise ValueError("Invalid volume")
    return result


def normalize_to_fixed_edt(clean: pd.DataFrame) -> pd.DataFrame:
    result = clean.copy()
    wall_clock = pd.to_datetime(result["timestamp"]).dt.tz_localize(None)
    result["timestamp"] = wall_clock.dt.tz_localize("Etc/GMT+4")
    return result


def write_frame(df: pd.DataFrame, base_path: Path, write_csv: bool, write_parquet: bool) -> dict[str, str]:
    outputs: dict[str, str] = {}
    if write_csv:
        csv_path = base_path.with_suffix(".csv")
        temporary = csv_path.with_suffix(".csv.tmp")
        df.to_csv(temporary, index=False)
        os.replace(temporary, csv_path)
        outputs["csv"] = str(csv_path)
    if write_parquet:
        parquet_path = base_path.with_suffix(".parquet")
        temporary = parquet_path.with_suffix(".parquet.tmp")
        df.to_parquet(temporary, index=False)
        os.replace(temporary, parquet_path)
        outputs["parquet"] = str(parquet_path)
    return outputs


def output_exists(base_path: Path, write_csv: bool, write_parquet: bool) -> bool:
    expected: list[Path] = []
    if write_csv:
        expected.append(base_path.with_suffix(".csv"))
    if write_parquet:
        expected.append(base_path.with_suffix(".parquet"))
    return bool(expected) and all(path.exists() and path.stat().st_size > 0 for path in expected)


def retry_delay(attempt: int, base_seconds: float, max_seconds: float) -> float:
    delay = min(max_seconds, base_seconds * (2 ** max(attempt - 1, 0)))
    return delay + random.uniform(0, min(3.0, delay * 0.2))


def is_transient_error(exc: Exception) -> bool:
    if is_session_error(exc):
        return True
    message = str(exc)
    transient_markers = [
        "502 Bad Gateway",
        "503 Service Unavailable",
        "504 Gateway Timeout",
        "timed out",
        "timeout",
        "temporarily unavailable",
        "Connection",
        "Deadline Exceeded",
        "UNAVAILABLE",
        "RESOURCE_EXHAUSTED",
    ]
    return any(marker.lower() in message.lower() for marker in transient_markers)


def is_empty_data_error(exc: Exception) -> bool:
    message = str(exc)
    markers = ["No data found"]
    return any(marker in message for marker in markers)


def is_permission_error(exc: Exception) -> bool:
    message = str(exc)
    markers = ["PROFESSIONAL", "PERMISSION_DENIED", "requiring a standard subscription"]
    return any(marker.lower() in message.lower() for marker in markers)


def is_session_error(exc: Exception) -> bool:
    message = str(exc)
    markers = ["Invalid session ID", "StatusCode.UNAUTHENTICATED"]
    return any(marker.lower() in message.lower() for marker in markers)


def with_retries(label: str, fn, retries: int, base_delay: float, max_delay: float, on_retry=None):
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as exc:
            if is_empty_data_error(exc) or is_permission_error(exc):
                raise
            if attempt >= retries or not is_transient_error(exc):
                raise
            delay = retry_delay(attempt, base_delay, max_delay)
            print(f"{label} transient error on attempt {attempt}/{retries}: {type(exc).__name__}. Sleeping {delay:.1f}s", flush=True)
            time.sleep(delay)
            if on_retry is not None:
                on_retry(exc)


def make_safe_client(**kwargs) -> ThetaClient:
    client = ThetaClient(**kwargs)
    # SDK 1.0.12 does not set RPC deadlines. Keep retries bounded.
    for method in ("GetStockHistoryOhlc", "GetIndexHistoryOhlc"):
        call = getattr(client.stub, method, None)
        if call is not None:
            setattr(client.stub, method, lambda request, call=call: call(request, timeout=180))
    return client


def history_ohlc(
    client: ThetaClient,
    asset_type: str,
    symbol: str,
    interval: str,
    start_date: date,
    end_date: date,
    venue: str,
):
    if asset_type == "index":
        return client.index_history_ohlc(
            symbol=symbol,
            interval=interval,
            start_date=start_date,
            end_date=end_date,
        )
    return client.stock_history_ohlc(
        symbol=symbol,
        interval=interval,
        start_date=start_date,
        end_date=end_date,
        venue=venue,
    )


def download_symbol(args: dict, existing_client: ThetaClient | None = None) -> dict:
    symbol = args["symbol"]
    asset_type = args["asset_type"]
    start_date = args["start_date"]
    end_date = args["end_date"]
    interval = args["interval"]
    venue = args["venue"]
    output_dir = args["output_dir"]
    write_csv = args["write_csv"]
    write_parquet = args["write_parquet"]
    write_raw = args["write_raw"]
    write_clean_local = args["write_clean_local"]
    force = args["force"]
    creds_file = args["creds_file"]
    retries = args["retries"]
    retry_base_delay = args["retry_base_delay"]
    retry_max_delay = args["retry_max_delay"]
    startup_delay = args["startup_delay"]

    suffix = f"{yyyymmdd(start_date)}_{yyyymmdd(end_date)}"
    symbol_dir = output_dir / symbol
    symbol_dir.mkdir(parents=True, exist_ok=True)
    clean_edt_base = symbol_dir / f"{symbol}_{interval}_unadjusted_clean_edt_{suffix}"
    metadata_path = symbol_dir / f"{symbol}_{interval}_metadata_{suffix}.json"

    if not force and output_exists(clean_edt_base, write_csv, write_parquet):
        return {
            "symbol": symbol,
            "status": "skipped_existing",
            "metadata_path": str(metadata_path),
        }

    if startup_delay > 0:
        print(f"[{symbol}] startup delay {startup_delay:.1f}s", flush=True)
        time.sleep(startup_delay)

    client_kwargs = {"dataframe_type": "pandas"}
    if creds_file:
        client_kwargs["creds_file"] = str(creds_file)

    def make_client() -> ThetaClient:
        return make_safe_client(**client_kwargs)

    if existing_client is None:
        client = with_retries(
            f"[{symbol}] auth",
            make_client,
            retries=retries,
            base_delay=retry_base_delay,
            max_delay=retry_max_delay,
        )
    else:
        client = existing_client

    def refresh_client_on_session_error(exc: Exception) -> None:
        nonlocal client
        if is_session_error(exc):
            print(f"[{symbol}] refreshing ThetaData session", flush=True)
            client = make_client()

    frames: list[pd.DataFrame] = []
    attempted_chunks = 0
    nonempty_chunks = 0
    for chunk_start, chunk_end in month_chunks(start_date, end_date):
        attempted_chunks += 1
        print(f"[{symbol}] {chunk_start} -> {chunk_end}", flush=True)
        try:
            df = with_retries(
                f"[{symbol}] {chunk_start}->{chunk_end}",
                lambda: history_ohlc(
                    client=client,
                    asset_type=asset_type,
                    symbol=symbol,
                    interval=interval,
                    start_date=chunk_start,
                    end_date=chunk_end,
                    venue=venue,
                ),
                retries=retries,
                base_delay=retry_base_delay,
                max_delay=retry_max_delay,
                on_retry=refresh_client_on_session_error,
            )
        except Exception as exc:
            if is_empty_data_error(exc):
                print(f"[{symbol}] skipped chunk {chunk_start}->{chunk_end}: {type(exc).__name__}", flush=True)
                continue
            raise
        if len(df) == 0:
            continue
        nonempty_chunks += 1
        frames.append(pd.DataFrame(df))

    if not frames:
        metadata = {
            "symbol": symbol,
            "asset_type": asset_type,
            "status": "no_rows",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "attempted_chunks": attempted_chunks,
            "nonempty_chunks": nonempty_chunks,
        }
        metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        return metadata

    raw = pd.concat(frames, ignore_index=True)
    raw = ensure_timestamp_column(raw)
    raw["timestamp"] = pd.to_datetime(raw["timestamp"])
    raw = raw.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    clean = clean_bars(raw)
    clean_edt = normalize_to_fixed_edt(clean)

    outputs: dict[str, dict[str, str]] = {}
    if write_raw:
        outputs["raw"] = write_frame(raw, symbol_dir / f"{symbol}_{interval}_raw_{suffix}", write_csv, write_parquet)
    if write_clean_local:
        outputs["clean_local"] = write_frame(clean, symbol_dir / f"{symbol}_{interval}_unadjusted_clean_{suffix}", write_csv, write_parquet)
    outputs["clean_edt"] = write_frame(clean_edt, clean_edt_base, write_csv, write_parquet)

    metadata = {
        "symbol": symbol,
        "asset_type": asset_type,
        "status": "completed",
        "start_date": start_date.isoformat(),
        "end_date": min(end_date, clean_edt["timestamp"].max().date()).isoformat(),
        "requested_end_date": end_date.isoformat(),
        "interval": interval,
        "venue": venue if asset_type == "stock" else None,
        "attempted_chunks": attempted_chunks,
        "nonempty_chunks": nonempty_chunks,
        "raw_rows": len(raw),
        "clean_rows": len(clean),
        "clean_edt_first_ts": str(clean_edt["timestamp"].min()) if len(clean_edt) else None,
        "clean_edt_last_ts": str(clean_edt["timestamp"].max()) if len(clean_edt) else None,
        "clean_edt_offsets": clean_edt["timestamp"].astype(str).str[-6:].unique().tolist() if len(clean_edt) else [],
        "outputs": outputs,
    }
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    return metadata


def parse_args() -> argparse.Namespace:
    today_ny = pd.Timestamp.now(tz=NY_TZ).date()
    default_end = last_weekday(today_ny - timedelta(days=1))

    parser = argparse.ArgumentParser(description="Parallel download and clean full-history 1-minute bars from ThetaData.")
    parser.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS)
    parser.add_argument("--asset-type", default=DEFAULT_ASSET_TYPE, choices=ASSET_TYPES)
    parser.add_argument("--start-date", type=parse_yyyymmdd, default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", type=parse_yyyymmdd, default=default_end)
    parser.add_argument("--interval", default=DEFAULT_INTERVAL, choices=["1m"])
    parser.add_argument("--venue", default=DEFAULT_VENUE, choices=["utp_cta", "nqb"])
    parser.add_argument("--output-dir", type=Path, default=Path("data/batch_1m"))
    parser.add_argument("--ranges-file", type=Path, help="JSON file with per-symbol start_date/end_date discovered by discover_1m_ranges.py.")
    parser.add_argument("--creds-file", type=Path)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--retries", type=int, default=8)
    parser.add_argument("--retry-base-delay", type=float, default=10.0)
    parser.add_argument("--retry-max-delay", type=float, default=180.0)
    parser.add_argument("--startup-stagger-seconds", type=float, default=20.0)
    parser.add_argument("--force", action="store_true", help="Redownload symbols even if final clean_edt files already exist.")
    parser.add_argument("--write-raw", action="store_true", help="Also write raw ThetaData rows, including empty OHLC placeholders.")
    parser.add_argument("--write-clean-local", action="store_true", help="Also write cleaned bars with original NY DST/EST offsets.")
    parser.add_argument("--no-csv", action="store_true", help="Do not write CSV files.")
    parser.add_argument("--no-parquet", action="store_true", help="Do not write Parquet files.")
    return parser.parse_args()


def load_ranges(path: Path | None) -> dict[str, dict]:
    if path is None:
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    ranges: dict[str, dict] = {}
    for item in payload.get("ranges", []):
        symbol = str(item["symbol"]).upper()
        ranges[symbol] = item
    return ranges


def main() -> None:
    args = parse_args()
    if args.start_date > args.end_date:
        raise ValueError("--start-date must be on or before --end-date")
    if args.no_csv and args.no_parquet:
        raise ValueError("At least one of CSV or Parquet output must be enabled.")

    symbols = dedupe_symbols(args.symbols)
    if args.workers < 1 or args.retries < 1:
        raise ValueError("workers and retries must be positive")
    args.output_dir = args.output_dir.expanduser().resolve()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Symbols ({len(symbols)}): {' '.join(symbols)}", flush=True)
    print(f"Asset type: {args.asset_type}", flush=True)
    print(f"Range: {args.start_date} -> {args.end_date}", flush=True)
    print(f"Workers: {args.workers}", flush=True)
    print(f"Output: {args.output_dir.resolve()}", flush=True)

    discovered_ranges = load_ranges(args.ranges_file)
    jobs = []
    for index, symbol in enumerate(symbols):
        symbol_range = discovered_ranges.get(symbol, {})
        symbol_start_date = parse_yyyymmdd(symbol_range["start_date"]) if symbol_range.get("start_date") else args.start_date
        symbol_end_date = parse_yyyymmdd(symbol_range["end_date"]) if symbol_range.get("end_date") else args.end_date
        jobs.append({
            "symbol": symbol,
            "asset_type": args.asset_type,
            "start_date": symbol_start_date,
            "end_date": symbol_end_date,
            "interval": args.interval,
            "venue": args.venue,
            "output_dir": args.output_dir,
            "write_csv": not args.no_csv,
            "write_parquet": not args.no_parquet,
            "write_raw": args.write_raw,
            "write_clean_local": args.write_clean_local,
            "force": args.force,
            "creds_file": args.creds_file,
            "retries": args.retries,
            "retry_base_delay": args.retry_base_delay,
            "retry_max_delay": args.retry_max_delay,
            "startup_delay": (index % max(args.workers, 1)) * args.startup_stagger_seconds,
        })

    results: list[dict] = []
    failures: list[dict] = []

    if args.workers <= 1:
        client_kwargs = {"dataframe_type": "pandas"}
        if args.creds_file:
            client_kwargs["creds_file"] = str(args.creds_file)
        shared_client = with_retries(
            "[sequential] auth",
            lambda: make_safe_client(**client_kwargs),
            retries=args.retries,
            base_delay=args.retry_base_delay,
            max_delay=args.retry_max_delay,
        )
        for job in jobs:
            symbol = job["symbol"]
            try:
                job["startup_delay"] = 0.0
                result = download_symbol(job, existing_client=shared_client)
                results.append(result)
                print(f"[{symbol}] {result.get('status')} rows={result.get('clean_rows', '-')}", flush=True)
            except Exception as exc:
                failure = {
                    "symbol": symbol,
                    "status": "failed",
                    "error": type(exc).__name__,
                    
                }
                failures.append(failure)
                print(f"[{symbol}] failed: {type(exc).__name__}", flush=True)
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
            future_to_symbol = {executor.submit(download_symbol, job): job["symbol"] for job in jobs}
            for future in concurrent.futures.as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    result = future.result()
                    results.append(result)
                    print(f"[{symbol}] {result.get('status')} rows={result.get('clean_rows', '-')}", flush=True)
                except Exception as exc:
                    failure = {
                        "symbol": symbol,
                        "status": "failed",
                        "error": type(exc).__name__,
                        
                    }
                    failures.append(failure)
                    print(f"[{symbol}] failed: {type(exc).__name__}", flush=True)

    summary = {
        "status": "completed_with_failures" if failures else "completed",
        "symbols": symbols,
        "asset_type": args.asset_type,
        "start_date": args.start_date.isoformat(),
        "end_date": args.end_date.isoformat(),
        "workers": args.workers,
        "results": sorted(results, key=lambda item: item.get("symbol", "")),
        "failures": failures,
    }
    summary_path = args.output_dir / f"batch_summary_{yyyymmdd(args.start_date)}_{yyyymmdd(args.end_date)}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Wrote {summary_path}", flush=True)
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Download failed: {type(exc).__name__}", flush=True)
        raise SystemExit(1) from None
