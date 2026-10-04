#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from thetadata import ThetaClient

from batch_download_clean_1m import (
    ASSET_TYPES,
    make_safe_client,
    DEFAULT_ASSET_TYPE,
    DEFAULT_INTERVAL,
    DEFAULT_VENUE,
    NY_TZ,
    clean_bars,
    dedupe_symbols,
    ensure_timestamp_column,
    history_ohlc,
    is_empty_data_error,
    is_session_error,
    last_weekday,
    month_chunks,
    normalize_to_fixed_edt,
    parse_yyyymmdd,
    with_retries,
    write_frame,
    yyyymmdd,
)


METADATA_RE = re.compile(r"^(?P<symbol>.+)_1m_metadata_(?P<start>\d{8})_(?P<end>\d{8})\.json$")


def parse_args() -> argparse.Namespace:
    today_ny = pd.Timestamp.now(tz=NY_TZ).date()
    default_end = last_weekday(today_ny - timedelta(days=1))

    parser = argparse.ArgumentParser(description="Incrementally update existing cleaned 1-minute ThetaData files.")
    parser.add_argument("--asset-type", default=DEFAULT_ASSET_TYPE, choices=ASSET_TYPES)
    parser.add_argument("--symbols", nargs="+", help="Symbols to update. Defaults to symbol directories in output-dir.")
    parser.add_argument("--add-symbols", nargs="*", default=[], help="New symbols to include even when no metadata exists yet.")
    parser.add_argument("--new-symbol-start-date", type=parse_yyyymmdd, default=parse_yyyymmdd("19900101"))
    parser.add_argument("--end-date", type=parse_yyyymmdd, default=default_end)
    parser.add_argument("--interval", default=DEFAULT_INTERVAL, choices=["1m"])
    parser.add_argument("--venue", default=DEFAULT_VENUE, choices=["utp_cta", "nqb"])
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--creds-file", type=Path)
    parser.add_argument("--retries", type=int, default=8)
    parser.add_argument("--retry-base-delay", type=float, default=10.0)
    parser.add_argument("--retry-max-delay", type=float, default=180.0)
    parser.add_argument("--no-csv", action="store_true", help="Do not write updated CSV files.")
    parser.add_argument("--no-parquet", action="store_true", help="Do not write updated Parquet files.")
    parser.add_argument("--prune-old", action="store_true", help="Remove superseded clean_edt and metadata files after a symbol succeeds.")
    return parser.parse_args()


def symbol_dirs(output_dir: Path) -> list[str]:
    return sorted(path.name.upper() for path in output_dir.iterdir() if path.is_dir())


def latest_metadata_path(symbol_dir: Path, symbol: str, interval: str) -> Path | None:
    best: tuple[date, Path] | None = None
    for path in symbol_dir.glob(f"{symbol}_{interval}_metadata_*.json"):
        match = METADATA_RE.match(path.name)
        if not match:
            continue
        end_date = parse_yyyymmdd(json.loads(path.read_text())["end_date"])
        if best is None or end_date > best[0]:
            best = (end_date, path)
    return best[1] if best else None


def load_metadata(path: Path | None) -> dict | None:
    if path is None:
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def clean_path_from_metadata(metadata: dict, symbol_dir: Path, symbol: str, interval: str, start_date: date, end_date: date) -> Path:
    outputs = metadata.get("outputs", {}).get("clean_edt", {})
    for key in ("parquet", "csv"):
        value = outputs.get(key)
        if value:
            # Prefer the local basename so relocated datasets do not read an old checkout.
            local = symbol_dir / Path(value).name
            if local.exists():
                return local
            if Path(value).is_absolute() and Path(value).exists():
                return Path(value)

    suffix = f"{yyyymmdd(start_date)}_{yyyymmdd(end_date)}"
    for suffix_name in ("parquet", "csv"):
        path = symbol_dir / f"{symbol}_{interval}_unadjusted_clean_edt_{suffix}.{suffix_name}"
        if path.exists():
            return path
    raise FileNotFoundError(f"Could not find clean_edt data for {symbol}")


def read_existing_clean(metadata: dict, symbol_dir: Path, symbol: str, interval: str, start_date: date, end_date: date) -> pd.DataFrame:
    path = clean_path_from_metadata(metadata, symbol_dir, symbol, interval, start_date, end_date)
    if path.suffix == ".parquet":
        frame = pd.read_parquet(path)
    elif path.suffix == ".csv":
        frame = pd.read_csv(path)
    else:
        raise ValueError(f"Unsupported existing data file: {path}")
    frame = ensure_timestamp_column(frame)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])
    return frame


def make_client_factory(creds_file: Path | None):
    client_kwargs = {"dataframe_type": "pandas"}
    if creds_file:
        client_kwargs["creds_file"] = str(creds_file)

    def make_client() -> ThetaClient:
        return make_safe_client(**client_kwargs)

    return make_client


def download_clean_range(
    *,
    client_holder: dict,
    make_client,
    asset_type: str,
    symbol: str,
    interval: str,
    start_date: date,
    end_date: date,
    venue: str,
    retries: int,
    retry_base_delay: float,
    retry_max_delay: float,
) -> tuple[pd.DataFrame, int, int, int]:
    frames: list[pd.DataFrame] = []
    attempted_chunks = 0
    nonempty_chunks = 0
    raw_rows = 0

    def refresh_client_on_session_error(exc: Exception) -> None:
        if is_session_error(exc):
            print(f"[{symbol}] refreshing ThetaData session", flush=True)
            client_holder["client"] = make_client()

    for chunk_start, chunk_end in month_chunks(start_date, end_date):
        attempted_chunks += 1
        print(f"[{symbol}] delta {chunk_start} -> {chunk_end}", flush=True)
        try:
            df = with_retries(
                f"[{symbol}] {chunk_start}->{chunk_end}",
                lambda: history_ohlc(
                    client=client_holder["client"],
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
                print(f"[{symbol}] skipped delta {chunk_start}->{chunk_end}: {type(exc).__name__}", flush=True)
                continue
            raise
        if len(df) == 0:
            continue
        nonempty_chunks += 1
        raw_rows += len(df)
        frames.append(pd.DataFrame(df))

    if not frames:
        return pd.DataFrame(), attempted_chunks, nonempty_chunks, raw_rows

    raw = pd.concat(frames, ignore_index=True)
    raw = ensure_timestamp_column(raw)
    raw["timestamp"] = pd.to_datetime(raw["timestamp"])
    raw = raw.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    clean = clean_bars(raw)
    return normalize_to_fixed_edt(clean), attempted_chunks, nonempty_chunks, raw_rows


def prune_superseded(symbol_dir: Path, symbol: str, interval: str, keep_suffix: str) -> list[str]:
    removed: list[str] = []
    patterns = [
        f"{symbol}_{interval}_metadata_*.json",
        f"{symbol}_{interval}_unadjusted_clean_edt_*.csv",
        f"{symbol}_{interval}_unadjusted_clean_edt_*.parquet",
    ]
    for pattern in patterns:
        for path in symbol_dir.glob(pattern):
            if keep_suffix in path.name:
                continue
            path.unlink()
            removed.append(str(path))
    return removed


def update_symbol(args: argparse.Namespace, symbol: str, client_holder: dict, make_client) -> dict:
    symbol_dir = args.output_dir / symbol
    symbol_dir.mkdir(parents=True, exist_ok=True)

    metadata_path = latest_metadata_path(symbol_dir, symbol, args.interval)
    metadata = load_metadata(metadata_path)
    if metadata:
        start_date = parse_yyyymmdd(metadata["start_date"])
        current_end_date = parse_yyyymmdd(metadata["end_date"])
        existing = read_existing_clean(metadata, symbol_dir, symbol, args.interval, start_date, current_end_date)
    else:
        start_date = args.new_symbol_start_date
        current_end_date = start_date - timedelta(days=1)
        existing = pd.DataFrame()

    if current_end_date >= args.end_date and len(existing):
        return {
            "symbol": symbol,
            "status": "already_latest",
            "start_date": start_date.isoformat(),
            "end_date": current_end_date.isoformat(),
            "clean_rows": len(existing),
            "metadata_path": str(metadata_path) if metadata_path else None,
        }

    delta_start = current_end_date + timedelta(days=1)
    delta, attempted_chunks, nonempty_chunks, downloaded_raw_rows = download_clean_range(
        client_holder=client_holder,
        make_client=make_client,
        asset_type=args.asset_type,
        symbol=symbol,
        interval=args.interval,
        start_date=delta_start,
        end_date=args.end_date,
        venue=args.venue,
        retries=args.retries,
        retry_base_delay=args.retry_base_delay,
        retry_max_delay=args.retry_max_delay,
    )

    if delta.empty:
        return {"symbol": symbol, "status": "no_new_rows", "clean_rows": len(existing), "metadata_path": str(metadata_path) if metadata_path else None}

    combined = pd.concat([existing, delta], ignore_index=True) if len(existing) else delta
    if len(combined):
        combined = ensure_timestamp_column(combined)
        combined["timestamp"] = pd.to_datetime(combined["timestamp"])
        combined = clean_bars(combined)
        combined = combined.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    full_suffix = f"{yyyymmdd(start_date)}_{yyyymmdd(args.end_date)}"
    clean_edt_base = symbol_dir / f"{symbol}_{args.interval}_unadjusted_clean_edt_{full_suffix}"
    outputs = {
        "clean_edt": write_frame(combined, clean_edt_base, not args.no_csv, not args.no_parquet)
    }

    new_metadata_path = symbol_dir / f"{symbol}_{args.interval}_metadata_{full_suffix}.json"
    new_metadata = {
        "symbol": symbol,
        "asset_type": args.asset_type,
        "status": "completed",
        "start_date": start_date.isoformat(),
        "end_date": min(args.end_date, combined["timestamp"].max().date()).isoformat(),
        "requested_end_date": args.end_date.isoformat(),
        "interval": args.interval,
        "venue": args.venue if args.asset_type == "stock" else None,
        "previous_end_date": current_end_date.isoformat() if metadata else None,
        "delta_start_date": delta_start.isoformat(),
        "attempted_delta_chunks": attempted_chunks,
        "nonempty_delta_chunks": nonempty_chunks,
        "downloaded_raw_rows": downloaded_raw_rows,
        "delta_clean_rows": len(delta),
        "clean_rows": len(combined),
        "clean_edt_first_ts": str(combined["timestamp"].min()) if len(combined) else None,
        "clean_edt_last_ts": str(combined["timestamp"].max()) if len(combined) else None,
        "clean_edt_offsets": combined["timestamp"].astype(str).str[-6:].unique().tolist() if len(combined) else [],
        "outputs": outputs,
    }
    new_metadata_path.write_text(json.dumps(new_metadata, indent=2), encoding="utf-8")

    removed = prune_superseded(symbol_dir, symbol, args.interval, full_suffix) if args.prune_old else []
    new_metadata["metadata_path"] = str(new_metadata_path)
    new_metadata["status"] = "updated" if metadata else "created"
    new_metadata["removed_superseded_files"] = removed
    return new_metadata


def main() -> None:
    args = parse_args()
    if args.no_csv and args.no_parquet:
        raise ValueError("At least one of CSV or Parquet output must be enabled.")
    if args.retries < 1:
        raise ValueError("retries must be positive")
    args.output_dir = args.output_dir.expanduser().resolve()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    base_symbols = args.symbols if args.symbols else symbol_dirs(args.output_dir)
    symbols = dedupe_symbols([*base_symbols, *args.add_symbols])
    print(f"Symbols ({len(symbols)}): {' '.join(symbols)}", flush=True)
    print(f"Asset type: {args.asset_type}", flush=True)
    print(f"Target end date: {args.end_date}", flush=True)
    print(f"Output: {args.output_dir.resolve()}", flush=True)

    make_client = make_client_factory(args.creds_file)
    client_holder = {
        "client": with_retries(
            "[update] auth",
            make_client,
            retries=args.retries,
            base_delay=args.retry_base_delay,
            max_delay=args.retry_max_delay,
        )
    }

    results: list[dict] = []
    failures: list[dict] = []
    started_at = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    symbols = dedupe_symbols(symbols)
    for symbol in symbols:
        try:
            result = update_symbol(args, symbol, client_holder, make_client)
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
        "asset_type": args.asset_type,
        "symbols": symbols,
        "end_date": args.end_date.isoformat(),
        "started_at": started_at,
        "finished_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "results": results,
        "failures": failures,
    }
    summary_path = args.output_dir / f"incremental_update_summary_{yyyymmdd(args.end_date)}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Wrote {summary_path}", flush=True)
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Update failed: {type(exc).__name__}", flush=True)
        raise SystemExit(1) from None
