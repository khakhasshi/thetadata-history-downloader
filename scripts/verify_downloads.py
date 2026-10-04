#!/usr/bin/env python3
"""Validate the latest cleaned dataset per symbol; never contacts the provider."""
import argparse
import json
from pathlib import Path
import pandas as pd
from batch_download_clean_1m import clean_bars, dedupe_symbols
from update_1m_data_to_latest import latest_metadata_path, load_metadata, clean_path_from_metadata


def verify(root, symbols=None, expected_end=None):
    results, failures = [], []
    symbols = dedupe_symbols(symbols or [p.name for p in root.iterdir() if p.is_dir()])
    if not symbols:
        raise ValueError('No symbol datasets found')
    for symbol in symbols:
        try:
            folder = root / symbol
            meta = load_metadata(latest_metadata_path(folder, symbol, '1m'))
            if not meta or not meta.get('outputs'):
                raise ValueError('No downloaded data')
            path = clean_path_from_metadata(meta, folder, symbol, '1m', pd.Timestamp(meta['start_date']).date(), pd.Timestamp(meta['end_date']).date())
            frame = pd.read_parquet(path) if path.suffix == '.parquet' else pd.read_csv(path)
            ts = pd.to_datetime(frame['timestamp'])
            if frame.empty or ts.isna().any() or ts.duplicated().any() or not ts.is_monotonic_increasing:
                raise ValueError('Empty, duplicate, or unsorted timestamps')
            if len(clean_bars(frame)) != len(frame):
                raise ValueError('Invalid OHLC rows')
            if ts.dt.tz is None or str(ts.dt.tz) not in ('Etc/GMT+4', 'UTC-04:00'):
                raise ValueError('Unexpected timezone')
            latest = str(ts.iloc[-1].date())
            if expected_end and latest != expected_end:
                raise ValueError('Expected last session not covered')
            results.append({'symbol': symbol, 'rows': len(frame), 'first_timestamp': str(ts.iloc[0]), 'last_timestamp': str(ts.iloc[-1])})
        except Exception as exc:
            failures.append({'symbol': symbol, 'error': type(exc).__name__})
    return {'status': 'passed' if not failures else 'failed', 'results': results, 'failures': failures}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output-dir', type=Path, required=True)
    p.add_argument('--symbols', nargs='+')
    p.add_argument('--expected-end', help='Actual final session date, YYYY-MM-DD')
    args = p.parse_args()
    report = verify(args.output_dir.resolve(), args.symbols, args.expected_end)
    print(json.dumps(report, indent=2))
    if report['failures']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
