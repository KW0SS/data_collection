from __future__ import annotations

import csv
from pathlib import Path

from automation.types import CheckResult, STATUS_FAIL, STATUS_PASS, STATUS_WARN


def _parse_ticker_year(csv_path: Path) -> tuple[str, str]:
    stem = csv_path.stem
    parts = stem.rsplit("_", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return stem, ""


def run(config: dict) -> CheckResult:
    output_dir = Path(config.get("output_dir", "data/output"))
    raw_dir = Path(config.get("raw_dir", "data/raw"))
    sample_size = int(config.get("sample_size", 50))

    if not output_dir.exists():
        return CheckResult(
            name="local_data",
            status=STATUS_FAIL,
            summary=f"Output directory not found: {output_dir}",
        )

    output_files = sorted(output_dir.rglob("*.csv"))
    if not output_files:
        return CheckResult(
            name="local_data",
            status=STATUS_FAIL,
            summary=f"No output CSV files found in {output_dir}",
        )

    sampled_files = output_files[:sample_size]
    required_cols = {"stock_code", "corp_name", "year", "quarter", "label"}
    bad_headers: list[str] = []
    empty_csvs: list[str] = []

    for csv_file in sampled_files:
        with open(csv_file, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            header = set(reader.fieldnames or [])
            if not required_cols.issubset(header):
                bad_headers.append(str(csv_file))
            first_row = next(reader, None)
            if first_row is None:
                empty_csvs.append(str(csv_file))

    if bad_headers:
        return CheckResult(
            name="local_data",
            status=STATUS_FAIL,
            summary=f"CSV header validation failed in {len(bad_headers)} files",
            details=bad_headers[:20],
            metrics={"output_csv_count": len(output_files), "sampled": len(sampled_files)},
        )

    raw_files = list(raw_dir.rglob("*.json")) if raw_dir.exists() else []
    if not raw_files:
        details = []
        if empty_csvs:
            details.extend([f"empty csv: {p}" for p in empty_csvs[:10]])
        return CheckResult(
            name="local_data",
            status=STATUS_WARN,
            summary="Output CSV exists, but raw JSON not found (raw save may be disabled)",
            details=details,
            metrics={
                "output_csv_count": len(output_files),
                "sampled": len(sampled_files),
                "empty_csv_count": len(empty_csvs),
                "raw_json_count": 0,
            },
        )

    missing_raw = 0
    missing_raw_files: list[str] = []
    for csv_file in sampled_files:
        ticker, year = _parse_ticker_year(csv_file)
        if not ticker or not year:
            continue
        pattern = f"{ticker}_{year}_*.json"
        matched = list(raw_dir.glob(pattern))
        if not matched:
            missing_raw += 1
            missing_raw_files.append(str(csv_file))

    if missing_raw > 0 or empty_csvs:
        details = [f"missing raw: {p}" for p in missing_raw_files[:10]]
        details.extend([f"empty csv: {p}" for p in empty_csvs[:10]])
        return CheckResult(
            name="local_data",
            status=STATUS_WARN,
            summary=(
                f"Local data checked with warnings "
                f"(missing_raw={missing_raw}, empty_csv={len(empty_csvs)})"
            ),
            details=details,
            metrics={
                "output_csv_count": len(output_files),
                "sampled": len(sampled_files),
                "raw_json_count": len(raw_files),
                "missing_raw_count": missing_raw,
                "empty_csv_count": len(empty_csvs),
            },
        )

    return CheckResult(
        name="local_data",
        status=STATUS_PASS,
        summary="Output/raw local data consistency looks good",
        metrics={
            "output_csv_count": len(output_files),
            "sampled": len(sampled_files),
            "raw_json_count": len(raw_files),
        },
    )

