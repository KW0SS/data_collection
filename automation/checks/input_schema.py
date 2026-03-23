from __future__ import annotations

import csv
from pathlib import Path

from automation.types import CheckResult, STATUS_FAIL, STATUS_PASS, STATUS_WARN


def run(config: dict) -> CheckResult:
    csv_path = Path(config.get("path", "data/input/companies_template.csv"))
    required_columns = config.get(
        "required_columns",
        ["stock_code", "corp_name", "label", "gics_sector", "start_year", "end_year"],
    )

    if not csv_path.exists():
        return CheckResult(
            name="input_schema",
            status=STATUS_FAIL,
            summary=f"Input CSV not found: {csv_path}",
        )

    errors: list[str] = []
    warnings: list[str] = []
    row_count = 0

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        missing_cols = [c for c in required_columns if c not in header]
        if missing_cols:
            return CheckResult(
                name="input_schema",
                status=STATUS_FAIL,
                summary=f"Missing required columns: {missing_cols}",
                metrics={"rows": 0},
            )

        for idx, row in enumerate(reader, start=2):
            row_count += 1
            stock_code = (row.get("stock_code") or "").strip()
            label = (row.get("label") or "").strip()
            start_year = (row.get("start_year") or "").strip()
            end_year = (row.get("end_year") or "").strip()
            gics_sector = (row.get("gics_sector") or "").strip()

            if not stock_code.isdigit() or len(stock_code) != 6:
                errors.append(f"line {idx}: invalid stock_code '{stock_code}'")
            if label not in {"0", "1"}:
                errors.append(f"line {idx}: invalid label '{label}'")

            if start_year and (not start_year.isdigit() or len(start_year) != 4):
                errors.append(f"line {idx}: invalid start_year '{start_year}'")
            if end_year and (not end_year.isdigit() or len(end_year) != 4):
                errors.append(f"line {idx}: invalid end_year '{end_year}'")
            if start_year.isdigit() and end_year.isdigit() and int(start_year) > int(end_year):
                errors.append(
                    f"line {idx}: start_year '{start_year}' > end_year '{end_year}'"
                )

            if not gics_sector:
                warnings.append(f"line {idx}: empty gics_sector")

    if errors:
        return CheckResult(
            name="input_schema",
            status=STATUS_FAIL,
            summary=f"Input schema validation failed ({len(errors)} errors)",
            details=errors[:20],
            metrics={"rows": row_count, "errors": len(errors), "warnings": len(warnings)},
        )

    if warnings:
        return CheckResult(
            name="input_schema",
            status=STATUS_WARN,
            summary=f"Input schema valid with warnings ({len(warnings)})",
            details=warnings[:20],
            metrics={"rows": row_count, "warnings": len(warnings)},
        )

    return CheckResult(
        name="input_schema",
        status=STATUS_PASS,
        summary="Input CSV schema is valid",
        metrics={"rows": row_count},
    )

