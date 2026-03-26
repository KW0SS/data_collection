from __future__ import annotations

import json
from pathlib import Path

from automation.types import CheckResult, STATUS_FAIL, STATUS_PASS, STATUS_WARN

REQUIRED_TOP_LEVEL = {"run_id", "member", "started_at", "finished_at", "input", "summary", "results"}
REQUIRED_SUMMARY = {"total", "success", "skipped", "failed"}
ALLOWED_RESULT_STATUS = {"SUCCESS", "SKIPPED", "FAILED"}


def run(config: dict) -> CheckResult:
    log_dir = Path(config.get("log_dir", "logs"))
    max_files = int(config.get("max_files", 20))

    if not log_dir.exists():
        return CheckResult(
            name="log_schema",
            status=STATUS_WARN,
            summary=f"Log directory not found: {log_dir}",
        )

    log_files = sorted(log_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    log_files = log_files[:max_files]
    if not log_files:
        return CheckResult(
            name="log_schema",
            status=STATUS_WARN,
            summary="No log JSON files found",
            metrics={"checked_logs": 0},
        )

    errors: list[str] = []
    warn_count = 0
    checked_results = 0

    for log_file in log_files:
        try:
            payload = json.loads(log_file.read_text(encoding="utf-8"))
        except Exception as e:
            errors.append(f"{log_file}: invalid JSON ({e})")
            continue

        missing = [k for k in REQUIRED_TOP_LEVEL if k not in payload]
        if missing:
            errors.append(f"{log_file}: missing keys {missing}")
            continue

        summary = payload.get("summary") or {}
        missing_summary = [k for k in REQUIRED_SUMMARY if k not in summary]
        if missing_summary:
            errors.append(f"{log_file}: missing summary keys {missing_summary}")
            continue

        results = payload.get("results") or []
        if not isinstance(results, list):
            errors.append(f"{log_file}: results must be list")
            continue

        for idx, result in enumerate(results):
            checked_results += 1
            status = result.get("status")
            if status not in ALLOWED_RESULT_STATUS:
                errors.append(f"{log_file} results[{idx}]: invalid status '{status}'")
            if not result.get("ticker"):
                warn_count += 1

    if errors:
        return CheckResult(
            name="log_schema",
            status=STATUS_FAIL,
            summary=f"Log schema validation failed ({len(errors)} errors)",
            details=errors[:20],
            metrics={"checked_logs": len(log_files), "checked_results": checked_results},
        )

    if warn_count > 0:
        return CheckResult(
            name="log_schema",
            status=STATUS_WARN,
            summary=f"Log schema valid with warnings (missing ticker in {warn_count} results)",
            metrics={"checked_logs": len(log_files), "checked_results": checked_results},
        )

    return CheckResult(
        name="log_schema",
        status=STATUS_PASS,
        summary="Log schema is valid",
        metrics={"checked_logs": len(log_files), "checked_results": checked_results},
    )

