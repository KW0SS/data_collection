from __future__ import annotations

import argparse
import json
import traceback
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Callable

from automation.checks import input_schema, local_data, log_schema, s3_integrity
from automation.types import (
    CheckResult,
    STATUS_FAIL,
    STATUS_PASS,
    STATUS_WARN,
)

ModeName = str
CheckRunner = Callable[[dict], CheckResult]

MODE_TO_CHECKS: dict[ModeName, list[tuple[str, CheckRunner]]] = {
    "non-s3": [
        ("input_schema", input_schema.run),
        ("local_data", local_data.run),
        ("log_schema", log_schema.run),
    ],
    "s3-only": [
        ("s3_integrity", s3_integrity.run),
    ],
    "all": [
        ("input_schema", input_schema.run),
        ("local_data", local_data.run),
        ("log_schema", log_schema.run),
        ("s3_integrity", s3_integrity.run),
    ],
}

DEFAULT_CONFIG = {
    "reports_dir": "automation/reports",
    "checks": {
        "input_schema": {
            "path": "data/input/companies_template.csv",
            "required_columns": [
                "stock_code",
                "corp_name",
                "label",
                "gics_sector",
                "start_year",
                "end_year",
            ],
        },
        "local_data": {
            "output_dir": "data/output",
            "raw_dir": "data/raw",
            "sample_size": 50,
        },
        "log_schema": {
            "log_dir": "logs",
            "max_files": 20,
        },
        "s3_integrity": {
            "log_dir": "logs",
            "max_logs": 5,
            "sample_size": 10,
            "allowed_missing": 0,
            "random_seed": 42,
            "bucket": None,
        },
    },
}


def _load_config(config_path: Path) -> dict:
    config = json.loads(json.dumps(DEFAULT_CONFIG))
    if not config_path.exists():
        return config

    user_config = json.loads(config_path.read_text(encoding="utf-8"))

    # shallow+checks merge
    config["reports_dir"] = user_config.get("reports_dir", config["reports_dir"])
    user_checks = user_config.get("checks", {})
    for check_name, base in config["checks"].items():
        override = user_checks.get(check_name, {})
        if isinstance(override, dict):
            merged = dict(base)
            merged.update(override)
            config["checks"][check_name] = merged

    return config


def _write_reports(report: dict, reports_dir: Path) -> tuple[Path, Path]:
    reports_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    mode = report.get("mode", "unknown")
    json_path = reports_dir / f"{ts}_{mode}_report.json"
    md_path = reports_dir / f"{ts}_{mode}_report.md"

    json_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    lines: list[str] = []
    lines.append(f"# Automation Check Report ({report['mode']})")
    lines.append("")
    lines.append(f"- started_at: {report['started_at']}")
    lines.append(f"- finished_at: {report['finished_at']}")
    lines.append(f"- duration_ms: {report['duration_ms']}")
    lines.append(f"- overall: {report['overall_status']}")
    lines.append("")
    lines.append("| check | status | duration_ms | summary |")
    lines.append("|---|---|---:|---|")
    for result in report["results"]:
        lines.append(
            f"| {result['name']} | {result['status']} | "
            f"{result.get('duration_ms') or 0} | {result['summary']} |"
        )
    lines.append("")
    for result in report["results"]:
        details = result.get("details") or []
        if not details:
            continue
        lines.append(f"## {result['name']} details")
        for detail in details:
            lines.append(f"- {detail}")
        lines.append("")

    md_path.write_text("\n".join(lines), encoding="utf-8")
    return json_path, md_path


def _run_one(check_name: str, runner: CheckRunner, config: dict) -> CheckResult:
    started = perf_counter()
    try:
        result = runner(config)
    except Exception as e:
        tb = traceback.format_exc(limit=2)
        result = CheckResult(
            name=check_name,
            status=STATUS_FAIL,
            summary=f"Unhandled exception: {e}",
            details=[tb],
        )
    elapsed_ms = int((perf_counter() - started) * 1000)
    result.duration_ms = elapsed_ms
    if result.name != check_name:
        result.name = check_name
    return result


def _overall_status(results: list[CheckResult], fail_on_warn: bool) -> str:
    if any(r.status == STATUS_FAIL for r in results):
        return STATUS_FAIL
    if fail_on_warn and any(r.status == STATUS_WARN for r in results):
        return STATUS_FAIL
    if any(r.status == STATUS_WARN for r in results):
        return STATUS_WARN
    return STATUS_PASS


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Local automation checks runner (without GitHub Actions)"
    )
    parser.add_argument(
        "--mode",
        choices=["non-s3", "s3-only", "all"],
        default="non-s3",
        help="Check mode: non-s3 / s3-only / all",
    )
    parser.add_argument(
        "--config",
        default="automation/config.json",
        help="Config JSON path",
    )
    parser.add_argument(
        "--fail-on-warn",
        action="store_true",
        help="Treat WARN as failure (exit 1)",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    config = _load_config(config_path)

    started_at = datetime.now()
    checks = MODE_TO_CHECKS[args.mode]
    results: list[CheckResult] = []

    print(f"[automation] mode={args.mode} checks={len(checks)} config={config_path}")
    for check_name, runner in checks:
        result = _run_one(
            check_name=check_name,
            runner=runner,
            config=config["checks"].get(check_name, {}),
        )
        results.append(result)
        print(f"- {result.name}: {result.status} ({result.duration_ms}ms) {result.summary}")

    finished_at = datetime.now()
    duration_ms = int((finished_at - started_at).total_seconds() * 1000)
    overall = _overall_status(results, fail_on_warn=args.fail_on_warn)

    report = {
        "mode": args.mode,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_ms": duration_ms,
        "overall_status": overall,
        "summary": {
            "total": len(results),
            "pass": sum(1 for r in results if r.status == STATUS_PASS),
            "warn": sum(1 for r in results if r.status == STATUS_WARN),
            "fail": sum(1 for r in results if r.status == STATUS_FAIL),
        },
        "results": [r.to_dict() for r in results],
    }

    reports_dir = Path(config.get("reports_dir", "automation/reports"))
    json_report, md_report = _write_reports(report, reports_dir)
    print(f"[automation] report json: {json_report}")
    print(f"[automation] report md  : {md_report}")
    print(f"[automation] overall={overall}")

    if overall == STATUS_FAIL:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
