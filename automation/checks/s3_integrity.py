from __future__ import annotations

import json
import random
from pathlib import Path
from urllib.parse import urlparse

from automation.types import CheckResult, STATUS_FAIL, STATUS_PASS, STATUS_WARN
from src.s3_uploader import _get_s3_client, _get_s3_config


def _parse_s3_uri(uri: str) -> tuple[str, str] | None:
    if not uri.startswith("s3://"):
        return None
    parsed = urlparse(uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if not bucket or not key:
        return None
    return bucket, key


def run(config: dict) -> CheckResult:
    log_dir = Path(config.get("log_dir", "logs"))
    max_logs = int(config.get("max_logs", 5))
    sample_size = int(config.get("sample_size", 10))
    random_seed = int(config.get("random_seed", 42))
    allowed_missing = int(config.get("allowed_missing", 0))
    bucket_override = config.get("bucket")

    if not log_dir.exists():
        return CheckResult(
            name="s3_integrity",
            status=STATUS_WARN,
            summary=f"Log directory not found: {log_dir}",
        )

    log_files = sorted(log_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    log_files = log_files[:max_logs]
    if not log_files:
        return CheckResult(
            name="s3_integrity",
            status=STATUS_WARN,
            summary="No logs found to build S3 verification sample",
        )

    uris: set[str] = set()
    for log_file in log_files:
        try:
            payload = json.loads(log_file.read_text(encoding="utf-8"))
        except Exception:
            continue
        results = payload.get("results") or []
        for result in results:
            if result.get("status") not in {"SUCCESS", "SKIPPED"}:
                continue
            uri = result.get("s3_data_path")
            if isinstance(uri, str) and uri.startswith("s3://"):
                uris.add(uri)

    if not uris:
        return CheckResult(
            name="s3_integrity",
            status=STATUS_WARN,
            summary="No S3 URIs found in recent logs",
            metrics={"checked_logs": len(log_files), "candidate_uris": 0},
        )

    sorted_uris = sorted(uris)
    if len(sorted_uris) <= sample_size:
        sample_uris = sorted_uris
    else:
        rng = random.Random(random_seed)
        sample_uris = rng.sample(sorted_uris, sample_size)

    try:
        config_s3 = _get_s3_config(bucket=bucket_override)
        client = _get_s3_client(config_s3)
    except Exception as e:
        return CheckResult(
            name="s3_integrity",
            status=STATUS_FAIL,
            summary=f"Failed to initialize S3 client: {e}",
        )

    missing: list[str] = []
    checked = 0
    for uri in sample_uris:
        parsed = _parse_s3_uri(uri)
        if parsed is None:
            missing.append(f"invalid uri: {uri}")
            continue
        bucket, key = parsed
        checked += 1
        try:
            client.head_object(Bucket=bucket, Key=key)
        except Exception:
            missing.append(uri)

    if len(missing) > allowed_missing:
        return CheckResult(
            name="s3_integrity",
            status=STATUS_FAIL,
            summary=(
                f"S3 integrity failed: missing={len(missing)} "
                f"(allowed={allowed_missing})"
            ),
            details=missing[:20],
            metrics={
                "checked_logs": len(log_files),
                "candidate_uris": len(sorted_uris),
                "sampled_uris": len(sample_uris),
                "head_checked": checked,
                "missing": len(missing),
            },
        )

    if missing:
        return CheckResult(
            name="s3_integrity",
            status=STATUS_WARN,
            summary=f"S3 integrity has warnings: missing={len(missing)}",
            details=missing[:20],
            metrics={
                "checked_logs": len(log_files),
                "candidate_uris": len(sorted_uris),
                "sampled_uris": len(sample_uris),
                "head_checked": checked,
                "missing": len(missing),
            },
        )

    return CheckResult(
        name="s3_integrity",
        status=STATUS_PASS,
        summary="S3 integrity sample check passed",
        metrics={
            "checked_logs": len(log_files),
            "candidate_uris": len(sorted_uris),
            "sampled_uris": len(sample_uris),
            "head_checked": checked,
        },
    )

