"""S3 업로드 모듈 – 원본 재무제표 JSON을 GICS 섹터별로 S3에 저장.

S3 디렉터리 구조
─────────────────
s3://{bucket}/
  ├── healthy/
  │   └── {gics_sector}/
  │       ├── 005930_2023_Q1.json
  │       └── ...
  └── delisted/
      └── {gics_sector}/
          ├── 019440_2023_ANNUAL.json
          └── ...

필요한 환경변수 (.env)
─────────────────────
  S3_ACCESS_KEY    – AWS Access Key ID
  S3_PRIVATE_KEY   – AWS Secret Access Key
  S3_BUCKET_NAME   – S3 버킷 이름
  S3_REGION        – (선택) AWS 리전 (기본: ap-northeast-2)
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

# ── KST 타임존 ────────────────────────────────────────────────
KST = timezone(timedelta(hours=9))


def _load_env() -> dict[str, str]:
    """프로젝트 루트의 .env 파일에서 환경변수 읽기."""
    env: dict[str, str] = {}
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return env
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def _get_s3_config(
    bucket: str | None = None,
    region: str | None = None,
) -> dict[str, str]:
    """S3 접속 정보를 환경변수 + .env에서 가져옴."""
    env = _load_env()

    access_key = os.getenv("S3_ACCESS_KEY") or env.get("S3_ACCESS_KEY")
    secret_key = os.getenv("S3_PRIVATE_KEY") or env.get("S3_PRIVATE_KEY")
    bucket_name = bucket or os.getenv("S3_BUCKET_NAME") or env.get("S3_BUCKET_NAME")
    region_name = region or os.getenv("S3_REGION") or env.get("S3_REGION", "ap-northeast-2")

    if not access_key or not secret_key:
        raise RuntimeError(
            "S3 인증 키가 없습니다. .env에 S3_ACCESS_KEY, S3_PRIVATE_KEY를 설정하세요."
        )
    if not bucket_name:
        raise RuntimeError(
            "S3 버킷 이름이 없습니다. --s3-bucket 옵션이나 .env에 S3_BUCKET_NAME을 설정하세요."
        )

    return {
        "access_key": access_key,
        "secret_key": secret_key,
        "bucket": bucket_name,
        "region": region_name,
    }


def _get_s3_client(config: dict[str, str]):
    """boto3 S3 클라이언트 생성."""
    try:
        import boto3
    except ImportError:
        raise RuntimeError(
            "boto3가 설치되어 있지 않습니다. pip install boto3 를 실행하세요."
        )

    return boto3.client(
        "s3",
        aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"],
        region_name=config["region"],
    )


def _try_create_bucket(client, bucket: str, region: str) -> None:
    """버킷이 없을 때 생성을 시도합니다.

    IAM 사용자에 CreateBucket 권한이 없으면 경고만 출력하고 넘어갑니다.
    (PutObject 권한만 있어도 기존 버킷에 업로드는 가능)
    """
    try:
        print(f"  🪣 S3 버킷 '{bucket}' 생성 시도 중...", file=sys.stderr)
        if region == "us-east-1":
            client.create_bucket(Bucket=bucket)
        else:
            client.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        print(f"  ✅ S3 버킷 '{bucket}' 생성 완료", file=sys.stderr)
    except client.exceptions.ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            print(f"  ✅ S3 버킷 '{bucket}' 이미 존재", file=sys.stderr)
        elif error_code == "AccessDenied":
            print(
                f"  ⚠️  버킷 생성 권한 없음 (기존 버킷에 직접 업로드 시도)",
                file=sys.stderr,
            )
        else:
            raise


def _check_s3_exists(client, bucket: str, key: str) -> bool:
    """S3에 해당 key가 이미 존재하는지 확인.

    Returns:
        True면 존재, False면 없음.
    """
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def upload_raw_to_s3(
    raw_items: list[dict[str, Any]],
    stock_code: str,
    year: str,
    quarter: str,
    gics_sector: str,
    label: str = "0",
    bucket: str | None = None,
    region: str | None = None,
) -> str:
    """
    원본 재무제표 JSON 1건을 S3에 업로드.

    S3 Key: {healthy|delisted}/{gics_sector}/{stock_code}_{year}_{quarter}.json

    Args:
        raw_items: DART에서 받은 원시 재무제표 데이터
        stock_code: 종목코드
        year: 연도
        quarter: 분기 (Q1, H1, Q3, ANNUAL)
        gics_sector: GICS 섹터명 (예: "Energy", "Industrials")
        label: "0"=정상(healthy), "1"=상폐(delisted)
        bucket: S3 버킷 이름 (없으면 .env에서 읽기)
        region: AWS 리전 (없으면 .env에서 읽기)

    Returns:
        업로드된 S3 key
    """
    config = _get_s3_config(bucket, region)
    client = _get_s3_client(config)

    # S3 key 생성: {healthy|delisted}/{gics_sector}/{stock_code}_{year}_{quarter}.json
    status_prefix = "delisted" if str(label).strip() == "1" else "healthy"
    s3_key = f"{status_prefix}/{gics_sector}/{stock_code}_{year}_{quarter}.json"
    body = json.dumps(raw_items, ensure_ascii=False, indent=2).encode("utf-8")

    # 업로드 시도 → NoSuchBucket이면 버킷 생성 후 재시도
    try:
        client.put_object(
            Bucket=config["bucket"], Key=s3_key, Body=body,
            ContentType="application/json; charset=utf-8",
        )
    except client.exceptions.NoSuchBucket:
        _try_create_bucket(client, config["bucket"], config["region"])
        client.put_object(
            Bucket=config["bucket"], Key=s3_key, Body=body,
            ContentType="application/json; charset=utf-8",
        )

    return f"s3://{config['bucket']}/{s3_key}"


def upload_batch_to_s3(
    raw_data_list: list[dict[str, Any]],
    bucket: str | None = None,
    region: str | None = None,
    force: bool = False,
) -> list[str]:
    """
    여러 건의 원본 재무제표를 S3에 배치 업로드.
    이미 S3에 존재하는 파일은 건너뛰고, force=True면 덮어씁니다.

    Args:
        raw_data_list: [
            {
                "raw_items": [...],
                "stock_code": "019440",
                "year": "2023",
                "quarter": "Q1",
                "gics_sector": "Materials",
                "label": "1",  # 0=정상(healthy), 1=상폐(delisted)
            },
            ...
        ]
        force: True면 기존 파일 덮어쓰기

    Returns:
        업로드된 S3 key 리스트
    """
    if not raw_data_list:
        return []

    config = _get_s3_config(bucket, region)
    client = _get_s3_client(config)
    bucket_name = config["bucket"]
    bucket_checked = False

    uploaded: list[str] = []
    skipped = 0

    for entry in raw_data_list:
        # label: 0=정상(healthy), 1=상폐(delisted)
        label = str(entry.get("label", "")).strip()
        status_prefix = "delisted" if label == "1" else "healthy"
        s3_key = (
            f"{status_prefix}/{entry['gics_sector']}/"
            f"{entry['stock_code']}_{entry['year']}_{entry['quarter']}.json"
        )

        # ── 중복 체크: S3에 이미 존재하면 스킵 ──
        if not force and _check_s3_exists(client, bucket_name, s3_key):
            print(f"  ☁️  s3://{bucket_name}/{s3_key} → ⏭ 이미 존재 (SKIP)", file=sys.stderr)
            skipped += 1
            continue

        body = json.dumps(
            entry["raw_items"], ensure_ascii=False, indent=2
        ).encode("utf-8")

        try:
            client.put_object(
                Bucket=bucket_name, Key=s3_key, Body=body,
                ContentType="application/json; charset=utf-8",
            )
        except client.exceptions.NoSuchBucket:
            if not bucket_checked:
                _try_create_bucket(client, bucket_name, config["region"])
                bucket_checked = True
                client.put_object(
                    Bucket=bucket_name, Key=s3_key, Body=body,
                    ContentType="application/json; charset=utf-8",
                )
            else:
                raise

        s3_uri = f"s3://{bucket_name}/{s3_key}"
        uploaded.append(s3_uri)
        print(f"  ☁️  {s3_uri}", file=sys.stderr)

    print(
        f"\n✅ S3 업로드 완료: {len(uploaded)}개 업로드, {skipped}개 스킵"
        f" → s3://{config['bucket']}/",
        file=sys.stderr,
    )
    return uploaded


# ── 로그 유틸 ──────────────────────────────────────────────────
def _now_kst() -> datetime:
    """현재 시각을 KST로 반환."""
    return datetime.now(KST)


def build_run_log(
    member: str,
    run_id: str,
    started_at: datetime,
    finished_at: datetime,
    gics_sector: str,
    results: list[dict[str, Any]],
    tickers: list[str],
    years: list[int],
    quarters: list[str],
    note: str = "",
) -> dict[str, Any]:
    """업로드 실행 로그를 표준 dict로 구성."""
    success = sum(1 for r in results if r.get("status") == "SUCCESS")
    skipped = sum(1 for r in results if r.get("status") == "SKIPPED")
    failed = sum(1 for r in results if r.get("status") == "FAILED")

    return {
        "run_id": run_id,
        "member": member,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "input": {
            "gics_sector": gics_sector,
            "tickers": tickers,
            "years": years,
            "quarters": quarters,
        },
        "summary": {
            "total": len(results),
            "success": success,
            "skipped": skipped,
            "failed": failed,
        },
        "results": results,
        "note": note,
    }


def upload_run_log(
    log: dict[str, Any],
    started_at: datetime,
    bucket: str | None = None,
    region: str | None = None,
) -> str | None:
    """로그 JSON을 S3 log/ 디렉터리에 업로드.

    Returns:
        업로드된 S3 URI, 실패 시 None.
    """
    try:
        config = _get_s3_config(bucket, region)
        client = _get_s3_client(config)
    except Exception as e:
        print(f"  ⚠️  S3 로그 업로드 실패 (설정 오류): {e}", file=sys.stderr)
        return None

    timestamp = started_at.strftime("%Y%m%d_%H%M%S")
    member = log.get("member", "unknown")
    s3_key = f"log/{timestamp}_{member}.json"
    body = json.dumps(log, ensure_ascii=False, indent=2).encode("utf-8")

    try:
        client.put_object(
            Bucket=config["bucket"],
            Key=s3_key,
            Body=body,
            ContentType="application/json; charset=utf-8",
        )
    except client.exceptions.NoSuchBucket:
        try:
            _try_create_bucket(client, config["bucket"], config["region"])
            client.put_object(
                Bucket=config["bucket"],
                Key=s3_key,
                Body=body,
                ContentType="application/json; charset=utf-8",
            )
        except Exception as e:
            print(f"  ⚠️  S3 로그 업로드 실패 (버킷 생성/재시도): {e}", file=sys.stderr)
            return None
    except Exception as e:
        print(f"  ⚠️  S3 로그 업로드 실패: {e}", file=sys.stderr)
        return None

    uri = f"s3://{config['bucket']}/{s3_key}"
    return uri
