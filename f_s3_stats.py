"""S3 재무제표 집계 스크립트.

S3 버킷에 저장된 재무제표 JSON 파일을 집계하여
정상(healthy)/상폐(delisted) × GICS 섹터별 현황을 출력합니다.

사용법:
    python f_s3_stats.py
    python f_s3_stats.py --bucket my-bucket --region ap-northeast-2
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from pathlib import Path

try:
    import boto3
except ImportError:
    print("boto3가 설치되어 있지 않습니다: pip install boto3")
    sys.exit(1)


# ── .env 로드 ─────────────────────────────────────────────────
def _load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    env_path = Path(__file__).resolve().parent / ".env"
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


def get_s3_client(bucket: str | None, region: str | None):
    env = _load_env()
    access_key = os.getenv("S3_ACCESS_KEY") or env.get("S3_ACCESS_KEY")
    secret_key = os.getenv("S3_PRIVATE_KEY") or env.get("S3_PRIVATE_KEY")
    bucket_name = bucket or os.getenv("S3_BUCKET_NAME") or env.get("S3_BUCKET_NAME")
    region_name = region or os.getenv("S3_REGION") or env.get("S3_REGION", "ap-northeast-2")

    if not access_key or not secret_key:
        print("S3 인증 키가 없습니다. .env에 S3_ACCESS_KEY, S3_PRIVATE_KEY를 설정하세요.")
        sys.exit(1)
    if not bucket_name:
        print("S3 버킷 이름이 없습니다. --bucket 옵션이나 .env에 S3_BUCKET_NAME을 설정하세요.")
        sys.exit(1)

    client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region_name,
    )
    return client, bucket_name


def list_all_keys(client, bucket: str) -> list[str]:
    """S3 버킷의 모든 키를 paginator로 조회."""
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def parse_key(key: str) -> dict[str, str] | None:
    """S3 키를 파싱하여 status, sector, stock_code, year, quarter 추출.

    예: healthy/Materials/005930_2023_Q1.json
        delisted/Industrials/019440_2012_ANNUAL.json
    """
    parts = key.split("/")
    if len(parts) != 3:
        return None

    status, sector, filename = parts
    if status not in ("healthy", "delisted"):
        return None
    if not filename.endswith(".json"):
        return None

    stem = filename[:-5]  # .json 제거
    tokens = stem.split("_")
    if len(tokens) < 3:
        return None

    return {
        "status": status,
        "sector": sector,
        "stock_code": tokens[0],
        "year": tokens[1],
        "quarter": "_".join(tokens[2:]),
    }


def print_stats(keys: list[str]) -> None:
    parsed = [parse_key(k) for k in keys]
    parsed = [p for p in parsed if p is not None]

    if not parsed:
        print("집계 가능한 파일이 없습니다.")
        return

    # ── 전체 요약 ────────────────────────────────────────────
    status_count: dict[str, int] = defaultdict(int)
    status_stocks: dict[str, set[str]] = defaultdict(set)
    sector_count: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    sector_stocks: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    year_count: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for p in parsed:
        st, sec, sc, yr = p["status"], p["sector"], p["stock_code"], p["year"]
        status_count[st] += 1
        status_stocks[st].add(sc)
        sector_count[st][sec] += 1
        sector_stocks[st][sec].add(sc)
        year_count[st][yr] += 1

    total = len(parsed)

    print("=" * 60)
    print(f"  S3 재무제표 집계 현황 (총 {total}건)")
    print("=" * 60)

    # ── 상태별 요약 ──────────────────────────────────────────
    print("\n📊 상태별 요약")
    print("-" * 40)
    print(f"  {'상태':<12} {'파일 수':>8} {'기업 수':>8}")
    print("-" * 40)
    for st in ["healthy", "delisted"]:
        cnt = status_count.get(st, 0)
        stocks = len(status_stocks.get(st, set()))
        label = "정상" if st == "healthy" else "상폐"
        print(f"  {label} ({st}){'':<1} {cnt:>6}건 {stocks:>6}개")
    print("-" * 40)
    print(f"  {'합계':<12} {total:>6}건 {len({p['stock_code'] for p in parsed}):>6}개")

    # ── 상태 × 섹터 상세 ─────────────────────────────────────
    all_sectors = sorted({p["sector"] for p in parsed})

    for st in ["healthy", "delisted"]:
        label = "정상 (healthy)" if st == "healthy" else "상폐 (delisted)"
        sectors = sector_count.get(st, {})
        if not sectors:
            continue

        print(f"\n📁 {label} — 섹터별")
        print("-" * 52)
        print(f"  {'섹터':<25} {'파일 수':>8} {'기업 수':>8}")
        print("-" * 52)
        sub_total = 0
        sub_stocks = 0
        for sec in all_sectors:
            cnt = sectors.get(sec, 0)
            if cnt == 0:
                continue
            stocks = len(sector_stocks[st].get(sec, set()))
            print(f"  {sec:<25} {cnt:>6}건 {stocks:>6}개")
            sub_total += cnt
            sub_stocks += stocks
        print("-" * 52)
        print(f"  {'소계':<25} {sub_total:>6}건 {sub_stocks:>6}개")

    # ── 연도 분포 ────────────────────────────────────────────
    print("\n📅 연도별 분포")
    all_years = sorted({p["year"] for p in parsed})
    print("-" * 44)
    print(f"  {'연도':<8} {'정상':>8} {'상폐':>8} {'합계':>8}")
    print("-" * 44)
    for yr in all_years:
        h = year_count.get("healthy", {}).get(yr, 0)
        d = year_count.get("delisted", {}).get(yr, 0)
        print(f"  {yr:<8} {h:>6}건 {d:>6}건 {h + d:>6}건")
    print("-" * 44)


def main():
    parser = argparse.ArgumentParser(description="S3 재무제표 집계 현황")
    parser.add_argument("--bucket", help="S3 버킷 이름 (기본: .env)")
    parser.add_argument("--region", help="AWS 리전 (기본: .env)")
    args = parser.parse_args()

    client, bucket = get_s3_client(args.bucket, args.region)

    print(f"S3 버킷 조회 중: {bucket} ...\n")
    keys = list_all_keys(client, bucket)
    print(f"전체 오브젝트: {len(keys)}개\n")

    print_stats(keys)


if __name__ == "__main__":
    main()
