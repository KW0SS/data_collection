"""로컬 raw data JSON → S3 업로드 스크립트.

data/output/ CSV에서 label을 읽어 S3 경로를 결정하고,
data/raw/ 의 원본 재무제표 JSON을 S3에 업로드한다.

S3 구조
────────
label=0 (정상기업) → s3://{S3_BUCKET_NAME}/healthy/{섹터}/{ticker}_{year}_{quarter}.json
label=1 (상폐기업) → s3://{S3_BUCKET_NAME}/delisted/{섹터}/{ticker}_{year}_{quarter}.json
작업 로그          → logs/{timestamp}_{member}.json (로컬 저장)
                   → s3://{S3_BUCKET_NAME}/log/{timestamp}_{member}.json (확인 후 수동 업로드)

사용 예시
─────────
# 1) raw data를 S3에 업로드 (로그는 logs/ 에 로컬 저장)
python -m src.s3_uploader_v2 --member hyeonji

# 2) 로컬 로그 확인 후 S3에 업로드
python -m src.s3_uploader_v2 --member hyeonji --upload-log logs/20260317_140000_hyeonji.json

# 3) 특정 섹터만
python -m src.s3_uploader_v2 --member hyeonji --sector "Information Technology"

# 4) 이미 올라간 파일도 덮어쓰기
python -m src.s3_uploader_v2 --member hyeonji --force
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path

from .s3_uploader import (
    _get_s3_config,
    _get_s3_client,
    _check_s3_exists,
    build_run_log,
    upload_run_log,
    _now_kst,
)

# ── 경로 설정 ──────────────────────────────────────────────────
DATA_DIR   = Path("data")
OUTPUT_DIR = DATA_DIR / "output"
RAW_DIR    = DATA_DIR / "raw"
LOG_DIR    = Path("logs")
KST        = timezone(timedelta(hours=9))

LABEL_PREFIX = {
    "0": "healthy",
    "1": "delisted",
}


def _detect_label_and_name(csv_path: Path) -> tuple[str | None, str]:
    """CSV 첫 번째 데이터 행에서 label과 corp_name을 읽어 반환."""
    try:
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                label = (row.get("label") or "").strip()
                corp_name = (row.get("corp_name") or "").strip()
                if label in LABEL_PREFIX:
                    return label, corp_name
    except Exception:
        pass
    return None, ""


def _parse_ticker_year(csv_path: Path) -> tuple[str, str]:
    """파일명 {ticker}_{year}.csv 에서 ticker, year 파싱."""
    stem = csv_path.stem  # 예: 005930_2023
    parts = stem.rsplit("_", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return stem, ""


def _find_raw_jsons(ticker: str, year: str, raw_dir: Path = RAW_DIR) -> list[Path]:
    """지정된 raw_dir 에서 해당 ticker/year의 raw JSON 파일 목록 반환.

    파일명 패턴: {ticker}_{year}_{quarter}_{fs_div}.json
    예: 006580_2015_Q1_CFS.json, 006580_2015_ANNUAL_OFS.json
    """
    if not raw_dir.exists():
        return []
    pattern = f"{ticker}_{year}_*.json"
    return sorted(raw_dir.glob(pattern))


def _parse_raw_filename(raw_path: Path) -> tuple[str, str, str, str]:
    """raw JSON 파일명에서 ticker, year, quarter, fs_div 파싱.

    {ticker}_{year}_{quarter}_{fs_div}.json
    """
    stem = raw_path.stem
    parts = stem.split("_")
    if len(parts) >= 4:
        return parts[0], parts[1], parts[2], parts[3]
    return stem, "", "", ""


def _save_log_local(log: dict, started_at: datetime, member: str) -> Path:
    """로그를 logs/{timestamp}_{member}.json 에 로컬 저장."""
    LOG_DIR.mkdir(exist_ok=True)
    timestamp = started_at.strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"{timestamp}_{member}.json"

    text = json.dumps(log, ensure_ascii=False, indent=2)

    def _compress_array(m):
        key = m.group(1)
        arr = json.loads(m.group(2))
        return f'"{key}": {json.dumps(arr, ensure_ascii=False)}'

    text = re.sub(
        r'"(tickers|years|quarters)":\s*(\[.*?\])',
        _compress_array,
        text,
        flags=re.DOTALL,
    )

    log_path.write_text(text, encoding="utf-8")
    return log_path


def upload_raw_data(
    output_dir: Path = OUTPUT_DIR,
    raw_dir: Path = RAW_DIR,
    sector_filter: str | None = None,
    force: bool = False,
    member: str = "unknown",
    note: str = "",
    log_bucket: str | None = None,
) -> None:
    """data/output/ CSV를 스캔하여 label을 결정하고,
    대응하는 data/raw/ JSON을 S3에 업로드."""
    config = _get_s3_config()
    client = _get_s3_client(config)
    bucket_name = config["bucket"]
    started_at = _now_kst()
    run_id = f"{member}-{started_at.strftime('%Y%m%d-%H%M%S')}"

    csv_files = list(output_dir.rglob("*.csv"))
    if not csv_files:
        print(f"output CSV 파일이 없습니다: {output_dir}")
        return

    if not raw_dir.exists():
        print(f"raw data 디렉터리가 없습니다: {raw_dir}")
        print("collect.py 실행 시 --save-raw 옵션으로 raw data를 저장하세요.")
        return

    uploaded = 0
    skipped  = 0
    failed   = 0
    no_raw   = 0
    run_log_results: list[dict] = []
    tickers_set: set[str] = set()
    years_set:   set[int] = set()
    quarters_set: set[str] = set()
    gics_set:    set[str] = set()

    for csv_path in sorted(csv_files):
        sector = csv_path.parent.name
        ticker, year = _parse_ticker_year(csv_path)

        if sector_filter and sector != sector_filter:
            continue

        label, corp_name = _detect_label_and_name(csv_path)
        if label is None:
            continue

        # 대응하는 raw JSON 찾기
        raw_jsons = _find_raw_jsons(ticker, year, raw_dir=raw_dir)
        if not raw_jsons:
            no_raw += 1
            continue

        prefix = LABEL_PREFIX[label]
        tickers_set.add(ticker)
        if year.isdigit():
            years_set.add(int(year))
        gics_set.add(sector)

        for raw_path in raw_jsons:
            _, _, quarter, _ = _parse_raw_filename(raw_path)
            quarters_set.add(quarter)

            # S3 key: {label}/{sector}/{ticker}_{year}_{quarter}.json
            s3_key = f"{prefix}/{sector}/{ticker}_{year}_{quarter}.json"
            s3_uri = f"s3://{bucket_name}/{s3_key}"

            if not force and _check_s3_exists(client, bucket_name, s3_key):
                print(f"  ⏭  SKIP (이미 존재): {s3_uri}")
                skipped += 1
                run_log_results.append({
                    "ticker": ticker,
                    "company_name": corp_name,
                    "year": int(year) if year.isdigit() else None,
                    "quarter": quarter,
                    "status": "SKIPPED",
                    "s3_data_path": s3_uri,
                    "record_count": 0,
                    "error_code": None,
                    "error_message": None,
                })
                continue

            try:
                raw_body = raw_path.read_bytes()
                client.put_object(
                    Bucket=bucket_name,
                    Key=s3_key,
                    Body=raw_body,
                    ContentType="application/json; charset=utf-8",
                )
                print(f"  ✅ UPLOAD: {s3_uri}")
                uploaded += 1
                run_log_results.append({
                    "ticker": ticker,
                    "company_name": corp_name,
                    "year": int(year) if year.isdigit() else None,
                    "quarter": quarter,
                    "status": "SUCCESS",
                    "s3_data_path": s3_uri,
                    "record_count": len(json.loads(raw_body)),
                    "error_code": None,
                    "error_message": None,
                })
            except Exception as e:
                print(f"  ❌ FAILED: {raw_path.name} → {e}")
                failed += 1
                run_log_results.append({
                    "ticker": ticker,
                    "company_name": corp_name,
                    "year": int(year) if year.isdigit() else None,
                    "quarter": quarter,
                    "status": "FAILED",
                    "s3_data_path": None,
                    "record_count": 0,
                    "error_code": "UPLOAD_ERROR",
                    "error_message": str(e),
                })

    print(f"\n완료: {uploaded}개 업로드 / {skipped}개 스킵 / {failed}개 실패 / {no_raw}개 raw 없음")

    # ── 로그 로컬 저장 ────────────────────────────────────────
    if not run_log_results:
        return

    finished_at = _now_kst()
    log = build_run_log(
        member=member,
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
        gics_sector=", ".join(sorted(gics_set)),
        results=run_log_results,
        tickers=sorted(tickers_set),
        years=sorted(years_set),
        quarters=sorted(quarters_set),
        note=note or "raw data upload",
    )

    log_path = _save_log_local(log, started_at, member)
    print(f"\n📋 로그 로컬 저장 완료: {log_path}")
    upload_log_cmd = f"python -m src.s3_uploader_v2 --member {member} --upload-log {log_path}"
    if log_bucket:
        upload_log_cmd += f" --log-bucket {log_bucket}"
    print(f"   S3 업로드: {upload_log_cmd}")


def upload_log_to_s3(log_path: Path, log_bucket: str | None = None) -> None:
    """로컬에 저장된 로그 JSON을 S3에 업로드."""
    if not log_path.exists():
        print(f"❌ 로그 파일을 찾을 수 없습니다: {log_path}")
        return

    log = json.loads(log_path.read_text(encoding="utf-8"))
    started_at = datetime.fromisoformat(log["started_at"]).astimezone(KST)
    uri = upload_run_log(log, started_at=started_at, bucket=log_bucket)
    if uri:
        print(f"✅ 로그 S3 업로드 완료: {uri}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="로컬 raw data JSON → S3 업로드 (label 기준 경로 분리)"
    )
    parser.add_argument("--member", required=True, help="작업자 이름 (로그 기록용)")
    parser.add_argument("--sector", help="특정 섹터만 업로드 (예: 'Materials')")
    parser.add_argument("--force", action="store_true", help="이미 존재하는 파일도 덮어쓰기")
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR), help="로컬 output CSV 디렉터리")
    parser.add_argument("--raw-dir", default=str(RAW_DIR), help="로컬 raw JSON 디렉터리")
    parser.add_argument(
        "--log-bucket",
        default=None,
        help="로그 JSON 업로드 버킷 (기본: .env의 S3_BUCKET_NAME)",
    )
    parser.add_argument("--note", default="", help="로그 메모")
    parser.add_argument(
        "--upload-log", metavar="LOG_PATH",
        help="로컬 로그 JSON을 S3에 업로드. 이 옵션만 있으면 raw data 업로드는 생략"
    )
    args = parser.parse_args()

    if args.upload_log:
        upload_log_to_s3(Path(args.upload_log), log_bucket=args.log_bucket)
        return

    upload_raw_data(
        output_dir=Path(args.output_dir),
        raw_dir=Path(args.raw_dir),
        sector_filter=args.sector,
        force=args.force,
        member=args.member,
        note=args.note,
        log_bucket=args.log_bucket,
    )


if __name__ == "__main__":
    main()
