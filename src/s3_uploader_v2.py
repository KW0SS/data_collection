"""로컬 output CSV → S3 업로드 스크립트.

label=0 (정상기업) → s3://kw0ss-raw-data-s3/healthy/{섹터}/{파일명}
label=1 (상폐기업) → s3://kw0ss-raw-data-s3/delisted/{섹터}/{파일명}
작업 로그          → logs/{timestamp}_{member}.json (로컬 저장)
                   → s3://kw0ss-raw-data-s3/log/{timestamp}_{member}.json (확인 후 수동 업로드)

사용 예시
─────────
# 1) CSV 업로드 (로그는 logs/ 에 로컬 저장)
python s3_uploader_v2.py --member hyeonji

# 2) 로컬 로그 확인 후 S3에 업로드
python s3_uploader_v2.py --member hyeonji --upload-log logs/20260317_140000_hyeonji.json

# 3) 특정 섹터만
python s3_uploader_v2.py --member hyeonji --sector "Information Technology"

# 4) 이미 올라간 파일도 덮어쓰기
python s3_uploader_v2.py --member hyeonji --force
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

# 프로젝트 루트를 import 경로에 추가
sys.path.insert(0, str(Path(__file__).resolve().parent))

from s3_uploader import (
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
LOG_DIR    = Path("logs")
BUCKET     = "kw0ss-raw-data-s3"
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


def _save_log_local(log: dict, started_at: datetime, member: str) -> Path:
    """로그를 logs/{timestamp}_{member}.json 에 로컬 저장.

    input 안의 tickers/years/quarters 배열은 한 줄로,
    results 배열의 각 항목도 한 줄로 저장.
    """
    import re
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


def upload_local_csvs(
    output_dir: Path = OUTPUT_DIR,
    sector_filter: str | None = None,
    force: bool = False,
    member: str = "unknown",
    note: str = "",
) -> None:
    """로컬 CSV를 S3에 업로드하고 로그를 로컬에 저장."""
    config = _get_s3_config(bucket=BUCKET)
    client = _get_s3_client(config)
    bucket_name = config["bucket"]
    started_at = _now_kst()
    run_id = f"{member}-{started_at.strftime('%Y%m%d-%H%M%S')}"

    csv_files = list(output_dir.rglob("*.csv"))
    if not csv_files:
        print(f"업로드할 CSV 파일이 없습니다: {output_dir}")
        return

    uploaded = 0
    skipped  = 0
    failed   = 0
    run_log_results: list[dict] = []
    tickers_set: set[str] = set()
    years_set:   set[int]  = set()
    gics_set:    set[str]  = set()

    for csv_path in sorted(csv_files):
        sector = csv_path.parent.name
        ticker, year = _parse_ticker_year(csv_path)

        if sector_filter and sector != sector_filter:
            continue

        tickers_set.add(ticker)
        if year.isdigit():
            years_set.add(int(year))
        gics_set.add(sector)

        label, corp_name = _detect_label_and_name(csv_path)
        if label is None:
            print(f"  ⚠️  label 확인 불가, 스킵: {csv_path}")
            failed += 1
            run_log_results.append({
                "ticker": ticker,
                "company_name": corp_name,
                "year": int(year) if year.isdigit() else None,
                "quarter": None,
                "status": "FAILED",
                "s3_data_path": None,
                "record_count": 0,
                "error_code": "NO_LABEL",
                "error_message": "label 컬럼 확인 불가",
            })
            continue

        prefix = LABEL_PREFIX[label]
        s3_key = f"{prefix}/{sector}/{csv_path.name}"
        s3_uri = f"s3://{bucket_name}/{s3_key}"

        if not force and _check_s3_exists(client, bucket_name, s3_key):
            print(f"  ⏭  SKIP (이미 존재): {s3_uri}")
            skipped += 1
            run_log_results.append({
                "ticker": ticker,
                "company_name": corp_name,
                "year": int(year) if year.isdigit() else None,
                "quarter": None,
                "status": "SKIPPED",
                "s3_data_path": s3_uri,
                "record_count": 0,
                "error_code": None,
                "error_message": None,
            })
            continue

        try:
            client.upload_file(
                str(csv_path),
                bucket_name,
                s3_key,
                ExtraArgs={"ContentType": "text/csv; charset=utf-8"},
            )
            print(f"  ✅ UPLOAD: {s3_uri}")
            uploaded += 1
            run_log_results.append({
                "ticker": ticker,
                "company_name": corp_name,
                "year": int(year) if year.isdigit() else None,
                "quarter": None,
                "status": "SUCCESS",
                "s3_data_path": s3_uri,
                "record_count": csv_path.stat().st_size,
                "error_code": None,
                "error_message": None,
            })
        except Exception as e:
            print(f"  ❌ FAILED: {csv_path.name} → {e}")
            failed += 1
            run_log_results.append({
                "ticker": ticker,
                "company_name": corp_name,
                "year": int(year) if year.isdigit() else None,
                "quarter": None,
                "status": "FAILED",
                "s3_data_path": None,
                "record_count": 0,
                "error_code": "UPLOAD_ERROR",
                "error_message": str(e),
            })

    print(f"\n완료: {uploaded}개 업로드 / {skipped}개 스킵 / {failed}개 실패")

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
        quarters=[],
        note=note or "local CSV upload",
    )

    log_path = _save_log_local(log, started_at, member)
    print(f"\n📋 로그 로컬 저장 완료: {log_path}")
    print(f"   S3 업로드: python s3_uploader_v2.py --member {member} --upload-log {log_path}")


def upload_log_to_s3(log_path: Path) -> None:
    """로컬에 저장된 로그 JSON을 S3에 업로드."""
    if not log_path.exists():
        print(f"❌ 로그 파일을 찾을 수 없습니다: {log_path}")
        return

    log = json.loads(log_path.read_text(encoding="utf-8"))
    started_at = datetime.fromisoformat(log["started_at"]).astimezone(KST)
    uri = upload_run_log(log, started_at=started_at)
    if uri:
        print(f"✅ 로그 S3 업로드 완료: {uri}")


def main() -> None:
    parser = argparse.ArgumentParser(description="로컬 CSV → S3 업로드 (label 기준 분리)")
    parser.add_argument("--member", required=True, help="작업자 이름 (로그 기록용, 예: hyeonj)")
    parser.add_argument("--sector", help="특정 섹터만 업로드 (예: 'Information Technology')")
    parser.add_argument("--force", action="store_true", help="이미 존재하는 파일도 덮어쓰기")
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR), help="로컬 output 디렉터리 경로")
    parser.add_argument("--note", default="", help="로그 메모")
    parser.add_argument(
        "--upload-log", metavar="LOG_PATH",
        help="로컬 로그 JSON을 S3에 업로드 (경로 지정). 이 옵션만 있으면 CSV 업로드는 생략"
    )
    args = parser.parse_args()

    # 로그만 S3에 올리는 모드
    if args.upload_log:
        upload_log_to_s3(Path(args.upload_log))
        return

    upload_local_csvs(
        output_dir=Path(args.output_dir),
        sector_filter=args.sector,
        force=args.force,
        member=args.member,
        note=args.note,
    )


if __name__ == "__main__":
    main()