#!/usr/bin/env python3
"""상폐 예측 모델용 재무비율 데이터 수집 CLI.

사용 예시
─────────
# 1) 종목코드 직접 입력 → 019440_2022.csv, 019440_2023.csv 생성
python collect.py collect --stock-codes 019440 --years 2022 2023

# 2) 기업 목록 CSV로 배치 수집 (CSV에 start_year/end_year가 있으면 --years 무시)
python collect.py collect --companies data/input/companies.csv --years 2021 2022 2023

# 3) 저장 디렉터리 지정
python collect.py collect --stock-codes 019440 --years 2023 -o data/output/my_folder/

# 4) 원본 재무제표 JSON도 함께 저장
python collect.py collect --stock-codes 019440 --years 2023 --save-raw

# 5) S3에 GICS 섹터별로 원본 데이터 업로드 (기업 목록 CSV에 gics_sector 필요)
python collect.py collect --companies data/input/companies.csv --years 2023 --upload-s3

# 6) 이미 수집된 데이터는 자동 스킵 (중복 방지)
python collect.py collect --stock-codes 019440 --years 2022 2023

# 7) 기존 데이터 무시하고 전체 재수집
python collect.py collect --stock-codes 019440 --years 2023 --force

# 8) 기업 검색 (DART corp_code 조회)
python collect.py search --name 세아특수강
python collect.py search --stock-code 019440
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# 프로젝트 루트를 import 경로에 추가
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.dart_api import (
    DartApiError,
    REPORT_CODES,
    download_corp_codes,
    find_corp,
    get_api_key,
    CORP_XML_PATH,
)
from src.collector import (
    collect_batch,
    generate_missing_csv,
    generate_missing_legacy_csv,
    MISSING_CSV,
    MISSING_LEGACY_CSV,
)


def cmd_collect(args: argparse.Namespace) -> int:
    """재무비율 데이터 수집."""
    try:
        # --stock-codes가 지정되면 CSV 무시 (직접 입력 우선)
        companies_csv = None
        if not args.stock_codes and args.companies:
            companies_csv = Path(args.companies)

        saved_files = collect_batch(
            stock_codes=args.stock_codes,
            companies_csv=companies_csv,
            years=args.years,
            quarters=args.quarters,
            fs_div=args.fs_div,
            output_dir=Path(args.output_dir) if args.output_dir else None,
            api_key=args.api_key,
            delay=args.delay,
            save_raw=args.save_raw,
            upload_s3=args.upload_s3,
            s3_bucket=args.s3_bucket,
            s3_region=args.s3_region,
            force=args.force,
            workers=args.workers,
        )
        print(f"결과 파일 ({len(saved_files)}개):")
        for f in saved_files:
            print(f"  {f}")
        return 0
    except (DartApiError, FileNotFoundError, ValueError) as e:
        print(f"오류: {e}", file=sys.stderr)
        return 1


def cmd_search(args: argparse.Namespace) -> int:
    """DART 기업코드 검색."""
    try:
        api_key = get_api_key(args.api_key)

        if args.refresh or not CORP_XML_PATH.exists():
            print("기업코드 XML 다운로드 중...", file=sys.stderr)
            download_corp_codes(api_key)

        results = find_corp(
            corp_name=args.name,
            stock_code=args.stock_code,
            limit=args.limit,
        )

        if not results:
            print("검색 결과가 없습니다.")
            return 0

        print(f"{'DART코드':<12} {'기업명':<20} {'종목코드':<10} {'수정일'}")
        print("-" * 60)
        for r in results:
            print(
                f"{r['corp_code']:<12} {r['corp_name']:<20} "
                f"{r['stock_code']:<10} {r['modify_date']}"
            )
        return 0
    except DartApiError as e:
        print(f"오류: {e}", file=sys.stderr)
        return 1


def cmd_check_missing(args: argparse.Namespace) -> int:
    """2015년 이전 누락 기업 목록 CSV 생성."""
    try:
        output_dir = Path(args.output_dir) if args.output_dir else None
        csv_path = generate_missing_legacy_csv(output_dir=output_dir)
        print(f"\n생성 완료: {csv_path}")
        return 0
    except (FileNotFoundError, ValueError) as e:
        print(f"오류: {e}", file=sys.stderr)
        return 1


def cmd_collect_legacy(args: argparse.Namespace) -> int:
    """누락된 2015년 이전 데이터 수집.

    1) --check 옵션이면 누락 목록 CSV를 먼저 생성
    2) 해당 CSV를 기반으로 collect_batch 실행
    """
    try:
        missing_csv = Path(args.missing_csv) if args.missing_csv else MISSING_LEGACY_CSV

        # --check: 누락 목록 갱신 후 수집
        if args.check or not missing_csv.exists():
            output_dir = Path(args.output_dir) if args.output_dir else None
            missing_csv = generate_missing_legacy_csv(output_dir=output_dir)
            print("", file=sys.stderr)

        saved_files = collect_batch(
            companies_csv=missing_csv,
            fs_div=args.fs_div,
            output_dir=Path(args.output_dir) if args.output_dir else None,
            api_key=args.api_key,
            delay=args.delay,
            save_raw=args.save_raw,
            upload_s3=args.upload_s3,
            s3_bucket=args.s3_bucket,
            s3_region=args.s3_region,
            force=args.force,
            workers=args.workers,
        )
        print(f"결과 파일 ({len(saved_files)}개):")
        for f in saved_files:
            print(f"  {f}")
        return 0
    except (DartApiError, FileNotFoundError, ValueError) as e:
        print(f"오류: {e}", file=sys.stderr)
        return 1


def cmd_retry(args: argparse.Namespace) -> int:
    """누락 데이터 탐지 및 재수집.

    1) companies_collected.csv vs output 디렉터리를 비교하여 누락 탐지
    2) --stock-codes 지정 시 해당 종목만 필터링
    3) companies_missing.csv 생성 → collect_batch 실행
    """
    try:
        import csv as _csv

        output_dir = Path(args.output_dir) if args.output_dir else None

        # 누락 목록 생성
        missing_csv = generate_missing_csv(output_dir=output_dir)
        print("", file=sys.stderr)

        # --stock-codes 필터링
        if args.stock_codes:
            codes = set(args.stock_codes)
            rows = []
            with open(missing_csv, newline="", encoding="utf-8-sig") as f:
                reader = _csv.DictReader(f)
                fieldnames = reader.fieldnames
                for row in reader:
                    if row["stock_code"] in codes:
                        rows.append(row)

            if not rows:
                print(f"지정한 종목코드에 누락 데이터가 없습니다: {args.stock_codes}")
                return 0

            # 필터링된 목록으로 덮어쓰기
            with open(missing_csv, "w", newline="", encoding="utf-8-sig") as f:
                writer = _csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)

            total = sum(int(r["end_year"]) - int(r["start_year"]) + 1 for r in rows)
            print(f"  → {len(rows)}개 구간, {total}건으로 필터링됨\n", file=sys.stderr)

        if args.check_only:
            print(f"누락 목록: {missing_csv}")
            return 0

        # 누락 데이터 수집
        saved_files = collect_batch(
            companies_csv=missing_csv,
            fs_div=args.fs_div,
            output_dir=output_dir,
            api_key=args.api_key,
            delay=args.delay,
            save_raw=args.save_raw,
            upload_s3=args.upload_s3,
            s3_bucket=args.s3_bucket,
            s3_region=args.s3_region,
            force=True,  # 누락 데이터이므로 항상 강제 수집
            workers=args.workers,
        )
        print(f"결과 파일 ({len(saved_files)}개):")
        for f in saved_files:
            print(f"  {f}")
        return 0
    except (DartApiError, FileNotFoundError, ValueError) as e:
        print(f"오류: {e}", file=sys.stderr)
        return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="상폐 예측 모델용 코스닥 재무비율 데이터 수집 도구",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--api-key", help="DART API 키 (또는 환경변수 DART_API_KEY)")

    sub = parser.add_subparsers(dest="command")

    # ── collect (기본 커맨드) ──
    collect_p = sub.add_parser("collect", help="재무비율 데이터 수집")
    collect_p.add_argument(
        "--stock-codes", nargs="+",
        help="종목코드 목록 (예: 005930 035720)"
    )
    collect_p.add_argument(
        "--companies",
        default="data/input/companies_template.csv",
        help="기업 목록 CSV 파일 경로 (기본: data/input/companies_template.csv)"
    )
    collect_p.add_argument(
        "--years", nargs="+", default=["2023"],
        help="수집 연도 (기본: 2023)"
    )
    collect_p.add_argument(
        "--quarters", nargs="+", default=None,
        choices=list(REPORT_CODES.keys()),
        help="수집 분기 (기본: 전체). Q1, H1, Q3, ANNUAL"
    )
    collect_p.add_argument(
        "--fs-div", default="CFS", choices=["CFS", "OFS"],
        help="CFS=연결재무제표, OFS=별도재무제표 (기본: CFS)"
    )
    collect_p.add_argument(
        "--output-dir", "-o",
        help="결과 CSV 저장 디렉터리 (기본: data/output/)"
    )
    collect_p.add_argument(
        "--delay", type=float, default=0.5,
        help="API 호출 간 대기(초). OpenDART 분당 제한 방지 (기본: 0.5)"
    )
    collect_p.add_argument(
        "--save-raw", action="store_true",
        help="원본 재무제표 JSON을 data/raw/에 저장"
    )
    collect_p.add_argument(
        "--upload-s3", action="store_true",
        help="원본 재무제표 JSON을 S3에 업로드 (GICS 섹터별 디렉터리)"
    )
    collect_p.add_argument(
        "--s3-bucket",
        help="S3 버킷 이름 (없으면 .env의 S3_BUCKET_NAME 사용)"
    )
    collect_p.add_argument(
        "--s3-region",
        help="AWS 리전 (없으면 .env의 S3_REGION 또는 ap-northeast-2)"
    )
    collect_p.add_argument(
        "--force", action="store_true",
        help="중복 체크를 무시하고 전체 재수집 (기존 데이터 덮어쓰기)"
    )
    collect_p.add_argument(
        "--workers", type=int, default=1,
        help="병렬 수집 워커 수 (기본: 1=순차, 5 권장)"
    )
    collect_p.set_defaults(func=cmd_collect)

    # ── search ──
    search_p = sub.add_parser("search", help="DART 기업코드 검색")
    search_p.add_argument("--name", help="기업명 검색어")
    search_p.add_argument("--stock-code", help="종목코드")
    search_p.add_argument("--refresh", action="store_true", help="기업코드 XML 새로 다운로드")
    search_p.add_argument("--limit", type=int, default=20, help="최대 검색 결과 수")
    search_p.set_defaults(func=cmd_search)

    # ── check-missing ──
    check_p = sub.add_parser(
        "check-missing",
        help="2015년 이전 누락 기업 목록 CSV 생성",
    )
    check_p.add_argument(
        "--output-dir", "-o",
        help="output 디렉터리 경로 (기본: data/output/)",
    )
    check_p.set_defaults(func=cmd_check_missing)

    # ── collect-legacy ──
    legacy_p = sub.add_parser(
        "collect-legacy",
        help="2015년 이전 누락 데이터 수집 (dart-fss)",
    )
    legacy_p.add_argument(
        "--missing-csv",
        help="누락 기업 목록 CSV 경로 (기본: data/input/companies_missing_legacy.csv)",
    )
    legacy_p.add_argument(
        "--check", action="store_true",
        help="수집 전 누락 목록 CSV를 새로 생성",
    )
    legacy_p.add_argument(
        "--fs-div", default="CFS", choices=["CFS", "OFS"],
        help="CFS=연결재무제표, OFS=별도재무제표 (기본: CFS)",
    )
    legacy_p.add_argument(
        "--output-dir", "-o",
        help="결과 CSV 저장 디렉터리 (기본: data/output/)",
    )
    legacy_p.add_argument(
        "--delay", type=float, default=0.5,
        help="API 호출 간 대기(초) (기본: 0.5)",
    )
    legacy_p.add_argument(
        "--save-raw", action="store_true",
        help="원본 재무제표 JSON을 data/raw/에 저장",
    )
    legacy_p.add_argument(
        "--upload-s3", action="store_true",
        help="원본 재무제표 JSON을 S3에 업로드",
    )
    legacy_p.add_argument("--s3-bucket", help="S3 버킷 이름")
    legacy_p.add_argument("--s3-region", help="AWS 리전")
    legacy_p.add_argument(
        "--force", action="store_true",
        help="중복 체크를 무시하고 전체 재수집",
    )
    legacy_p.add_argument(
        "--workers", type=int, default=1,
        help="병렬 수집 워커 수 (기본: 1=순차, 5 권장)",
    )
    legacy_p.set_defaults(func=cmd_collect_legacy)

    # ── retry (누락 재수집) ──
    retry_p = sub.add_parser(
        "retry",
        help="누락 데이터 탐지 및 재수집 (legacy + 2015 이후 모두)",
    )
    retry_p.add_argument(
        "--stock-codes", nargs="+",
        help="재수집할 종목코드 목록 (미지정 시 전체 누락 대상)",
    )
    retry_p.add_argument(
        "--check-only", action="store_true",
        help="누락 목록만 생성하고 수집은 하지 않음",
    )
    retry_p.add_argument(
        "--fs-div", default="CFS", choices=["CFS", "OFS"],
        help="CFS=연결재무제표, OFS=별도재무제표 (기본: CFS)",
    )
    retry_p.add_argument(
        "--output-dir", "-o",
        help="output 디렉터리 경로 (기본: data/output/)",
    )
    retry_p.add_argument(
        "--delay", type=float, default=0.5,
        help="API 호출 간 대기(초) (기본: 0.5)",
    )
    retry_p.add_argument(
        "--save-raw", action="store_true",
        help="원본 재무제표 JSON을 data/raw/에 저장",
    )
    retry_p.add_argument(
        "--upload-s3", action="store_true",
        help="원본 재무제표 JSON을 S3에 업로드",
    )
    retry_p.add_argument("--s3-bucket", help="S3 버킷 이름")
    retry_p.add_argument("--s3-region", help="AWS 리전")
    retry_p.add_argument(
        "--workers", type=int, default=1,
        help="병렬 수집 워커 수 (기본: 1=순차, 5 권장)",
    )
    retry_p.set_defaults(func=cmd_retry)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if not hasattr(args, "func"):
        # 서브커맨드 없이 호출 시 → collect로 간주 (stock_codes 직접 확인)
        parser.print_help()
        return 0

    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
