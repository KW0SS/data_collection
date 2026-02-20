#!/usr/bin/env python3
"""상폐 예측 모델용 재무비율 데이터 수집 CLI.

사용 예시
─────────
# 1) 종목코드 직접 입력 → 019440_2022.csv, 019440_2023.csv 생성
python collect.py collect --stock-codes 019440 --years 2022 2023

# 2) 기업 목록 CSV로 배치 수집
python collect.py collect --companies data/input/companies.csv --years 2021 2022 2023

# 3) 저장 디렉터리 지정
python collect.py collect --stock-codes 019440 --years 2023 -o data/output/my_folder/

# 4) 원본 재무제표 JSON도 함께 저장
python collect.py collect --stock-codes 019440 --years 2023 --save-raw

# 5) 기업 검색 (DART corp_code 조회)
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
from src.collector import collect_batch


def cmd_collect(args: argparse.Namespace) -> int:
    """재무비율 데이터 수집."""
    try:
        saved_files = collect_batch(
            stock_codes=args.stock_codes,
            companies_csv=Path(args.companies) if args.companies else None,
            years=args.years,
            quarters=args.quarters,
            fs_div=args.fs_div,
            output_dir=Path(args.output_dir) if args.output_dir else None,
            api_key=args.api_key,
            delay=args.delay,
            save_raw=args.save_raw,
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
        help="기업 목록 CSV 파일 경로 (예: data/input/companies.csv)"
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
    collect_p.set_defaults(func=cmd_collect)

    # ── search ──
    search_p = sub.add_parser("search", help="DART 기업코드 검색")
    search_p.add_argument("--name", help="기업명 검색어")
    search_p.add_argument("--stock-code", help="종목코드")
    search_p.add_argument("--refresh", action="store_true", help="기업코드 XML 새로 다운로드")
    search_p.add_argument("--limit", type=int, default=20, help="최대 검색 결과 수")
    search_p.set_defaults(func=cmd_search)

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
