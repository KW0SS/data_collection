#!/usr/bin/env python3
"""통합 수집 파이프라인.

엑셀 파일을 data/etc/에 배치한 뒤, 섹터와 기업 상태를 지정하면
기업 목록 생성 → 재무제표 수집 → S3 업로드를 한 번에 수행한다.

사전 준비
─────────
1. data/etc/상장폐지현황.xlsx     (KRX 상장폐지현황)
2. data/etc/corp_codes.xml       (DART 고유번호 전체 — 상폐 기업 사용 시)
3. data/input/krx_all_companies.xlsx  (KRX 전종목 — 정상 기업 사용 시)

사용 예시
─────────
# 정상 기업만, IT + Communication Services 섹터
python3 run_pipeline.py --status normal \
    --sectors "Information Technology" "Communication Services" \
    --member hann

# 상폐 기업만, Consumer Discretionary 섹터
python3 run_pipeline.py --status delisted \
    --sectors "Consumer Discretionary" \
    --member hann

# 정상 + 상폐 모두, 전체 섹터
python3 run_pipeline.py --status all --member hann

# S3 업로드 건너뛰기 (수집만)
python3 run_pipeline.py --status normal \
    --sectors "Information Technology" \
    --member hann --skip-s3

# dry-run: 수집 대상 기업 목록만 확인
python3 run_pipeline.py --status delisted \
    --sectors "Information Technology" \
    --member hann --dry-run

# 2015년 이전 데이터도 포함 (dart-fss 사용)
python3 run_pipeline.py --status delisted \
    --sectors "Materials" \
    --member hann --start-year 2002 --skip-s3

# 정상 기업 2010~2025 전체 수집
python3 run_pipeline.py --status normal \
    --sectors "Information Technology" \
    --member hann --start-year 2010 --end-year 2025
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import pandas as pd

# ── 경로 설정 ──────────────────────────────────────────────────
DATA_DIR = Path("data")
ETC_DIR = DATA_DIR / "etc"
INPUT_DIR = DATA_DIR / "input"

KRX_XLSX = INPUT_DIR / "krx_all_companies.xlsx"
DELISTED_XLSX = ETC_DIR / "상장폐지현황.xlsx"
CORP_CODES_XML = ETC_DIR / "corp_codes.xml"
INDUTY_CACHE = ETC_DIR / "delisted_induty_codes.csv"

# 파이프라인이 생성하는 임시 기업 목록
PIPELINE_CSV = INPUT_DIR / "pipeline_companies.csv"

COLUMNS = ["stock_code", "corp_name", "label", "gics_sector", "start_year", "end_year"]
DEFAULT_START_YEAR = 2015
DEFAULT_END_YEAR = 2025
LEGACY_CUTOFF_YEAR = 2015  # 이 연도 미만은 dart-fss 기반 legacy 수집

VALID_GICS_SECTORS = [
    "Information Technology",
    "Communication Services",
    "Consumer Discretionary",
    "Consumer Staples",
    "Health Care",
    "Industrials",
    "Materials",
    "Energy",
    "Utilities",
    "Financials",
    "Real Estate",
]

SECTOR_ALIASES = {
    "it": "Information Technology",
    "tech": "Information Technology",
    "communication": "Communication Services",
    "consumer discretionary": "Consumer Discretionary",
    "consumer staples": "Consumer Staples",
    "healthcare": "Health Care",
    "health care": "Health Care",
    "industrial": "Industrials",
    "materials": "Materials",
    "energy": "Energy",
    "utilities": "Utilities",
    "financial": "Financials",
    "financials": "Financials",
    "real estate": "Real Estate",
    "realestate": "Real Estate",
}

# KRX "업종" 문자열 기준 정상기업 GICS 매핑.
SECTOR_KEYWORDS_NORMAL: dict[str, list[str]] = {
    "Information Technology": [
        "반도체", "전자부품", "디스플레이", "컴퓨터", "소프트웨어", "프로그래밍",
        "시스템 통합", "정보 서비스", "자료처리", "호스팅", "포털", "통신 및 방송 장비",
        "영상 및 음향기기", "광학기기",
    ],
    "Communication Services": [
        "전기 통신", "통신", "텔레비전 방송", "방송", "영화", "비디오", "게임",
        "광고", "출판", "오디오물", "창작", "엔터테인먼트", "영상·오디오물", "미디어",
        "콘텐츠",
    ],
    "Health Care": [
        "의약", "제약", "의료", "바이오", "치과", "진단",
    ],
    "Financials": [
        "금융", "은행", "보험", "증권", "여신", "리스", "신탁",
    ],
    "Real Estate": [
        "부동산", "리츠",
    ],
    "Utilities": [
        "전력", "수도", "가스 공급", "증기", "폐기물", "하수",
    ],
    "Energy": [
        "원유", "석유", "가스", "연료", "코크스",
    ],
    "Materials": [
        "화학", "비금속", "금속", "철강", "비철", "시멘트", "유리", "종이", "펄프",
        "목재", "고무", "플라스틱", "도료", "염료", "비료", "광업",
    ],
    "Consumer Staples": [
        "식품", "음료", "담배", "농업", "축산", "수산", "생활용품", "가정용품",
    ],
    "Consumer Discretionary": [
        "의복", "의류", "가죽", "화장품", "가구", "숙박", "음식점", "소매", "백화점",
        "무점포", "유원지", "신발", "자동차",
    ],
    "Industrials": [
        "기계", "장비", "조선", "항공", "운송", "물류", "건설", "토목", "상사업",
        "도매", "자동차 신품 부품", "산업용",
    ],
}

# 상폐기업 induty_code(한국표준산업분류) 기반 매핑.
INDUTY_GICS_3: dict[str, str] = {
    "061": "Energy",
    "192": "Energy",
    "261": "Information Technology",
    "262": "Information Technology",
    "263": "Information Technology",
    "264": "Information Technology",
    "211": "Health Care",
}

INDUTY_GICS_2: dict[str, str] = {
    "01": "Consumer Staples",
    "02": "Consumer Staples",
    "03": "Consumer Staples",
    "05": "Materials",
    "06": "Energy",
    "07": "Materials",
    "08": "Materials",
    "10": "Consumer Staples",
    "11": "Consumer Staples",
    "12": "Consumer Staples",
    "13": "Consumer Discretionary",
    "14": "Consumer Discretionary",
    "15": "Consumer Discretionary",
    "16": "Materials",
    "17": "Materials",
    "18": "Communication Services",
    "19": "Energy",
    "20": "Materials",
    "21": "Health Care",
    "22": "Materials",
    "23": "Materials",
    "24": "Materials",
    "25": "Industrials",
    "26": "Information Technology",
    "27": "Industrials",
    "28": "Industrials",
    "29": "Consumer Discretionary",
    "30": "Industrials",
    "31": "Consumer Discretionary",
    "32": "Industrials",
    "33": "Industrials",
    "35": "Utilities",
    "36": "Utilities",
    "37": "Utilities",
    "38": "Utilities",
    "39": "Utilities",
    "41": "Industrials",
    "42": "Industrials",
    "45": "Consumer Discretionary",
    "46": "Industrials",
    "47": "Consumer Discretionary",
    "49": "Industrials",
    "50": "Industrials",
    "51": "Industrials",
    "52": "Industrials",
    "55": "Consumer Discretionary",
    "56": "Consumer Discretionary",
    "58": "Information Technology",
    "59": "Communication Services",
    "60": "Communication Services",
    "61": "Communication Services",
    "62": "Information Technology",
    "63": "Information Technology",
    "64": "Financials",
    "65": "Financials",
    "66": "Financials",
    "68": "Real Estate",
    "69": "Industrials",
    "70": "Industrials",
    "71": "Industrials",
    "72": "Industrials",
    "73": "Communication Services",
    "74": "Communication Services",
    "75": "Industrials",
    "77": "Industrials",
    "78": "Communication Services",
    "79": "Consumer Discretionary",
    "80": "Industrials",
    "81": "Industrials",
    "82": "Industrials",
    "84": "Industrials",
    "85": "Industrials",
    "86": "Health Care",
    "87": "Health Care",
    "88": "Health Care",
    "90": "Communication Services",
    "91": "Consumer Discretionary",
    "92": "Consumer Discretionary",
    "93": "Consumer Discretionary",
    "94": "Industrials",
    "95": "Consumer Discretionary",
    "96": "Consumer Discretionary",
}

FINANCIAL_RISK_KEYWORDS = [
    "감사의견거절", "감사의견 거절",
    "감사범위제한", "감사범위 제한",
    "감사의견 부적정", "감사의견부적정",
    "자본전액잠식", "자본잠식률",
    "최종부도", "부도",
    "영업손실",
    "법인세비용차감전계속사업손실", "법인세차감전계속사업손실",
    "매출액 미달", "매출액미달",
    "시가총액",
    "계속기업",
    "회생절차",
    "파산",
    "기업의 계속성",
]

EXCLUDE_REASON_KEYWORDS = [
    "피흡수합병", "합병",
    "유가증권시장 상장", "증권거래소 상장", "한국증권거래소 상장",
    "자진등록취소", "상장폐지신청", "상장폐지 신청", "자진 등록취소",
    "등록법인의 취소신청",
    "주식분산기준", "주식분산기분",
    "주된영업의 양도", "주된 영업의 양도",
    "증권투자회사법", "간접투자자산운용업법",
    "불성실공시",
    "거래실적부진",
    "액면가액일정비율", "액면가액 일정비율",
    "타법인의 완전자회사",
    "주식양도 제한",
    "존립기간의 만료",
]


def _run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    print(f"  $ {' '.join(cmd)}")
    return subprocess.run(cmd, check=check, text=True)


def _normalize_sectors(sectors: list[str] | None) -> list[str] | None:
    if not sectors:
        return None
    alias_map = {k.lower(): v for k, v in SECTOR_ALIASES.items()}
    alias_map.update({s.lower(): s for s in VALID_GICS_SECTORS})

    normalized: list[str] = []
    unknown: list[str] = []
    for raw in sectors:
        key = raw.strip().lower()
        val = alias_map.get(key)
        if not val:
            unknown.append(raw)
            continue
        if val not in normalized:
            normalized.append(val)

    if unknown:
        valid = ", ".join(VALID_GICS_SECTORS)
        raise ValueError(
            f"Unknown sector(s): {unknown}. Valid sectors: {valid}"
        )
    return normalized


def _map_gics_from_industry(industry: str) -> str | None:
    if not isinstance(industry, str):
        return None
    text = industry.strip()
    if not text:
        return None

    priority = [
        "Information Technology",
        "Communication Services",
        "Health Care",
        "Financials",
        "Real Estate",
        "Utilities",
        "Energy",
        "Materials",
        "Consumer Staples",
        "Consumer Discretionary",
        "Industrials",
    ]
    for sector in priority:
        for kw in SECTOR_KEYWORDS_NORMAL[sector]:
            if kw in text:
                return sector

    if "제조업" in text:
        return "Industrials"
    return None


def _map_gics_from_induty_code(code: str) -> str | None:
    if not isinstance(code, str):
        return None
    code = code.strip()
    if len(code) < 2:
        return None
    prefix3 = code[:3]
    prefix2 = code[:2]
    if prefix3 in INDUTY_GICS_3:
        return INDUTY_GICS_3[prefix3]
    return INDUTY_GICS_2.get(prefix2)


def _is_financial_risk(reason: str | float | None) -> bool:
    if pd.isna(reason):
        return False
    reason_text = str(reason)
    if any(k in reason_text for k in EXCLUDE_REASON_KEYWORDS):
        return False
    return any(k in reason_text for k in FINANCIAL_RISK_KEYWORDS)


# ── 정상 기업 목록 생성 ──────────────────────────────────────────
def _build_normal_companies(
    sectors: list[str] | None,
    start_year: int = DEFAULT_START_YEAR,
    end_year: int = DEFAULT_END_YEAR,
) -> pd.DataFrame:
    """KRX 엑셀에서 정상 기업 목록을 생성 (범용 섹터 매핑)."""
    if not KRX_XLSX.exists():
        print(f"KRX 전종목 파일이 없습니다: {KRX_XLSX}")
        print("KRX 정보데이터시스템에서 다운로드하세요.")
        sys.exit(1)

    df = pd.read_excel(str(KRX_XLSX), dtype={"종목코드": str})
    df = df[~df["회사명"].str.contains("스펙", na=False)]
    df = df[["종목코드", "회사명", "업종"]].copy()
    df.columns = ["stock_code", "corp_name", "industry"]
    df["gics_sector"] = df["industry"].apply(_map_gics_from_industry)
    df = df[df["gics_sector"].notna()]
    df = df[df["stock_code"].str.match(r"^\d{6}$")]

    df["label"] = 0
    df["start_year"] = start_year
    df["end_year"] = end_year

    df = df[COLUMNS]

    if sectors:
        df = df[df["gics_sector"].isin(sectors)]

    return df


# ── 상폐 기업 목록 생성 ──────────────────────────────────────────
def _ensure_induty_cache() -> None:
    """업종코드 캐시가 없으면 f_fetch_induty_codes.py 실행."""
    if INDUTY_CACHE.exists():
        print(f"  업종코드 캐시 사용: {INDUTY_CACHE}")
        return

    print("  업종코드 캐시 없음 → f_fetch_induty_codes.py 실행 (DART API 호출, 시간 소요)")
    if not CORP_CODES_XML.exists():
        print(f"  corp_codes.xml이 없습니다: {CORP_CODES_XML}")
        print("  DART OpenAPI에서 다운로드하세요.")
        sys.exit(1)

    _run(["python3", "f_fetch_induty_codes.py"])

    if not INDUTY_CACHE.exists():
        print("  업종코드 캐시 생성 실패.")
        sys.exit(1)


def _build_delisted_companies(
    sectors: list[str] | None,
    start_year: int = DEFAULT_START_YEAR,
) -> pd.DataFrame:
    """상폐 기업 목록을 생성 (범용 섹터 매핑)."""
    if not DELISTED_XLSX.exists():
        print(f"상장폐지현황 파일이 없습니다: {DELISTED_XLSX}")
        sys.exit(1)

    _ensure_induty_cache()

    # 업종코드 로드
    df_induty = pd.read_csv(str(INDUTY_CACHE), dtype={"종목코드": str, "induty_code": str})

    # 상폐 기업 로드
    df_del = pd.read_excel(str(DELISTED_XLSX), dtype={"종목코드": str})
    df_del = df_del[["종목코드", "폐지일자", "폐지사유"]]

    # 병합
    df = df_induty.merge(df_del, on="종목코드", how="left")

    # SPAC 제거
    SPAC_KEYWORDS = ["스펙", "기업인수목적", "SPAC"]
    df = df[~df["회사명"].str.contains("|".join(SPAC_KEYWORDS), na=False)]

    # 재무적 리스크 필터링
    df = df[df["폐지사유"].apply(_is_financial_risk)]

    # GICS 매핑
    df["gics_sector"] = df["induty_code"].apply(_map_gics_from_induty_code)
    df = df[df["gics_sector"].notna()]

    # end_year = 폐지 연도
    df["end_year"] = pd.to_datetime(
        df["폐지일자"], errors="coerce"
    ).dt.year.fillna(2025).astype(int)

    # start_year: 사용자 지정 start_year와 end_year 중 작은 값
    # 예: start_year=2002, 폐지연도=2010 → 2002~2010 수집
    # 예: start_year=2002, 폐지연도=2000 → 2000~2000 수집
    df["start_year"] = df["end_year"].apply(
        lambda ey: min(start_year, ey)
    )

    df = df.copy()
    df["stock_code"] = df["종목코드"]
    df["corp_name"] = df["회사명"]
    df["label"] = 1

    df = df[COLUMNS]

    if sectors:
        df = df[df["gics_sector"].isin(sectors)]

    return df


# ── 메인 파이프라인 ──────────────────────────────────────────────
def main() -> int:
    parser = argparse.ArgumentParser(
        description="통합 수집 파이프라인: 기업 목록 생성 → 재무제표 수집 → S3 업로드",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--status",
        required=True,
        choices=["normal", "delisted", "all"],
        help="수집 대상: normal(정상), delisted(상폐), all(전체)",
    )
    parser.add_argument(
        "--sectors",
        nargs="+",
        default=None,
        help="수집할 GICS 섹터 (예: 'Information Technology' 'Consumer Discretionary'). 미지정 시 전체",
    )
    parser.add_argument(
        "--member",
        required=True,
        help="작업자 이름 (S3 로그 기록용)",
    )
    parser.add_argument(
        "--skip-s3",
        action="store_true",
        help="S3 업로드 건너뛰기 (수집만 수행)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="이미 수집된 데이터도 재수집 + S3 덮어쓰기",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=None,
        help=f"수집 시작 연도 (기본: {DEFAULT_START_YEAR}). "
             f"{LEGACY_CUTOFF_YEAR} 미만이면 dart-fss로 legacy 수집 자동 전환",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help=f"수집 종료 연도 (기본: {DEFAULT_END_YEAR})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="수집 대상 기업 목록만 출력하고 종료",
    )
    args = parser.parse_args()
    try:
        args.sectors = _normalize_sectors(args.sectors)
    except ValueError as e:
        print(str(e))
        return 1

    # ── 연도 범위 결정 ──
    start_year = args.start_year if args.start_year is not None else DEFAULT_START_YEAR
    end_year = args.end_year if args.end_year is not None else DEFAULT_END_YEAR
    has_legacy = start_year < LEGACY_CUTOFF_YEAR
    step_total = 4  # 기업목록 → 수집 → 누락재수집 → S3

    # ── 1) 기업 목록 생성 ──
    print("=" * 60)
    print(f"[1/{step_total}] 기업 목록 생성")
    print("=" * 60)
    print(f"  수집 범위: {start_year}~{end_year}")
    if has_legacy:
        print(f"  {LEGACY_CUTOFF_YEAR} 이전은 dart-fss 사용")

    frames: list[pd.DataFrame] = []

    if args.status in ("normal", "all"):
        print("\n정상 기업 목록 생성 중...")
        df_normal = _build_normal_companies(args.sectors, start_year, end_year)
        print(f"  정상 기업: {len(df_normal)}개")
        frames.append(df_normal)

    if args.status in ("delisted", "all"):
        print("\n상폐 기업 목록 생성 중...")
        df_delisted = _build_delisted_companies(args.sectors, start_year)
        print(f"  상폐 기업: {len(df_delisted)}개")
        frames.append(df_delisted)

    if not frames:
        print("수집 대상 기업이 없습니다.")
        return 1

    df_all = pd.concat(frames, ignore_index=True)
    df_all = df_all.drop_duplicates(subset=["stock_code"], keep="first")

    print(f"\n총 수집 대상: {len(df_all)}개")
    print("\n섹터별 분포:")
    for sector, group in df_all.groupby("gics_sector"):
        normal_cnt = (group["label"] == 0).sum()
        delisted_cnt = (group["label"] == 1).sum()
        print(f"  {sector}: 정상 {normal_cnt} / 상폐 {delisted_cnt}")

    # legacy 대상 기업 수
    legacy_count = (df_all["start_year"] < LEGACY_CUTOFF_YEAR).sum()
    if legacy_count > 0:
        print(f"\n  {LEGACY_CUTOFF_YEAR}년 이전 수집 대상: {legacy_count}개 기업 (dart-fss)")

    if args.dry_run:
        print(f"\n[dry-run] 기업 목록 미리보기 (상위 20개):")
        print(df_all.head(20).to_string(index=False))
        return 0

    # 기업 목록 CSV 저장
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    df_all.to_csv(str(PIPELINE_CSV), index=False, encoding="utf-8-sig")
    print(f"\n기업 목록 저장: {PIPELINE_CSV}")

    # ── 2) 재무제표 수집 ──
    print("\n" + "=" * 60)
    print(f"[2/{step_total}] 재무제표 수집 (collect.py)")
    print("=" * 60)

    collect_cmd = [
        "python3", "collect.py", "collect",
        "--companies", str(PIPELINE_CSV),
        "--save-raw",
    ]
    if args.force:
        collect_cmd.append("--force")

    result = _run(collect_cmd, check=False)
    if result.returncode != 0:
        print("\n재무제표 수집 중 오류 발생. 로그를 확인하세요.")
        # 수집 실패해도 다음 단계는 시도 (이미 수집된 데이터가 있을 수 있음)

    # ── 3) 누락 검증 및 재수집 ──
    print("\n" + "=" * 60)
    print(f"[3/{step_total}] 누락 데이터 검증 및 재수집")
    print("=" * 60)

    retry_cmd = [
        "python3", "collect.py", "retry",
        "--save-raw",
    ]

    result = _run(retry_cmd, check=False)
    if result.returncode != 0:
        print("\n누락 재수집 중 오류 발생. 로그를 확인하세요.")

    # ── S3 업로드 ──
    s3_step = step_total
    if args.skip_s3:
        print("\n[skip] S3 업로드 건너뜀 (--skip-s3)")
    else:
        print("\n" + "=" * 60)
        print(f"[{s3_step}/{step_total}] S3 업로드")
        print("=" * 60)

        # collect.py의 --upload-s3 로 업로드 (수집과 동시 처리도 가능)
        upload_sectors = args.sectors or df_all["gics_sector"].unique().tolist()
        for sector in upload_sectors:
            print(f"\n  섹터: {sector}")
            s3_cmd = [
                "python3", "collect.py", "collect",
                "--companies", str(PIPELINE_CSV),
                "--upload-s3",
            ]
            if args.force:
                s3_cmd.append("--force")
            _run(s3_cmd, check=False)
            break  # collect_batch가 전체 기업을 한 번에 처리하므로 한 번만 실행

    # ── 완료 ──
    print("\n" + "=" * 60)
    print("파이프라인 완료")
    print("=" * 60)
    print(f"  대상: {args.status} ({len(df_all)}개 기업)")
    print(f"  수집 범위: {start_year}~{end_year}")
    if has_legacy:
        print(f"  Legacy: {start_year}~{LEGACY_CUTOFF_YEAR - 1} (dart-fss)")
    if args.sectors:
        print(f"  섹터: {', '.join(args.sectors)}")
    print(f"  CSV: data/output/{{sector}}/{{ticker}}_{{year}}.csv")
    print(f"  Raw: data/raw/{{ticker}}_{{year}}_{{quarter}}_{{fs_div}}.json")
    if not args.skip_s3:
        print(f"  S3:  s3://kw0ss-raw-data-s3/{{healthy|delisted}}/{{sector}}/...")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
