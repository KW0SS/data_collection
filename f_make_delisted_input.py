# 상폐 기업 A섹터 필터링 + 정상 기업과 합쳐서 A_companies_final.csv 완성
# 사전 준비: f_fetch_induty_codes.py 실행 → data/etc/delisted_induty_codes.csv 생성
import sys
from pathlib import Path

import pandas as pd

# ── 경로 설정 ──────────────────────────────────────────────────────────
INDUTY_FILE  = Path("data/etc/delisted_induty_codes.csv")   # f_fetch_induty_codes.py 결과
DELISTED_FILE= Path("data/etc/상장폐지현황.xlsx")
NORMAL_FILE  = Path("data/input/A_companies.csv")
OUTPUT_FILE  = Path("data/input/A_companies_final.csv")

START_YEAR   = 2015
LABEL        = 1   # 상폐 기업


def _check_input_files() -> None:
    """입력 파일 존재 여부를 검증. 단독 실행 시에만 호출."""
    missing = [p for p in [INDUTY_FILE, DELISTED_FILE, NORMAL_FILE] if not p.exists()]
    if missing:
        print("필요한 입력 파일이 없습니다:")
        for p in missing:
            print(f"  - {p}")
        if INDUTY_FILE in missing:
            print("\n먼저 f_fetch_induty_codes.py를 실행하세요.")
        if NORMAL_FILE in missing:
            print("먼저 f_make_A_input.py를 실행하세요.")
        sys.exit(1)

# ── induty_code → GICS 매핑 ───────────────────────────────────────────
# 앞자리 3자리 우선 매핑, 없으면 2자리로 fallback
# 포함 기준: A섹터 정의 (IT, Communication Services, Consumer Discretionary)

INDUTY_GICS_3 = {
    # IT - 반도체·디스플레이·통신장비
    "261": "Information Technology",   # 반도체
    "263": "Information Technology",   # 디스플레이·광학
    "264": "Information Technology",   # 통신장비·방송장비

    # Consumer Discretionary - 가정용기기
    "265": "Consumer Discretionary",   # 가정용기기

    # 제외: 262(전자부품), 266~269(기타 전자)
}

INDUTY_GICS_2 = {
    # Information Technology
    "58": "Information Technology",    # 출판·소프트웨어
    "62": "Information Technology",    # 컴퓨터 프로그래밍·시스템통합
    "63": "Information Technology",    # 정보서비스·자료처리

    # Communication Services
    "59": "Communication Services",    # 영화·비디오·방송프로그램
    "60": "Communication Services",    # 방송
    "61": "Communication Services",    # 통신
    "74": "Communication Services",    # 광고
    "90": "Communication Services",    # 창작·예술·여가

    # Consumer Discretionary
    "14": "Consumer Discretionary",    # 의복·봉제
    "31": "Consumer Discretionary",    # 가구
    "47": "Consumer Discretionary",    # 소매
    "56": "Consumer Discretionary",    # 음식점
    "55": "Consumer Discretionary",    # 숙박
    "91": "Consumer Discretionary",    # 스포츠·오락
}

# 명시적 제외 코드 (앞 3자리)
EXCLUDE_3 = {
    "262",   # 전자부품 (납품구조, Industrials)
}

# 명시적 제외 코드 (앞 2자리)
EXCLUDE_2 = {
    "26",    # 앞자리만 있는 경우 (분류 불명확)
    "64",    # 금융
    "65",    # 보험
    "66",    # 금융지원
    "68",    # 부동산
    "41",    # 건설
    "42",    # 토목
}


def map_gics_by_code(code: str):
    """induty_code → GICS 섹터. 3자리 우선, 없으면 2자리."""
    if not code or not isinstance(code, str):
        return None

    code = code.strip()
    prefix3 = code[:3]
    prefix2 = code[:2]

    # 명시적 제외 먼저
    if prefix3 in EXCLUDE_3 or prefix2 in EXCLUDE_2:
        return None

    # 3자리 매핑 우선
    if prefix3 in INDUTY_GICS_3:
        return INDUTY_GICS_3[prefix3]

    # 2자리 매핑 fallback
    if prefix2 in INDUTY_GICS_2:
        return INDUTY_GICS_2[prefix2]

    return None


# ── 재무적 리스크 필터링 ──────────────────────────────────────────
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


def is_financial_risk(reason):
    """폐지사유가 재무적 리스크에 해당하는지 판별."""
    if pd.isna(reason):
        return False
    if any(k in reason for k in EXCLUDE_REASON_KEYWORDS):
        return False
    return any(k in reason for k in FINANCIAL_RISK_KEYWORDS)


def main():
    _check_input_files()

    # 1) 업종코드 파일 로드
    df_induty = pd.read_csv(INDUTY_FILE, dtype={"종목코드": str, "induty_code": str})
    print(f"업종코드 보유 기업: {len(df_induty)}개")

    # 2) 상폐 기업 파일 로드 (폐지일자 + 폐지사유 가져오기)
    df_del = pd.read_excel(DELISTED_FILE, dtype={"종목코드": str})
    df_del = df_del[["종목코드", "폐지일자", "폐지사유"]]

    # 3) 업종코드 병합
    df = df_induty.merge(df_del, on="종목코드", how="left")

    # 4) SPAC 제거 (강화)
    SPAC_KEYWORDS = ["스펙", "기업인수목적", "SPAC"]
    mask_spac = df["회사명"].str.contains("|".join(SPAC_KEYWORDS), na=False)
    df = df[~mask_spac]
    print(f"SPAC 제거 후: {len(df)}개")

    # 4-1) 재무적 리스크 사유만 필터링
    before = len(df)
    df = df[df["폐지사유"].apply(is_financial_risk)]
    print(f"재무적 리스크 필터링 후: {len(df)}개 (제거: {before - len(df)}개)")

    # 5) GICS 매핑
    df["gics_sector"] = df["induty_code"].apply(map_gics_by_code)

    # 매핑 결과 확인
    print("\n=== GICS 매핑 결과 ===")
    print(df["gics_sector"].value_counts(dropna=False))

    # 6) 매핑 실패 제외
    df = df[df["gics_sector"].notna()]
    print(f"\nA섹터 필터링 후: {len(df)}개")

    # 7) end_year = 폐지연도
    df["end_year"] = pd.to_datetime(
        df["폐지일자"], errors="coerce"
    ).dt.year.fillna(2025).astype(int)

    # 7-1) start_year > end_year 방지: 2015년 이전 폐지 기업은 start_year를 end_year에 맞춤
    df["start_year_raw"] = START_YEAR
    df["start_year_raw"] = df.apply(
        lambda r: min(START_YEAR, r["end_year"]) if r["end_year"] < START_YEAR else START_YEAR,
        axis=1,
    )
    invalid_count = (df["end_year"] < START_YEAR).sum()
    if invalid_count > 0:
        print(f"  ⚠️ {START_YEAR}년 이전 폐지 기업 {invalid_count}건 → start_year 조정됨")

    # 8) 컬럼 정리
    df = df.copy()
    df["stock_code"] = df["종목코드"]
    df["corp_name"]  = df["회사명"]
    df["label"]      = LABEL
    df["start_year"] = df["start_year_raw"]

    df_del_final = df[["stock_code", "corp_name", "label", "gics_sector", "start_year", "end_year"]]

    print("\n=== 상폐 기업 최종 ===")
    print(df_del_final["gics_sector"].value_counts())

    # 9) 정상 기업과 합치기
    df_normal = pd.read_csv(NORMAL_FILE, dtype={"stock_code": str})
    print(f"\n정상 기업: {len(df_normal)}개")

    df_final = pd.concat([df_normal, df_del_final], ignore_index=True)

    # 중복 종목코드 제거 (정상 기업 우선)
    df_final = df_final.drop_duplicates(subset=["stock_code"], keep="first")

    print(f"\n=== 최종 결과 ===")
    print(f"전체 기업 수: {len(df_final)}")
    print(df_final["label"].value_counts())
    print(df_final["gics_sector"].value_counts())

    # 10) 저장
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df_final.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"\n저장 완료: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()