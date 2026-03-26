# 기업 리스트 생성 + GICS 매핑
import pandas as pd
import os
import sys
from pathlib import Path

# KRX 엑셀 파일 경로
INPUT_FILE = Path("data/input/krx_all_companies.xlsx")

# 저장 파일
OUTPUT_FILE = Path("data/input/A_companies.csv")

# ── 수집 연도 설정 ──────────────────────────────
START_YEAR = 2015
END_YEAR   = 2025
LABEL      = 0   # 정상 기업
# ────────────────────────────────────────────────

INCLUDE_KEYWORDS = [
    # IT
    "반도체", "소프트웨어", "컴퓨터", "프로그래밍",
    "시스템 통합", "정보 서비스", "자료처리", "호스팅", "포털",
    "통신 및 방송 장비", "영상 및 음향기기",
    "광학기기", "가정용 기기",

    # Communication
    "전기 통신", "텔레비전 방송",
    "영화", "비디오", "게임", "광고",
    "출판", "오디오물", "창작", "엔터테인먼트",
    "영상·오디오물",

    # Consumer Discretionary
    "의복", "의류", "가죽", "화장품",
    "가구", "숙박", "음식점",
    "종합 소매", "무점포 소매", "가전제품 및 정보통신장비 소매",
    "유원지",
]

EXCLUDE_KEYWORDS = [
    "금융", "보험", "부동산", "건설", "연료",
    "가스", "철강", "석유", "광업", "도매업",
    "공사업", "전력", "수도",
    "전자부품",          # 납품 구조, Industrials 성격
    "자동차 신품 부품",   # 동일
    "직물", "방적",       # 전통 섬유제조
]

EXCLUDE_INDUSTRIES_STRICT = [
    "화학섬유 제조업",
    "기타 섬유제품 제조업",
    "자동차 차체나 트레일러 제조업",
    "자동차 재제조 부품 제조업",
    "섬유제품 염색, 정리 및 마무리 가공업",
    "자동차용 엔진 및 자동차 제조업",
    "측정, 시험, 항해, 제어 및 기타 정밀기기 제조업; 광학기기 제외"
]


def is_A_sector(industry):
    if any(k in industry for k in INCLUDE_KEYWORDS):
        if not any(e in industry for e in EXCLUDE_KEYWORDS):
            return True
    return False


def contains_any(industry, keywords):
    return any(k in industry for k in keywords)


def map_gics(industry):
    IT   = ["반도체", "전자부품", "컴퓨터", "소프트웨어",
            "시스템 통합", "정보 서비스", "자료처리", "포털",
            "통신 및 방송 장비", "영상 및 음향기기",
            "사진장비", "광학기기", "가정용 기기"]

    COMM = ["전기 통신", "방송", "영화", "광고",
            "출판", "엔터", "게임",
            "창작", "유원지", "영상·오디오물"]

    CONS = ["의복", "의류", "가죽", "화장품",
            "가구", "숙박", "음식점",
            "소매", "백화점", "신발"]

    if contains_any(industry, IT):
        return "Information Technology"
    elif contains_any(industry, COMM):
        return "Communication Services"
    elif contains_any(industry, CONS):
        return "Consumer Discretionary"
    else:
        return None


if __name__ == "__main__":
    if not INPUT_FILE.exists():
        print(f"KRX 기업 목록 파일이 없습니다: {INPUT_FILE}")
        print("KRX에서 전종목 엑셀을 다운로드하여 해당 경로에 저장하세요.")
        sys.exit(1)

    df = pd.read_excel(str(INPUT_FILE), dtype={"종목코드": str})
    # SPAC 제거
    df = df[~df["회사명"].str.contains("스펙", na=False)]

    df_A = df[df["업종"].apply(is_A_sector)]

    # 애매한 산업 제거
    df_A = df_A[~df_A["업종"].isin(EXCLUDE_INDUSTRIES_STRICT)]

    # 컬럼 정리 및 이름 변경
    df_A = df_A[["종목코드", "회사명", "업종"]].copy()
    df_A.columns = ["stock_code", "corp_name", "industry"]

    # GICS 매핑
    df_A["gics_sector"] = df_A["industry"].apply(map_gics)
    df_A = df_A[df_A["gics_sector"].notna()]

    # 6자리 숫자 종목코드만
    df_A = df_A[df_A["stock_code"].str.match(r"^\d{6}$")]

    print("\n=== 필터링 후 포함된 업종 ===")
    print(df_A["industry"].value_counts().to_string())

    print("\n=== 기업 수 ===")
    print("총 기업 수:", len(df_A))

    print("\n=== GICS 섹터별 분포 ===")
    print(df_A["gics_sector"].value_counts())

    # collect.py가 필요로 하는 컬럼 추가
    df_A["label"]      = LABEL
    df_A["start_year"] = START_YEAR
    df_A["end_year"]   = END_YEAR

    # 컬럼 순서 맞추기
    df_A = df_A[["stock_code", "corp_name", "label", "gics_sector", "start_year", "end_year"]]

    # 저장
    os.makedirs("data/input", exist_ok=True)
    df_A.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print("A섹터 기업 CSV 생성 완료")
    print("기업 수:", len(df_A))
    print(df_A.head())
