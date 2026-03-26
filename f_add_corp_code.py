# A_companies_final.csv에 corp_code 컬럼 미리 추가
# → collect.py 실행 시 742번 API 조회 없이 바로 수집 시작 가능
import pandas as pd
import xml.etree.ElementTree as ET
from pathlib import Path

CORP_XML   = Path("data/corpCode.xml")
INPUT_FILE = Path("data/input/A_companies_final.csv")

# 1) XML에서 종목코드 → corp_code 매핑 테이블 생성
print("corp_codes.xml 파싱 중...")
tree = ET.parse(CORP_XML)
root = tree.getroot()
code_map = {
    (item.findtext("stock_code") or "").strip():
    (item.findtext("corp_code") or "").strip()
    for item in root.findall("list")
}
print(f"  총 {len(code_map)}개 매핑 테이블 생성 완료")

# 2) CSV 로드
df = pd.read_csv(INPUT_FILE, dtype={"stock_code": str})
print(f"  기업 수: {len(df)}개")

# 3) corp_code 컬럼 추가 (이미 있으면 덮어쓰기)
df["corp_code"] = df["stock_code"].map(code_map)

# 4) 매핑 결과 확인
mapped   = df["corp_code"].notna().sum()
unmapped = df["corp_code"].isna().sum()
print(f"\n매핑 성공: {mapped}개")
print(f"매핑 실패: {unmapped}개")

if unmapped > 0:
    print("\n매핑 실패 종목:")
    print(df[df["corp_code"].isna()][["stock_code", "corp_name"]])

# 5) 컬럼 순서: stock_code, corp_code, corp_name, label, gics_sector, start_year, end_year
cols = ["stock_code", "corp_code", "corp_name", "label", "gics_sector", "start_year", "end_year"]
df = df[cols]

# 6) 저장
df.to_csv(INPUT_FILE, index=False, encoding="utf-8-sig")
print(f"\n저장 완료: {INPUT_FILE}")
print("이제 collect.py 실행 시 corp_code 조회 없이 바로 수집 시작됩니다!")