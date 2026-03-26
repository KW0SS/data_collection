# 상폐 기업 업종코드 조회 → delisted_induty_codes.csv 저장
import requests, os, sys, time
import xml.etree.ElementTree as ET
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

api_key = os.getenv("DART_API_KEY")
if not api_key:
    print("DART_API_KEY가 설정되지 않았습니다. .env 파일을 확인하세요.")
    sys.exit(1)

CORP_CODES_PATH = Path("data/etc/corp_codes.xml")
DELISTED_XLSX   = Path("data/etc/상장폐지현황.xlsx")

if not CORP_CODES_PATH.exists():
    print(f"corp_codes.xml이 없습니다: {CORP_CODES_PATH}")
    print("DART OpenAPI에서 고유번호 전체 목록을 다운로드하세요:")
    print("  https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key=<API_KEY>")
    sys.exit(1)

if not DELISTED_XLSX.exists():
    print(f"상장폐지현황 파일이 없습니다: {DELISTED_XLSX}")
    sys.exit(1)

tree = ET.parse(str(CORP_CODES_PATH))
root = tree.getroot()
code_map = {
    (item.findtext("stock_code") or "").strip():
    (item.findtext("corp_code") or "").strip()
    for item in root.findall("list")
}

df = pd.read_excel(str(DELISTED_XLSX), dtype={"종목코드": str})
df = df[df["종목코드"].str.match(r"^\d{6}$", na=False)]

# SPAC 제거 강화
SPAC_KEYWORDS = ["스펙", "기업인수목적", "SPAC"]
df = df[~df["회사명"].str.contains("|".join(SPAC_KEYWORDS), na=False)]
print(f"SPAC 제거 후: {len(df)}개")

results = []
total = len(df)
for i, (_, row) in enumerate(df.iterrows(), 1):
    sc = row["종목코드"]
    corp_code = code_map.get(sc)
    if not corp_code:
        continue
    try:
        resp = requests.get(
            "https://opendart.fss.or.kr/api/company.json",
            params={"crtfc_key": api_key, "corp_code": corp_code},
            timeout=30
        )
        data = resp.json()
        if data.get("status") == "000":
            results.append({
                "종목코드": sc,
                "회사명": data.get("corp_name"),
                "induty_code": data.get("induty_code")
            })
    except Exception as e:
        print(f"  [ERR] {sc}: {e}")

    if i % 100 == 0:
        print(f"  진행: {i}/{total}")
    time.sleep(0.3)

df_result = pd.DataFrame(results)
if df_result.empty:
    print("조회 결과가 없습니다. API 키와 corp_codes.xml을 확인하세요.")
    sys.exit(1)

OUTPUT_PATH = Path("data/etc/delisted_induty_codes.csv")
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
df_result.to_csv(str(OUTPUT_PATH), index=False, encoding="utf-8-sig")

print(f"\n저장 완료: {OUTPUT_PATH} ({len(df_result)}건)")
if "induty_code" in df_result.columns:
    print("\n앞 2자리 분포:")
    print(df_result["induty_code"].str[:2].value_counts().sort_index())