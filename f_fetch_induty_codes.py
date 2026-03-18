# 상폐 기업 업종코드 조회 → delisted_induty_codes.csv 저장
import requests, os, time
import xml.etree.ElementTree as ET
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

api_key = os.getenv("DART_API_KEY")

tree = ET.parse("data/etc/corp_codes.xml")
root = tree.getroot()
code_map = {
    (item.findtext("stock_code") or "").strip(): 
    (item.findtext("corp_code") or "").strip()
    for item in root.findall("list")
}

df = pd.read_excel("data/etc/상장폐지현황.xlsx", dtype={"종목코드": str})
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
df_result.to_csv("data/etc/delisted_induty_codes.csv", index=False, encoding="utf-8-sig")

print("\n앞 2자리 분포:")
print(df_result["induty_code"].str[:2].value_counts().sort_index())