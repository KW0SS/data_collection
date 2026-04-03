# 상폐 기업 업종코드 조회 → delisted_induty_codes.csv 저장
# - 이전 실행 결과 캐시: 이미 조회된 종목은 스킵
# - 100건마다 중간 저장: 중단 시 이어서 재개
# - 병렬 3 workers: 약 1/3 시간 단축
import requests, os, sys, time
import xml.etree.ElementTree as ET
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

api_key = os.getenv("DART_API_KEY")
if not api_key:
    print("DART_API_KEY가 설정되지 않았습니다. .env 파일을 확인하세요.")
    sys.exit(1)

CORP_CODES_PATH = Path("data/etc/corp_codes.xml")
DELISTED_XLSX   = Path("data/etc/상장폐지현황.xlsx")
OUTPUT_PATH = Path("data/etc/delisted_induty_codes.csv")

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

# ── 이전 실행 결과 캐시 로드 ──────────────────────────────────
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
cached_codes: set[str] = set()
results: list[dict[str, str]] = []
if OUTPUT_PATH.exists():
    df_cached = pd.read_csv(OUTPUT_PATH, dtype={"종목코드": str})
    cached_codes = set(df_cached["종목코드"])
    results = df_cached.to_dict("records")
    print(f"캐시 로드: {len(cached_codes)}건")

# ── 미조회 종목 필터 ──────────────────────────────────────────
pending = [
    (row["종목코드"], code_map.get(row["종목코드"]))
    for _, row in df.iterrows()
    if row["종목코드"] not in cached_codes and code_map.get(row["종목코드"])
]
print(f"신규 조회 대상: {len(pending)}건")

if not pending:
    print("조회할 종목이 없습니다. (모두 캐시됨)")
    sys.exit(0)

BATCH_SAVE_SIZE = 100
MAX_WORKERS = 3


def fetch_one(sc: str, corp_code: str) -> dict[str, str] | None:
    """단일 종목 업종코드 조회. rate limit 준수를 위해 호출 후 sleep."""
    try:
        resp = requests.get(
            "https://opendart.fss.or.kr/api/company.json",
            params={"crtfc_key": api_key, "corp_code": corp_code},
            timeout=30
        )
        data = resp.json()
        if data.get("status") == "000":
            return {
                "종목코드": sc,
                "회사명": data.get("corp_name"),
                "induty_code": data.get("induty_code")
            }
    except Exception as e:
        print(f"  [ERR] {sc}: {e}")
    finally:
        time.sleep(0.3)
    return None


# ── 병렬 조회 (3 workers) ────────────────────────────────────
done = 0
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
    futures = {
        pool.submit(fetch_one, sc, cc): sc
        for sc, cc in pending
    }
    for future in as_completed(futures):
        done += 1
        result = future.result()
        if result:
            results.append(result)

        # 중간 저장
        if done % BATCH_SAVE_SIZE == 0:
            pd.DataFrame(results).to_csv(
                str(OUTPUT_PATH), index=False, encoding="utf-8-sig"
            )
            print(f"  중간 저장: {len(results)}건 (진행: {done}/{len(pending)})")

# ── 최종 저장 ─────────────────────────────────────────────────
df_result = pd.DataFrame(results)
if df_result.empty:
    print("조회 결과가 없습니다. API 키와 corp_codes.xml을 확인하세요.")
    sys.exit(1)

df_result.to_csv(str(OUTPUT_PATH), index=False, encoding="utf-8-sig")

print(f"\n저장 완료: {OUTPUT_PATH} ({len(df_result)}건)")
if "induty_code" in df_result.columns:
    print("\n앞 2자리 분포:")
    print(df_result["induty_code"].str[:2].value_counts().sort_index())
