"""거시경제 데이터 수집기.

수집 변수 (6개):
  1. 신용스프레드       — ECOS 817Y002: 회사채AA-(010300000) - 국고채3년(010200000), 일별→분기평균
  2. 코스닥 분기 수익률 — ECOS 802Y001: KOSDAQ지수(0089000), 일별→분기말값→수익률
  3. GDP 성장률 yoy    — ECOS 200Y102: GDP전년동기비(10211), 분기
  4. 환율 전기비 변화율 — ECOS 731Y001: 원/달러매매기준율(0000001), 일별→분기말값→전기비변화율(%)
  5. VIX 분기 평균     — yfinance: ^VIX, 일별→분기평균
  6. CPI 전년동기비    — ECOS 901Y009: 소비자물가지수총지수(0), 분기→전년동기비(%)

확인된 ECOS 통계표/항목코드
──────────────────────────────
  817Y002 / 010300000  회사채(3년, AA-)        일별(D)
  817Y002 / 010200000  국고채(3년)             일별(D)
  802Y001 / 0089000    KOSDAQ지수              일별(D)
  200Y102 / 10211      GDP 실질 전년동기비      분기(Q)  형식: 2015Q1
  731Y001 / 0000001    원/달러 매매기준율       일별(D)
  901Y009 / 0          소비자물가지수 총지수    분기(Q)  형식: 2015Q1

출력:
  data/macro/macro_quarterly.csv
  컬럼: year, quarter, credit_spread, kosdaq_return,
         gdp_growth_yoy, usdkrw_chg, vix_avg, cpi_yoy

사용 예시:
  python macro_collector.py
  python macro_collector.py --start 2015 --end 2025
  python macro_collector.py --api-key YOUR_KEY --no-vix

사전 준비:
  pip install requests pandas yfinance python-dotenv
  .env 파일에 ECOS_API_KEY=your_key 추가
"""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

DATA_DIR    = Path(__file__).resolve().parent.parent / "data"
MACRO_DIR   = DATA_DIR / "macro"
OUTPUT_FILE = MACRO_DIR / "macro_quarterly.csv"

ECOS_BASE = "https://ecos.bok.or.kr/api/StatisticSearch"

QUARTER_MONTHS = {
    "Q1":     [1, 2, 3],
    "H1":     [4, 5, 6],
    "Q3":     [7, 8, 9],
    "ANNUAL": [10, 11, 12],
}


def _get_api_key(explicit=None):
    if explicit:
        return explicit
    key = os.getenv("ECOS_API_KEY")
    if not key:
        raise ValueError(
            "ECOS API 키가 없습니다.\n"
            "ECOS_API_KEY 환경변수를 설정하거나 .env 파일에 추가하세요.\n"
            "키 발급: https://ecos.bok.or.kr → Open API"
        )
    return key


def _ecos_daily(api_key, stat_code, item_code, start_d, end_d):
    """ECOS 일별(D) 조회 → DataFrame(date: datetime, value: float)."""
    url = (
        f"{ECOS_BASE}/{api_key}/json/kr/1/100000/"
        f"{stat_code}/D/{start_d}/{end_d}/{item_code}"
    )
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    if "StatisticSearch" not in data:
        print(f"    [경고] 데이터 없음: {stat_code}/{item_code}")
        return pd.DataFrame(columns=["date", "value"])

    rows = data["StatisticSearch"].get("row", [])
    if not rows:
        return pd.DataFrame(columns=["date", "value"])

    df = pd.DataFrame(rows)[["TIME", "DATA_VALUE"]].rename(
        columns={"TIME": "date", "DATA_VALUE": "value"}
    )
    df["date"]  = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df.dropna(subset=["date", "value"])


def _ecos_quarterly(api_key, stat_code, item_code, start_yq, end_yq):
    """ECOS 분기(Q) 조회 → DataFrame(date: str '2015Q1', value: float)."""
    url = (
        f"{ECOS_BASE}/{api_key}/json/kr/1/10000/"
        f"{stat_code}/Q/{start_yq}/{end_yq}/{item_code}"
    )
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    if "StatisticSearch" not in data:
        print(f"    [경고] 데이터 없음: {stat_code}/{item_code}")
        return pd.DataFrame(columns=["date", "value"])

    rows = data["StatisticSearch"].get("row", [])
    if not rows:
        return pd.DataFrame(columns=["date", "value"])

    df = pd.DataFrame(rows)[["TIME", "DATA_VALUE"]].rename(
        columns={"TIME": "date", "DATA_VALUE": "value"}
    )
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df


def _add_ym(df):
    df = df.copy()
    df["year"]  = df["date"].dt.year
    df["month"] = df["date"].dt.month
    return df


# ── 변수별 수집 ────────────────────────────────────────────────

def fetch_credit_spread(api_key, start_d, end_d, quarters):
    print("  회사채 AA- 금리 수집...")
    corp = _ecos_daily(api_key, "817Y002", "010300000", start_d, end_d)
    time.sleep(0.5)
    print("  국고채 3년 금리 수집...")
    gov  = _ecos_daily(api_key, "817Y002", "010200000", start_d, end_d)

    if corp.empty or gov.empty:
        return {}

    merged = corp.merge(gov, on="date", suffixes=("_corp", "_gov"))
    merged["spread"] = merged["value_corp"] - merged["value_gov"]
    merged = _add_ym(merged)

    result = {}
    for (yr, q) in quarters:
        mask = (merged["year"] == yr) & (merged["month"].isin(QUARTER_MONTHS[q]))
        vals = merged.loc[mask, "spread"].dropna()
        result[(yr, q)] = round(float(vals.mean()), 4) if not vals.empty else None
    return result


def fetch_kosdaq_return(api_key, start_d, end_d, quarters):
    print("  KOSDAQ 지수 수집...")
    df = _ecos_daily(api_key, "802Y001", "0089000", start_d, end_d)

    if df.empty:
        return {}

    df = _add_ym(df)

    # 월별 마지막 영업일 종가
    month_end = {}
    for (yr, mo), grp in df.groupby(["year", "month"]):
        month_end[(yr, mo)] = grp.sort_values("date")["value"].iloc[-1]

    result = {}
    for (yr, q) in quarters:
        end_mo = QUARTER_MONTHS[q][-1]
        if q == "Q1":
            base_yr, base_mo = yr - 1, 12
        elif q == "H1":
            base_yr, base_mo = yr, 3
        elif q == "Q3":
            base_yr, base_mo = yr, 6
        else:
            base_yr, base_mo = yr, 9

        end_val  = month_end.get((yr, end_mo))
        base_val = month_end.get((base_yr, base_mo))

        if end_val and base_val and base_val != 0:
            result[(yr, q)] = round((end_val / base_val - 1) * 100, 4)
        else:
            result[(yr, q)] = None
    return result


def fetch_gdp_growth(api_key, start_year, end_year, quarters):
    print("  GDP 성장률 수집...")
    df = _ecos_quarterly(
        api_key, "200Y102", "10211",
        f"{start_year}Q1", f"{end_year}Q4"
    )

    if df.empty:
        return {}

    ECOS_TO_Q = {"Q1": "Q1", "Q2": "H1", "Q3": "Q3", "Q4": "ANNUAL"}
    quarter_set = set(quarters)

    result = {}
    for _, row in df.iterrows():
        t = str(row["date"])
        if len(t) == 6 and "Q" in t:
            yr = int(t[:4])
            q  = ECOS_TO_Q.get(t[4:])
            if q and (yr, q) in quarter_set:
                result[(yr, q)] = row["value"]
    return result


def fetch_usdkrw_chg(api_key, start_d, end_d, quarters):
    """환율 전기비 변화율(%) = (분기말 환율 / 직전분기말 환율 - 1) * 100.
    양수 = 원화 약세, 음수 = 원화 강세."""
    print("  원/달러 환율 수집...")
    df = _ecos_daily(api_key, "731Y001", "0000001", start_d, end_d)

    if df.empty:
        return {}

    df = _add_ym(df)

    # 월별 마지막 영업일 환율
    month_end = {}
    for (yr, mo), grp in df.groupby(["year", "month"]):
        month_end[(yr, mo)] = grp.sort_values("date")["value"].iloc[-1]

    result = {}
    for (yr, q) in quarters:
        end_mo = QUARTER_MONTHS[q][-1]
        if q == "Q1":
            base_yr, base_mo = yr - 1, 12
        elif q == "H1":
            base_yr, base_mo = yr, 3
        elif q == "Q3":
            base_yr, base_mo = yr, 6
        else:
            base_yr, base_mo = yr, 9

        end_val  = month_end.get((yr, end_mo))
        base_val = month_end.get((base_yr, base_mo))

        if end_val and base_val and base_val != 0:
            result[(yr, q)] = round((end_val / base_val - 1) * 100, 4)
        else:
            result[(yr, q)] = None
    return result


def fetch_vix_avg(start_date, end_date, quarters):
    print("  VIX 수집 (yfinance)...")
    try:
        import yfinance as yf
    except ImportError:
        raise ImportError("pip install yfinance  또는  --no-vix 옵션 사용")

    df = yf.Ticker("^VIX").history(start=start_date, end=end_date, auto_adjust=False)
    if df.empty:
        print("  [경고] VIX 데이터 없음")
        return {}

    df = df[["Close"]].reset_index()
    df.columns = ["date", "value"]
    df["date"]  = pd.to_datetime(df["date"]).dt.tz_localize(None)
    df["year"]  = df["date"].dt.year
    df["month"] = df["date"].dt.month

    result = {}
    for (yr, q) in quarters:
        mask = (df["year"] == yr) & (df["month"].isin(QUARTER_MONTHS[q]))
        vals = df.loc[mask, "value"].dropna()
        result[(yr, q)] = round(float(vals.mean()), 4) if not vals.empty else None
    return result


def fetch_cpi_yoy(api_key, start_year, end_year, quarters):
    """CPI 전년동기비(%) = (당기 CPI / 전년동기 CPI - 1) * 100."""
    print("  CPI 수집...")
    # 전년동기비 계산을 위해 1년 앞 데이터부터 수집
    df = _ecos_quarterly(
        api_key, "901Y009", "0",
        f"{start_year - 1}Q1", f"{end_year}Q4"
    )

    if df.empty:
        return {}

    ECOS_TO_Q = {"Q1": "Q1", "Q2": "H1", "Q3": "Q3", "Q4": "ANNUAL"}

    # TIME → (year, quarter) 매핑 딕셔너리
    cpi_map = {}
    for _, row in df.iterrows():
        t = str(row["date"])
        if len(t) == 6 and "Q" in t:
            yr = int(t[:4])
            q  = ECOS_TO_Q.get(t[4:])
            if q:
                cpi_map[(yr, q)] = row["value"]

    result = {}
    for (yr, q) in quarters:
        curr = cpi_map.get((yr, q))
        prev = cpi_map.get((yr - 1, q))
        if curr and prev and prev != 0:
            result[(yr, q)] = round((curr / prev - 1) * 100, 4)
        else:
            result[(yr, q)] = None
    return result


# ── 메인 ──────────────────────────────────────────────────────

def collect_macro(start_year=2015, end_year=2025, api_key=None,
                  include_vix=True, delay=0.5):
    key = _get_api_key(api_key)

    # 직전분기말 + 전년동기 확보를 위해 2년 여유
    start_d = f"{start_year - 1}1201"
    end_d   = f"{end_year}1231"

    quarters = [
        (yr, q)
        for yr in range(start_year, end_year + 1)
        for q in ["Q1", "H1", "Q3", "ANNUAL"]
    ]

    print(f"\n수집 범위: {start_year}~{end_year} ({len(quarters)}개 분기)")
    print("=" * 50)

    print("[1/6] 신용스프레드")
    spread = fetch_credit_spread(key, start_d, end_d, quarters)
    time.sleep(delay)

    print("[2/6] 코스닥 수익률")
    kosdaq = fetch_kosdaq_return(key, start_d, end_d, quarters)
    time.sleep(delay)

    print("[3/6] GDP 성장률")
    gdp = fetch_gdp_growth(key, start_year, end_year, quarters)
    time.sleep(delay)

    print("[4/6] 환율 전기비 변화율")
    usd = fetch_usdkrw_chg(key, start_d, end_d, quarters)
    time.sleep(delay)

    vix = {}
    if include_vix:
        print("[5/6] VIX")
        try:
            vix = fetch_vix_avg(f"{start_year}-01-01", f"{end_year}-12-31", quarters)
        except Exception as e:
            print(f"  [경고] VIX 실패: {e}")
    else:
        print("[5/6] VIX → 건너뜀")

    print("[6/6] CPI")
    cpi = fetch_cpi_yoy(key, start_year, end_year, quarters)
    time.sleep(delay)

    rows = []
    for (yr, q) in quarters:
        rows.append({
            "year":           yr,
            "quarter":        q,
            "credit_spread":  spread.get((yr, q)),
            "kosdaq_return":  kosdaq.get((yr, q)),
            "gdp_growth_yoy": gdp.get((yr, q)),
            "usdkrw_chg":     usd.get((yr, q)),
            "vix_avg":        vix.get((yr, q)) if include_vix else None,
            "cpi_yoy":        cpi.get((yr, q)),
        })

    df = pd.DataFrame(rows)

    print("\n=== 결측률 ===")
    for col in ["credit_spread", "kosdaq_return", "gdp_growth_yoy",
                "usdkrw_chg", "vix_avg", "cpi_yoy"]:
        n   = df[col].isna().sum()
        pct = n / len(df) * 100
        print(f"  {col:<22}: {n}건 ({pct:.0f}%)")

    return df


def save_macro(df, out_path=OUTPUT_FILE):
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n저장 완료: {out_path}")
    return out_path


def merge_macro_to_ratio(ratio_df, macro_path=OUTPUT_FILE):
    macro_path = Path(macro_path)
    if not macro_path.exists():
        raise FileNotFoundError(f"거시경제 데이터 없음: {macro_path}")
    macro_df = pd.read_csv(macro_path)
    macro_df["year"] = macro_df["year"].astype(str)
    ratio_df = ratio_df.copy()
    ratio_df["year"] = ratio_df["year"].astype(str)
    return ratio_df.merge(macro_df, on=["year", "quarter"], how="left")


def main():
    parser = argparse.ArgumentParser(description="거시경제 데이터 수집기 (ECOS + yfinance)")
    parser.add_argument("--api-key")
    parser.add_argument("--start",  type=int, default=2015)
    parser.add_argument("--end",    type=int, default=2025)
    parser.add_argument("--no-vix", action="store_true")
    parser.add_argument("--delay",  type=float, default=0.5)
    parser.add_argument("--output", default=str(OUTPUT_FILE))
    args = parser.parse_args()

    try:
        df = collect_macro(
            start_year  = args.start,
            end_year    = args.end,
            api_key     = args.api_key,
            include_vix = not args.no_vix,
            delay       = args.delay,
        )
        save_macro(df, args.output)
        print("\n앞 8행 미리보기:")
        print(df.head(8).to_string(index=False))
        return 0
    except Exception as e:
        print(f"\n오류: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())