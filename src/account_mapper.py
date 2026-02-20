"""DART 계정과목명 → 표준 키 매핑.

OpenDART에서 반환하는 account_nm은 기업마다 표현이 다를 수 있다.
이 모듈은 다양한 변형을 표준 키로 통합한다.

사용 가능한 표준 키(Standard Key) 목록
────────────────────────────────────────
■ 재무상태표 (BS)
  total_assets           자산총계
  current_assets         유동자산
  non_current_assets     비유동자산
  tangible_assets        유형자산
  intangible_assets      무형자산
  trade_receivables      매출채권
  inventories            재고자산
  cash                   현금및현금성자산
  total_liabilities      부채총계
  current_liabilities    유동부채
  short_term_borrowings  단기차입금
  long_term_borrowings   장기차입금
  bonds_payable          사채
  total_equity           자본총계 (= 자기자본)
  paid_in_capital        납입자본금 (= 자본금)
  retained_earnings      이익잉여금
  capital_surplus        자본잉여금

■ 손익계산서 (IS)
  revenue                매출액
  cost_of_sales          매출원가
  gross_profit           매출총이익
  operating_income       영업이익(손실)
  net_income             당기순이익(손실)
  interest_expense       이자비용

■ 현금흐름표 (CF) – 감가상각비 관련
  depreciation           유형자산감가상각비
  amortization           무형자산상각비
"""

from __future__ import annotations

import re
from typing import Any

# ── 계정명 패턴 → 표준 키 ─────────────────────────────────────
# 각 튜플: (표준키, sj_div 필터 또는 None, 정규식 패턴)
# 매칭 순서가 중요: 먼저 정의된 패턴이 우선.
ACCOUNT_PATTERNS: list[tuple[str, str | None, str]] = [
    # ─── BS (재무상태표) ───
    ("total_assets",          "BS", r"자산\s*총계"),
    ("current_assets",        "BS", r"유동\s*자산$"),
    ("non_current_assets",    "BS", r"비유동\s*자산$"),
    ("tangible_assets",       "BS", r"유형\s*자산$"),
    ("intangible_assets",     "BS", r"무형\s*자산$|영업권\s*이외의\s*무형자산"),
    ("trade_receivables",     "BS", r"매출\s*채권|단기매출채권"),
    ("inventories",           "BS", r"재고\s*자산$"),
    ("cash",                  "BS", r"현금\s*(및|과)\s*현금\s*성?\s*자산"),
    ("total_liabilities",     "BS", r"부채\s*총계"),
    ("current_liabilities",   "BS", r"유동\s*부채$"),
    ("short_term_borrowings", "BS", r"단기\s*차입금"),
    ("long_term_borrowings",  "BS", r"장기\s*차입금"),
    ("bonds_payable",         "BS", r"^사채$"),
    ("total_equity",          "BS", r"자본\s*총계"),
    ("paid_in_capital",       "BS", r"^자본금$|납입\s*자본"),
    ("retained_earnings",     "BS", r"이익\s*잉여금"),
    ("capital_surplus",       "BS", r"자본\s*잉여금"),

    # ─── IS (손익계산서) ───
    ("revenue",               "IS", r"^매출액$|^매출$|^수익\s*\(매출액\)$|^영업\s*수익$|^수익$"),
    ("cost_of_sales",         "IS", r"매출\s*원가"),
    ("gross_profit",          "IS", r"매출\s*총이익|매출\s*총\s*손익"),
    ("operating_income",      "IS", r"영업\s*이익|영업\s*손익"),
    ("net_income",            "IS", r"당기\s*순이익|당기순이익|당기\s*순\s*손익"),
    ("interest_expense",      "IS", r"이자\s*비용"),

    # CIS (포괄손익계산서) 에서도 매출/이익 항목이 나올 수 있음
    ("revenue",               "CIS", r"^매출액$|^매출$|^수익\s*\(매출액\)$|^영업\s*수익$|^수익$"),
    ("cost_of_sales",         "CIS", r"매출\s*원가"),
    ("gross_profit",          "CIS", r"매출\s*총이익|매출\s*총\s*손익"),
    ("operating_income",      "CIS", r"영업\s*이익|영업\s*손익"),
    ("net_income",            "CIS", r"당기\s*순이익|당기순이익|당기\s*순\s*손익"),
    ("interest_expense",      "CIS", r"이자\s*비용"),

    # ─── CF (현금흐름표) – 감가상각비 ───
    ("depreciation",          "CF", r"유형\s*자산\s*감가\s*상각비|감가\s*상각비"),
    ("amortization",          "CF", r"무형\s*자산\s*상각비|무형자산상각비"),
]


def _parse_amount(raw: Any) -> float | None:
    """DART 금액 문자열 → float. 파싱 실패 시 None."""
    if raw is None:
        return None
    s = str(raw).strip().replace(",", "").replace(" ", "")
    if not s or s == "-":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def extract_standard_items(
    dart_items: list[dict[str, Any]],
) -> dict[str, dict[str, float | None]]:
    """
    DART 재무제표 항목 리스트로부터 표준 키별 금액을 추출.

    Returns:
        {
          "standard_key": {
            "thstrm": 당기 금액,
            "frmtrm": 전기 금액,
            "bfefrmtrm": 전전기 금액,
          },
          ...
        }
    """
    result: dict[str, dict[str, float | None]] = {}
    # 이미 매핑된 키는 중복 방지 (먼저 매칭된 것이 우선)
    matched_keys: set[str] = set()

    compiled = [
        (key, sj_div, re.compile(pattern))
        for key, sj_div, pattern in ACCOUNT_PATTERNS
    ]

    for item in dart_items:
        account_nm = (item.get("account_nm") or "").strip()
        sj_div = (item.get("sj_div") or "").strip()
        if not account_nm:
            continue

        for std_key, filter_sj, regex in compiled:
            if std_key in matched_keys:
                continue
            if filter_sj and sj_div != filter_sj:
                continue
            if regex.search(account_nm):
                result[std_key] = {
                    "thstrm": _parse_amount(item.get("thstrm_amount")),
                    "frmtrm": _parse_amount(item.get("frmtrm_amount")),
                    "bfefrmtrm": _parse_amount(item.get("bfefrmtrm_amount")),
                }
                matched_keys.add(std_key)
                break

    return result
