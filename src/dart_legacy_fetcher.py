"""dart-fss 기반 2015년 이전 재무제표 수집.

DART OpenAPI의 fnlttSinglAcntAll은 2015년 이후만 지원하므로,
dart-fss 라이브러리를 통해 XBRL/HTML 원문에서 재무제표를 추출한다.

추출 결과를 기존 account_mapper가 기대하는 dict 형식으로 변환하여
동일한 ratio_calculator 파이프라인을 재사용한다.
"""

from __future__ import annotations

import sys
import warnings
from typing import Any

from .dart_api import DartApiError, get_api_key

# dart-fss lazy import (설치 안 된 환경 대비)
_dart_fss = None
_dart_api_key_set = False


def _ensure_dart_fss(api_key: str) -> Any:
    """dart-fss 모듈을 import하고 API 키를 설정."""
    global _dart_fss, _dart_api_key_set
    if _dart_fss is None:
        try:
            import dart_fss
            _dart_fss = dart_fss
        except ImportError:
            raise DartApiError(
                "dart-fss 라이브러리가 필요합니다: pip install dart-fss"
            )
    if not _dart_api_key_set:
        _dart_fss.set_api_key(api_key)
        _dart_api_key_set = True
    return _dart_fss


# ── sj_div 매핑 ──────────────────────────────────────────────
# dart-fss의 fs_tp → 기존 account_mapper의 sj_div 매핑
_FS_TYPE_TO_SJ_DIV = {
    "bs": "BS",
    "is": "IS",
    "cis": "CIS",
    "cf": "CF",
}


def _safe_str_amount(val: Any) -> str | None:
    """숫자 값을 문자열로 안전하게 변환. NaN/None/빈값은 None."""
    if val is None:
        return None
    # NaN 체크 (float('nan') != float('nan'))
    try:
        if val != val:
            return None
        return str(int(float(val)))
    except (ValueError, TypeError):
        return None


def _extract_date_columns(df: Any) -> list[tuple]:
    """DataFrame에서 날짜(값) 컬럼만 추출.

    dart-fss DataFrame의 컬럼은 MultiIndex이며:
    - 메타 컬럼: (report_title, 'concept_id'), (report_title, 'label_ko'), ...
    - 값 컬럼: ('20111231', ('별도재무제표',)) 또는 ('20110101-20111231', ('별도재무제표',))
    """
    date_cols = []
    for col in df.columns.tolist():
        sub = col[1] if len(col) > 1 else ""
        # 메타 컬럼은 문자열, 값 컬럼은 튜플
        if isinstance(sub, tuple):
            date_cols.append(col)
    return date_cols


def _date_col_to_year(col: tuple) -> str:
    """날짜 컬럼에서 연도 추출.

    BS: ('20111231', ...) → '2011'
    IS/CF: ('20110101-20111231', ...) → '2011'
    """
    date_str = col[0]
    if "-" in date_str:
        # 기간 형식: '20110101-20111231' → 끝 날짜 기준
        date_str = date_str.split("-")[1]
    return date_str[:4]


def _df_to_dart_items(
    df: Any,
    sj_div: str,
    target_year: str,
) -> list[dict[str, Any]]:
    """dart-fss DataFrame → 기존 DART API 호환 dict 리스트로 변환.

    기존 account_mapper.extract_standard_items()가 기대하는 형식:
    {
        "sj_div": "BS",
        "account_nm": "자산총계",
        "thstrm_amount": "12345678",   # 당기
        "frmtrm_amount": "11111111",   # 전기
        "bfefrmtrm_amount": "9999999", # 전전기
    }
    """
    if df is None:
        return []

    date_cols = _extract_date_columns(df)
    if not date_cols:
        return []

    # 날짜 컬럼을 연도 기준으로 정렬 (최신 → 과거)
    date_cols_with_year = [(col, _date_col_to_year(col)) for col in date_cols]
    date_cols_with_year.sort(key=lambda x: x[1], reverse=True)

    # target_year 기준으로 당기/전기/전전기 매핑
    thstrm_col = None
    frmtrm_col = None
    bfefrmtrm_col = None

    for col, yr in date_cols_with_year:
        if yr == target_year:
            thstrm_col = col
        elif thstrm_col is not None and frmtrm_col is None and yr < target_year:
            frmtrm_col = col
        elif frmtrm_col is not None and bfefrmtrm_col is None and yr < target_year:
            bfefrmtrm_col = col

    # target_year에 해당하는 당기 데이터가 없으면
    # 가장 최신 데이터를 당기로 사용
    if thstrm_col is None and date_cols_with_year:
        thstrm_col = date_cols_with_year[0][0]
        if len(date_cols_with_year) > 1:
            frmtrm_col = date_cols_with_year[1][0]
        if len(date_cols_with_year) > 2:
            bfefrmtrm_col = date_cols_with_year[2][0]

    # label_ko 컬럼 찾기
    label_col = None
    for col in df.columns.tolist():
        if isinstance(col[1], str) and col[1] == "label_ko":
            label_col = col
            break

    if label_col is None:
        return []

    items: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        account_nm = row.get(label_col)
        if not account_nm or not isinstance(account_nm, str):
            continue

        item: dict[str, Any] = {
            "sj_div": sj_div,
            "account_nm": account_nm.strip(),
        }

        # 금액 값 추출
        if thstrm_col is not None:
            val = row.get(thstrm_col)
            item["thstrm_amount"] = _safe_str_amount(val)
        else:
            item["thstrm_amount"] = None

        if frmtrm_col is not None:
            val = row.get(frmtrm_col)
            item["frmtrm_amount"] = _safe_str_amount(val)
        else:
            item["frmtrm_amount"] = None

        if bfefrmtrm_col is not None:
            val = row.get(bfefrmtrm_col)
            item["bfefrmtrm_amount"] = _safe_str_amount(val)
        else:
            item["bfefrmtrm_amount"] = None

        items.append(item)

    return items


def fetch_legacy_financial_statements(
    api_key: str,
    corp_code: str,
    year: str,
    fs_div: str = "CFS",
) -> list[dict[str, Any]]:
    """dart-fss를 사용하여 2015년 이전 재무제표를 수집.

    Args:
        api_key: DART API 키
        corp_code: DART 고유번호 (8자리)
        year: 수집 연도 (예: "2012")
        fs_div: "CFS" (연결) 또는 "OFS" (별도)

    Returns:
        기존 DART API 호환 형식의 재무제표 항목 리스트.
        데이터가 없으면 빈 리스트.
    """
    dart = _ensure_dart_fss(api_key)
    separate = fs_div != "CFS"

    bgn_de = f"{year}0101"
    end_de = f"{year}1231"

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        # dart-fss 내부에서 traceback을 stderr에 직접 출력하므로 억제
        import io
        import logging
        logging.disable(logging.CRITICAL)
        _old_stderr = sys.stderr
        sys.stderr = io.StringIO()
        try:
            fs = dart.extract(
                corp_code=corp_code,
                bgn_de=bgn_de,
                end_de=end_de,
                fs_tp=("bs", "is", "cis", "cf"),
                separate=separate,
                report_tp="annual",
                separator=False,
                progressbar=False,
                skip_error=True,
                dataset="xbrl",
            )
        except Exception as e:
            err_name = type(e).__name__
            # 연결재무제표 없음 → 빈 리스트 반환 (collector에서 OFS 폴백 처리)
            if "NotFoundConsolidated" in err_name or "Consolidated" in str(e):
                return []
            raise DartApiError(f"dart-fss 추출 오류 ({year}): {e}") from e
        finally:
            sys.stderr = _old_stderr
            logging.disable(logging.NOTSET)

    if fs is None:
        return []

    # 모든 재무제표 유형의 데이터를 합산
    all_items: list[dict[str, Any]] = []
    for fs_tp, sj_div in _FS_TYPE_TO_SJ_DIV.items():
        df = fs._statements.get(fs_tp)
        if df is not None:
            items = _df_to_dart_items(df, sj_div, year)
            all_items.extend(items)

    return all_items
