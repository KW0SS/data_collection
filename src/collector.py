"""데이터 수집 오케스트레이터.

기업 목록(CSV or 직접 입력) + 연도 범위 → 분기별 재무비율 CSV 출력.
선택적으로 원본 재무제표 JSON을 로컬(data/raw/) 또는 S3에 저장.
"""

from __future__ import annotations

import csv
import json
import sys
import time
from pathlib import Path
from typing import Any

from .dart_api import (
    DartApiError,
    REPORT_CODES,
    fetch_financial_statements,
    get_api_key,
    resolve_corp_code,
)
from .account_mapper import extract_standard_items
from .ratio_calculator import RATIO_NAMES, compute_all_ratios
from .s3_uploader import upload_batch_to_s3

# ── 기본 경로 ─────────────────────────────────────────────────
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
OUTPUT_DIR = DATA_DIR / "output"
INPUT_DIR = DATA_DIR / "input"
RAW_DIR = DATA_DIR / "raw"


def _ensure_dirs(save_raw: bool = False) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    if save_raw:
        RAW_DIR.mkdir(parents=True, exist_ok=True)


def _save_raw_json(
    raw_items: list[dict[str, Any]],
    stock_code: str,
    year: str,
    quarter: str,
    fs_div: str,
) -> None:
    """원본 재무제표 JSON을 data/raw/ 폴더에 저장."""
    filename = f"{stock_code}_{year}_{quarter}_{fs_div}.json"
    path = RAW_DIR / filename
    path.write_text(
        json.dumps(raw_items, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ── CSV 입력 파싱 ─────────────────────────────────────────────
def load_company_list(csv_path: Path) -> list[dict[str, str]]:
    """
    기업 목록 CSV 읽기.

    CSV 컬럼 (헤더 필수):
      stock_code   – 종목코드 (6자리, 예: 005930)
      corp_name    – 기업명 (참고용)
      label        – 0=정상, 1=상폐 (참고용, 수집에는 미사용)

    Optional:
      corp_code    – DART 고유코드 (있으면 API 변환 생략)
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"기업 목록 파일이 없습니다: {csv_path}")

    rows: list[dict[str, str]] = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({k.strip(): (v or "").strip() for k, v in row.items()})
    return rows


# ── 단일 기업·단일 보고서 처리 ────────────────────────────────
def collect_single(
    api_key: str,
    corp_code: str,
    stock_code: str,
    corp_name: str,
    year: str,
    quarter: str,
    fs_div: str = "CFS",
    save_raw: bool = False,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """
    한 기업의 한 분기 재무비율을 수집.

    Returns:
        (ratio_row, raw_items) 튜플.
        ratio_row: {"stock_code": ..., "corp_name": ..., "비율1": val, ...}
        raw_items: DART 원본 데이터 (데이터 없으면 빈 리스트)
    """
    reprt_code = REPORT_CODES[quarter]
    try:
        raw_items = fetch_financial_statements(
            api_key, corp_code, year, reprt_code, fs_div
        )
    except DartApiError as e:
        print(f"  ⚠ API 오류 ({corp_name} {year}-{quarter}): {e}", file=sys.stderr)
        raw_items = []

    if raw_items and save_raw:
        _save_raw_json(raw_items, stock_code, year, quarter, fs_div)

    if not raw_items:
        ratios = {name: None for name in RATIO_NAMES}
    else:
        std_items = extract_standard_items(raw_items)
        ratios = compute_all_ratios(std_items)

    row: dict[str, Any] = {
        "stock_code": stock_code,
        "corp_name": corp_name,
        "year": year,
        "quarter": quarter,
    }
    row.update(ratios)
    return row, raw_items


# ── 배치 수집 ─────────────────────────────────────────────────
def collect_batch(
    stock_codes: list[str] | None = None,
    corp_codes: list[str] | None = None,
    years: list[str] | None = None,
    quarters: list[str] | None = None,
    companies_csv: Path | None = None,
    fs_div: str = "CFS",
    output_dir: Path | None = None,
    api_key: str | None = None,
    delay: float = 0.5,
    save_raw: bool = False,
    upload_s3: bool = False,
    s3_bucket: str | None = None,
    s3_region: str | None = None,
) -> list[Path]:
    """
    여러 기업 × 연도 × 분기의 재무비율을 수집하여 CSV 저장.

    파일명 규칙: {종목코드}_{연도}.csv  (예: 019440_2023.csv)
    각 기업 × 연도별로 별도의 CSV 파일로 저장됩니다.

    사용 방식 두 가지:
    1) companies_csv 지정 → CSV에서 기업 목록 로드
    2) stock_codes/corp_codes 직접 전달

    Args:
        stock_codes: 종목코드 리스트 (예: ["005930", "035720"])
        corp_codes: DART 고유코드 리스트
        years: 수집 연도 리스트 (예: ["2022", "2023"])
        quarters: 분기 리스트 (예: ["Q1", "H1", "Q3", "ANNUAL"])
        companies_csv: 기업 목록 CSV 파일 경로
        fs_div: "CFS" (연결) 또는 "OFS" (별도)
        output_dir: 결과 CSV 저장 디렉터리 (기본: data/output/)
        api_key: DART API 키
        delay: API 호출 간 대기 시간(초)
        save_raw: 원본 재무제표 JSON을 data/raw/에 저장할지 여부
        upload_s3: 원본 재무제표를 S3에 업로드할지 여부
        s3_bucket: S3 버킷 이름 (없으면 .env에서 읽기)
        s3_region: AWS 리전 (없으면 .env에서 읽기)

    Returns:
        저장된 CSV 파일 경로 리스트
    """
    _ensure_dirs(save_raw=save_raw)
    key = get_api_key(api_key)

    if quarters is None:
        quarters = list(REPORT_CODES.keys())
    if years is None:
        years = ["2023"]

    # 기업 목록 구성
    companies: list[dict[str, str]] = []

    if companies_csv:
        companies = load_company_list(companies_csv)
    elif stock_codes:
        for sc in stock_codes:
            companies.append({"stock_code": sc, "corp_name": "", "corp_code": ""})
    elif corp_codes:
        for cc in corp_codes:
            companies.append({"stock_code": "", "corp_name": "", "corp_code": cc})
    else:
        raise ValueError("기업 목록이 없습니다. stock_codes, corp_codes, 또는 companies_csv를 지정하세요.")

    # corp_code 확보
    for comp in companies:
        if not comp.get("corp_code"):
            try:
                comp["corp_code"] = resolve_corp_code(
                    key,
                    stock_code=comp.get("stock_code"),
                    corp_name=comp.get("corp_name") or None,
                )
            except DartApiError as e:
                print(f"  ⚠ 기업코드 확인 실패 ({comp}): {e}", file=sys.stderr)
                comp["corp_code"] = ""

    # 결과 수집
    all_rows: list[dict[str, Any]] = []
    s3_upload_queue: list[dict[str, Any]] = []  # S3 업로드 대기열
    total = len(companies) * len(years) * len(quarters)
    done = 0

    for comp in companies:
        cc = comp.get("corp_code", "")
        sc = comp.get("stock_code", "")
        cn = comp.get("corp_name", "")
        gics = comp.get("gics_sector", "Unknown")
        if not cc:
            print(f"  ⏭ 건너뜀 (corp_code 없음): {sc} {cn}", file=sys.stderr)
            done += len(years) * len(quarters)
            continue

        for yr in years:
            for q in quarters:
                done += 1
                print(f"  [{done}/{total}] {cn or sc} {yr}-{q} ...", file=sys.stderr)

                # CFS 시도 → 실패하면 OFS 폴백
                row, raw_items = collect_single(key, cc, sc, cn, yr, q, fs_div, save_raw=save_raw)
                # CFS에서 데이터가 비어 있으면 OFS로 재시도
                if fs_div == "CFS" and all(row.get(n) is None for n in RATIO_NAMES):
                    row, raw_items = collect_single(key, cc, sc, cn, yr, q, "OFS", save_raw=save_raw)

                row["label"] = comp.get("label", "")
                all_rows.append(row)

                # S3 업로드 대기열에 추가
                if upload_s3 and raw_items:
                    s3_upload_queue.append({
                        "raw_items": raw_items,
                        "stock_code": sc,
                        "year": yr,
                        "quarter": q,
                        "gics_sector": gics,
                    })

                if delay > 0:
                    time.sleep(delay)

    # ── CSV 저장: 기업코드_연도.csv 별로 분리 ──────────────────
    save_dir = output_dir if output_dir else OUTPUT_DIR
    save_dir.mkdir(parents=True, exist_ok=True)

    # (stock_code, year) 기준으로 그룹핑
    from collections import defaultdict
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in all_rows:
        key = (row["stock_code"], row["year"])
        groups[key].append(row)

    fieldnames = ["stock_code", "corp_name", "year", "quarter", "label"] + RATIO_NAMES
    saved_files: list[Path] = []

    for (sc, yr), rows in groups.items():
        filename = f"{sc}_{yr}.csv"
        filepath = save_dir / filename
        with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        saved_files.append(filepath)
        print(f"  📄 {filepath}  ({len(rows)}행)", file=sys.stderr)

    print(f"\n✅ CSV 저장 완료: {save_dir}/  (총 {len(saved_files)}개 파일, {len(all_rows)}행)", file=sys.stderr)

    # ── S3 업로드 ─────────────────────────────────────────────
    if upload_s3 and s3_upload_queue:
        print(f"\n☁️  S3 업로드 시작 ({len(s3_upload_queue)}개 파일)...", file=sys.stderr)
        upload_batch_to_s3(s3_upload_queue, bucket=s3_bucket, region=s3_region)

    return saved_files
