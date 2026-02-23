"""데이터 수집 오케스트레이터.

기업 목록(CSV or 직접 입력) + 연도 범위 → 분기별 재무비율 CSV 출력.
선택적으로 원본 재무제표 JSON을 로컬(data/raw/) 또는 S3에 저장.
중복 데이터는 자동으로 건너뛰고, 누락 분기만 추가 수집합니다.
"""

from __future__ import annotations

import csv
import json
import sys
import time
from collections import defaultdict
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


# ── 중복 체크: 기존 CSV에서 수집 완료된 분기 목록 로드 ────────
FIELDNAMES = ["stock_code", "corp_name", "year", "quarter", "label"] + RATIO_NAMES


def _load_existing_quarters(
    save_dir: Path,
    stock_code: str,
    year: str,
) -> set[str]:
    """기존 CSV에서 이미 수집된 분기 목록을 반환.

    Returns:
        {"Q1", "H1"} 형태의 set. 파일이 없으면 빈 set.
    """
    filepath = save_dir / f"{stock_code}_{year}.csv"
    if not filepath.exists():
        return set()
    quarters: set[str] = set()
    with open(filepath, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            q = (row.get("quarter") or "").strip()
            if q:
                quarters.add(q)
    return quarters


def _load_existing_rows(
    save_dir: Path,
    stock_code: str,
    year: str,
) -> list[dict[str, Any]]:
    """기존 CSV 파일의 모든 행을 읽어서 반환.

    Returns:
        행 리스트. 파일이 없으면 빈 리스트.
    """
    filepath = save_dir / f"{stock_code}_{year}.csv"
    if not filepath.exists():
        return []
    rows: list[dict[str, Any]] = []
    with open(filepath, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(dict(row))
    return rows


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
    force: bool = False,
) -> list[Path]:
    """
    여러 기업 × 연도 × 분기의 재무비율을 수집하여 CSV 저장.

    파일명 규칙: {종목코드}_{연도}.csv  (예: 019440_2023.csv)
    각 기업 × 연도별로 별도의 CSV 파일로 저장됩니다.
    이미 수집된 (종목코드, 연도, 분기) 조합은 건너뛰고 누락 분기만 추가합니다.

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
        force: True이면 중복 체크를 무시하고 전체 재수집

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

    # ── 저장 디렉터리 결정 ────────────────────────────────────────
    save_dir = output_dir if output_dir else OUTPUT_DIR
    save_dir.mkdir(parents=True, exist_ok=True)

    # ── 중복 체크를 위한 기존 데이터 로드 ───────────────────────
    # existing_quarters[(stock_code, year)] = {"Q1", "H1", ...}
    existing_quarters: dict[tuple[str, str], set[str]] = {}
    if not force:
        for comp in companies:
            sc = comp.get("stock_code", "")
            if not sc:
                continue
            for yr in years:
                eq = _load_existing_quarters(save_dir, sc, yr)
                if eq:
                    existing_quarters[(sc, yr)] = eq

    # ── 결과 수집 ──────────────────────────────────────────────
    new_rows: list[dict[str, Any]] = []
    s3_upload_queue: list[dict[str, Any]] = []
    total = len(companies) * len(years) * len(quarters)
    done = 0
    skipped = 0

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

                # ── 중복 체크 ──
                if not force and q in existing_quarters.get((sc, yr), set()):
                    print(
                        f"  [{done}/{total}] {cn or sc} {yr}-{q} ... "
                        f"⏭ 이미 수집됨 (SKIP)",
                        file=sys.stderr,
                    )
                    skipped += 1
                    continue

                print(f"  [{done}/{total}] {cn or sc} {yr}-{q} ... 수집 중", file=sys.stderr)

                # CFS 시도 → 실패하면 OFS 폴백
                row, raw_items = collect_single(key, cc, sc, cn, yr, q, fs_div, save_raw=save_raw)
                if fs_div == "CFS" and all(row.get(n) is None for n in RATIO_NAMES):
                    row, raw_items = collect_single(key, cc, sc, cn, yr, q, "OFS", save_raw=save_raw)

                row["label"] = comp.get("label", "")
                new_rows.append(row)

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

    # ── CSV 저장: 기존 데이터 + 신규 데이터 병합 ──────────────
    # 신규 데이터를 (stock_code, year) 기준으로 그룹핑
    new_groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in new_rows:
        grp_key = (row["stock_code"], row["year"])
        new_groups[grp_key].append(row)

    saved_files: list[Path] = []
    quarter_order = list(REPORT_CODES.keys())  # Q1, H1, Q3, ANNUAL

    # 신규 데이터가 있는 파일만 저장
    for (sc, yr), rows in new_groups.items():
        # 기존 CSV 행 로드 (force면 무시)
        if force:
            merged = rows
        else:
            existing_rows = _load_existing_rows(save_dir, sc, yr)
            # 기존 분기 + 신규 분기 병합
            existing_q_set = {r.get("quarter") for r in existing_rows}
            merged = list(existing_rows)
            for r in rows:
                if r["quarter"] not in existing_q_set:
                    merged.append(r)

        # 분기 순서대로 정렬
        merged.sort(key=lambda r: quarter_order.index(r.get("quarter", "")) if r.get("quarter", "") in quarter_order else 99)

        filename = f"{sc}_{yr}.csv"
        filepath = save_dir / filename
        with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
            writer.writeheader()
            for row in merged:
                writer.writerow(row)
        saved_files.append(filepath)
        print(f"  📄 {filepath}  ({len(merged)}행)", file=sys.stderr)

    collected = len(new_rows)
    print(
        f"\n✅ 완료: 신규 {collected}건 수집, {skipped}건 스킵"
        f"  ({len(saved_files)}개 파일 저장)",
        file=sys.stderr,
    )

    # ── S3 업로드 ─────────────────────────────────────────────
    if upload_s3 and s3_upload_queue:
        print(f"\n☁️  S3 업로드 시작 ({len(s3_upload_queue)}개 파일)...", file=sys.stderr)
        upload_batch_to_s3(
            s3_upload_queue,
            bucket=s3_bucket,
            region=s3_region,
            force=force,
        )

    return saved_files
