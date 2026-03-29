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
from .dart_legacy_fetcher import fetch_legacy_financial_statements
from .ratio_calculator import RATIO_NAMES, compute_all_ratios
from .s3_uploader import upload_batch_to_s3

# ── 기본 경로 ─────────────────────────────────────────────────
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
OUTPUT_DIR = DATA_DIR / "output"
INPUT_DIR = DATA_DIR / "input"
RAW_DIR = DATA_DIR / "raw"
COLLECTED_CSV = INPUT_DIR / "companies_collected.csv"

# ── 수집 완료 기업 기록 ───────────────────────────────────────
COLLECTED_FIELDNAMES = [
    "stock_code", "corp_name", "label", "gics_sector", "start_year", "end_year",
]


def _record_collected_companies(
    companies: list[dict[str, str]],
    collected_stock_codes: set[str],
) -> None:
    """수집이 완료된 기업 정보를 companies_collected.csv에 누적 기록.

    이미 기록된 종목코드는 중복 추가하지 않습니다.
    """
    if not collected_stock_codes:
        return

    # 기존 파일에서 이미 기록된 종목코드 로드
    existing_codes: set[str] = set()
    existing_rows: list[dict[str, str]] = []
    if COLLECTED_CSV.exists():
        with open(COLLECTED_CSV, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                sc = (row.get("stock_code") or "").strip()
                existing_codes.add(sc)
                existing_rows.append(row)

    # 새로 기록할 기업만 필터
    new_entries: list[dict[str, str]] = []
    for comp in companies:
        sc = comp.get("stock_code", "").strip()
        if sc in collected_stock_codes and sc not in existing_codes:
            new_entries.append({
                "stock_code": sc,
                "corp_name": comp.get("corp_name", ""),
                "label": comp.get("label", ""),
                "gics_sector": comp.get("gics_sector", "Unknown"),
                "start_year": comp.get("start_year", ""),
                "end_year": comp.get("end_year", ""),
            })

    if not new_entries:
        return

    # 기존 + 신규 합쳐서 전체 다시 쓰기
    all_rows = existing_rows + new_entries
    with open(COLLECTED_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=COLLECTED_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    for entry in new_entries:
        print(
            f"  📋 수집 기록: {entry['corp_name'] or entry['stock_code']} "
            f"→ {COLLECTED_CSV.name}",
            file=sys.stderr,
        )


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


def _sector_dir(save_dir: Path, gics_sector: str) -> Path:
    """GICS 섹터 서브디렉터리 경로를 반환."""
    return save_dir / (gics_sector or "Unknown")


def _load_existing_quarters(
    save_dir: Path,
    stock_code: str,
    year: str,
    gics_sector: str = "Unknown",
) -> set[str]:
    """기존 CSV에서 이미 수집된 분기 목록을 반환.

    Returns:
        {"Q1", "H1"} 형태의 set. 파일이 없으면 빈 set.
    """
    filepath = _sector_dir(save_dir, gics_sector) / f"{stock_code}_{year}.csv"
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
    gics_sector: str = "Unknown",
) -> list[dict[str, Any]]:
    """기존 CSV 파일의 모든 행을 읽어서 반환.

    Returns:
        행 리스트. 파일이 없으면 빈 리스트.
    """
    filepath = _sector_dir(save_dir, gics_sector) / f"{stock_code}_{year}.csv"
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
      gics_sector  – GICS 섹터 영문명 (예: Materials)
      start_year   – 수집 시작 연도 (예: 2020). --years 대신 기업별 범위 지정
      end_year     – 수집 종료 연도 (예: 2023). start_year와 함께 사용
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"기업 목록 파일이 없습니다: {csv_path}")

    rows: list[dict[str, str]] = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({k.strip(): (v or "").strip() for k, v in row.items()})
    return rows


# ── 2015년 이전 데이터 수집 기준 연도 ────────────────────────
LEGACY_CUTOFF_YEAR = 2015


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


def collect_single_legacy(
    api_key: str,
    corp_code: str,
    stock_code: str,
    corp_name: str,
    year: str,
    fs_div: str = "CFS",
    save_raw: bool = False,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """dart-fss를 사용하여 2015년 이전 재무비율을 수집.

    dart-fss는 사업보고서(ANNUAL) 단위로 추출하므로 분기 구분 없이
    ANNUAL로 고정하여 수집합니다.

    Returns:
        (ratio_row, raw_items) 튜플.
    """
    try:
        raw_items = fetch_legacy_financial_statements(
            api_key, corp_code, year, fs_div
        )
    except DartApiError as e:
        print(f"  ⚠ dart-fss 오류 ({corp_name} {year}): {e}", file=sys.stderr)
        raw_items = []

    if raw_items and save_raw:
        _save_raw_json(raw_items, stock_code, year, "ANNUAL", fs_div)

    if not raw_items:
        ratios = {name: None for name in RATIO_NAMES}
    else:
        std_items = extract_standard_items(raw_items)
        ratios = compute_all_ratios(std_items)

    row: dict[str, Any] = {
        "stock_code": stock_code,
        "corp_name": corp_name,
        "year": year,
        "quarter": "ANNUAL",
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

    # ── 기업별 연도 범위 결정 헬퍼 ────────────────────────────
    def _resolve_years(comp: dict[str, str]) -> list[str]:
        """CSV의 start_year/end_year가 있으면 기업별 범위, 없으면 글로벌 years 사용."""
        sy = comp.get("start_year", "").strip()
        ey = comp.get("end_year", "").strip()
        if sy and ey:
            return [str(y) for y in range(int(sy), int(ey) + 1)]
        return years  # CLI --years 또는 기본값

    # ── 중복 체크를 위한 기존 데이터 로드 ───────────────────────
    # existing_quarters[(stock_code, year)] = {"Q1", "H1", ...}
    existing_quarters: dict[tuple[str, str], set[str]] = {}
    if not force:
        for comp in companies:
            sc = comp.get("stock_code", "")
            gics = comp.get("gics_sector", "Unknown")
            if not sc:
                continue
            for yr in _resolve_years(comp):
                eq = _load_existing_quarters(save_dir, sc, yr, gics)
                if eq:
                    existing_quarters[(sc, yr)] = eq

    # ── 결과 수집 ──────────────────────────────────────────────
    new_rows: list[dict[str, Any]] = []
    s3_upload_queue: list[dict[str, Any]] = []
    total = sum(len(_resolve_years(c)) * len(quarters) for c in companies)
    done = 0
    skipped = 0

    for comp in companies:
        cc = comp.get("corp_code", "")
        sc = comp.get("stock_code", "")
        cn = comp.get("corp_name", "")
        gics = comp.get("gics_sector", "Unknown")
        comp_years = _resolve_years(comp)
        if not cc:
            print(f"  ⏭ 건너뜀 (corp_code 없음): {sc} {cn}", file=sys.stderr)
            done += len(comp_years) * len(quarters)
            continue

        for yr in comp_years:
            is_legacy = int(yr) < LEGACY_CUTOFF_YEAR

            if is_legacy:
                # 2015년 이전: dart-fss로 사업보고서(ANNUAL) 단위 수집
                done += len(quarters)  # legacy는 분기 루프 대신 연도 단위

                # ── 중복 체크 ──
                if not force and "ANNUAL" in existing_quarters.get((sc, yr), set()):
                    print(
                        f"  [{done}/{total}] {cn or sc} {yr}-ANNUAL ... "
                        f"⏭ 이미 수집됨 (SKIP)",
                        file=sys.stderr,
                    )
                    skipped += len(quarters)
                    continue

                print(
                    f"  [{done}/{total}] {cn or sc} {yr}-ANNUAL ... "
                    f"수집 중 (dart-fss)",
                    file=sys.stderr,
                )

                # CFS 시도 → 실패하면 OFS 폴백
                row, raw_items = collect_single_legacy(
                    key, cc, sc, cn, yr, fs_div, save_raw=save_raw,
                )
                if fs_div == "CFS" and all(row.get(n) is None for n in RATIO_NAMES):
                    row, raw_items = collect_single_legacy(
                        key, cc, sc, cn, yr, "OFS", save_raw=save_raw,
                    )

                row["label"] = comp.get("label", "")
                row["gics_sector"] = gics
                new_rows.append(row)

                if upload_s3 and raw_items:
                    s3_upload_queue.append({
                        "raw_items": raw_items,
                        "stock_code": sc,
                        "year": yr,
                        "quarter": "ANNUAL",
                        "gics_sector": gics,
                        "label": comp.get("label", ""),
                    })

                if delay > 0:
                    time.sleep(delay)
                continue

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
                row["gics_sector"] = gics  # 섹터별 디렉터리 저장용
                new_rows.append(row)

                # S3 업로드 대기열에 추가
                if upload_s3 and raw_items:
                    s3_upload_queue.append({
                        "raw_items": raw_items,
                        "stock_code": sc,
                        "year": yr,
                        "quarter": q,
                        "gics_sector": gics,
                        "label": comp.get("label", ""),
                    })

                if delay > 0:
                    time.sleep(delay)

    # ── CSV 저장: 기존 데이터 + 신규 데이터 병합 ──────────────
    # 신규 데이터를 (stock_code, year, gics_sector) 기준으로 그룹핑
    new_groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in new_rows:
        grp_key = (row["stock_code"], row["year"], row.get("gics_sector", "Unknown"))
        new_groups[grp_key].append(row)

    saved_files: list[Path] = []
    quarter_order = list(REPORT_CODES.keys())  # Q1, H1, Q3, ANNUAL

    # 신규 데이터가 있는 파일만 저장 (섹터별 서브디렉터리)
    for (sc, yr, gics), rows in new_groups.items():
        sector_path = _sector_dir(save_dir, gics)
        sector_path.mkdir(parents=True, exist_ok=True)

        # 기존 CSV 행 로드 (force면 무시)
        if force:
            merged = rows
        else:
            existing_rows = _load_existing_rows(save_dir, sc, yr, gics)
            # 기존 분기 + 신규 분기 병합
            existing_q_set = {r.get("quarter") for r in existing_rows}
            merged = list(existing_rows)
            for r in rows:
                if r["quarter"] not in existing_q_set:
                    merged.append(r)

        # 모든 행의 재무비율 값이 전부 비어있으면 파일 생성 건너뛰기
        has_any_value = any(
            row.get(name) is not None and row.get(name) != ""
            for row in merged
            for name in RATIO_NAMES
        )
        if not has_any_value:
            print(f"  ⏭ {sc}_{yr}.csv 건너뜀 (데이터 없음)", file=sys.stderr)
            continue

        # 분기 순서대로 정렬
        merged.sort(key=lambda r: quarter_order.index(r.get("quarter", "")) if r.get("quarter", "") in quarter_order else 99)

        filename = f"{sc}_{yr}.csv"
        filepath = sector_path / filename
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

    # ── 수집 완료 기업 기록 ───────────────────────────────────
    if saved_files and companies_csv:
        # CSV 저장이 실제로 이루어진 종목코드만 기록
        collected_codes = {row["stock_code"] for row in new_rows}
        _record_collected_companies(companies, collected_codes)

    return saved_files


# ── 누락 기업 목록 생성 ──────────────────────────────────────────
MISSING_CSV = INPUT_DIR / "companies_missing.csv"
MISSING_LEGACY_CSV = INPUT_DIR / "companies_missing_legacy.csv"


def _scan_actual_files(save_dir: Path) -> dict[str, set[int]]:
    """output 디렉터리에서 실제 존재하는 CSV 파일을 스캔.

    Returns:
        {stock_code: {2015, 2016, ...}} 형태의 딕셔너리
    """
    actual: dict[str, set[int]] = {}
    for csv_file in save_dir.rglob("*.csv"):
        parts = csv_file.stem.split("_")
        if len(parts) == 2:
            sc, yr = parts
            try:
                actual.setdefault(sc, set()).add(int(yr))
            except ValueError:
                continue
    return actual


def generate_missing_csv(
    output_dir: Path | None = None,
) -> Path:
    """companies_collected.csv와 output 디렉터리를 비교하여
    전체 누락 연도(legacy + 2015 이후 모두)가 있는 기업의 CSV를 생성.

    누락 연도가 연속되지 않을 수 있으므로(예: 2011-2014 + 2023-2025),
    기업별로 여러 행을 생성하여 연속 구간별로 분리합니다.

    생성 파일: data/input/companies_missing.csv

    Returns:
        생성된 CSV 파일 경로
    """
    if not COLLECTED_CSV.exists():
        raise FileNotFoundError(
            f"{COLLECTED_CSV}가 없습니다. 먼저 collect 명령으로 데이터를 수집하세요."
        )

    save_dir = output_dir if output_dir else OUTPUT_DIR

    # collected CSV 로드
    companies: list[dict[str, str]] = []
    with open(COLLECTED_CSV, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            companies.append({k.strip(): (v or "").strip() for k, v in row.items()})

    actual_files = _scan_actual_files(save_dir)

    # 누락 기업 추출 (전체 연도)
    missing_entries: list[dict[str, str]] = []
    for comp in companies:
        sc = comp.get("stock_code", "").strip()
        sy = comp.get("start_year", "").strip()
        ey = comp.get("end_year", "").strip()
        if not sc or not sy or not ey:
            continue

        start = int(sy)
        end = int(ey)
        if start > end:
            continue

        expected = set(range(start, end + 1))
        actual = actual_files.get(sc, set())
        missing_years = sorted(expected - actual)

        if not missing_years:
            continue

        # 연속 구간으로 분리 (예: [2011,2012,2013,2023,2024] → [(2011,2013), (2023,2024)])
        ranges: list[tuple[int, int]] = []
        range_start = missing_years[0]
        prev = missing_years[0]
        for yr in missing_years[1:]:
            if yr != prev + 1:
                ranges.append((range_start, prev))
                range_start = yr
            prev = yr
        ranges.append((range_start, prev))

        for rs, re in ranges:
            missing_entries.append({
                "stock_code": sc,
                "corp_name": comp.get("corp_name", ""),
                "label": comp.get("label", ""),
                "gics_sector": comp.get("gics_sector", "Unknown"),
                "start_year": str(rs),
                "end_year": str(re),
            })

    # CSV 저장
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(MISSING_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=COLLECTED_FIELDNAMES)
        writer.writeheader()
        writer.writerows(missing_entries)

    total_years = sum(
        int(e["end_year"]) - int(e["start_year"]) + 1 for e in missing_entries
    )

    # legacy / modern 구분 집계
    legacy_years = sum(
        min(int(e["end_year"]), LEGACY_CUTOFF_YEAR - 1) - int(e["start_year"]) + 1
        for e in missing_entries
        if int(e["start_year"]) < LEGACY_CUTOFF_YEAR
    )
    modern_years = total_years - legacy_years

    print(
        f"📋 누락 목록 생성: {MISSING_CSV}\n"
        f"   {len(missing_entries)}개 구간, 총 {total_years}개 연도"
        f" (legacy: {legacy_years}, modern: {modern_years})",
        file=sys.stderr,
    )

    # 상세 출력
    for entry in missing_entries:
        is_legacy = int(entry["start_year"]) < LEGACY_CUTOFF_YEAR
        tag = "[legacy]" if is_legacy else "[modern]"
        print(
            f"  {tag:9} {entry['stock_code']} {entry['corp_name']:<14} "
            f"{entry['gics_sector']:<25} {entry['start_year']}-{entry['end_year']}",
            file=sys.stderr,
        )

    return MISSING_CSV


def generate_missing_legacy_csv(
    output_dir: Path | None = None,
) -> Path:
    """companies_collected.csv와 output 디렉터리를 비교하여
    2015년 이전 누락 연도가 있는 기업만 추출한 CSV를 생성.

    생성 파일: data/input/companies_missing_legacy.csv

    Returns:
        생성된 CSV 파일 경로
    """
    if not COLLECTED_CSV.exists():
        raise FileNotFoundError(
            f"{COLLECTED_CSV}가 없습니다. 먼저 collect 명령으로 데이터를 수집하세요."
        )

    save_dir = output_dir if output_dir else OUTPUT_DIR

    companies: list[dict[str, str]] = []
    with open(COLLECTED_CSV, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            companies.append({k.strip(): (v or "").strip() for k, v in row.items()})

    actual_files = _scan_actual_files(save_dir)

    # 누락 기업 추출 (2015년 이전만)
    missing_entries: list[dict[str, str]] = []
    for comp in companies:
        sc = comp.get("stock_code", "").strip()
        sy = comp.get("start_year", "").strip()
        ey = comp.get("end_year", "").strip()
        if not sc or not sy or not ey:
            continue

        start = int(sy)
        end = min(int(ey), LEGACY_CUTOFF_YEAR - 1)
        if start > end:
            continue

        expected = set(range(start, end + 1))
        actual = actual_files.get(sc, set())
        missing_years = sorted(expected - actual)

        if not missing_years:
            continue

        # 연속 구간으로 분리 (예: [2005,2006,2010,2011] → [(2005,2006), (2010,2011)])
        ranges: list[tuple[int, int]] = []
        range_start = missing_years[0]
        prev = missing_years[0]
        for yr in missing_years[1:]:
            if yr != prev + 1:
                ranges.append((range_start, prev))
                range_start = yr
            prev = yr
        ranges.append((range_start, prev))

        for rs, re in ranges:
            missing_entries.append({
                "stock_code": sc,
                "corp_name": comp.get("corp_name", ""),
                "label": comp.get("label", ""),
                "gics_sector": comp.get("gics_sector", "Unknown"),
                "start_year": str(rs),
                "end_year": str(re),
            })

    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(MISSING_LEGACY_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=COLLECTED_FIELDNAMES)
        writer.writeheader()
        writer.writerows(missing_entries)

    total_years = sum(
        int(e["end_year"]) - int(e["start_year"]) + 1 for e in missing_entries
    )
    print(
        f"📋 누락 기업 목록 생성 (legacy): {MISSING_LEGACY_CSV}\n"
        f"   {len(missing_entries)}개 기업, 총 {total_years}개 연도",
        file=sys.stderr,
    )

    for entry in missing_entries:
        print(
            f"  {entry['stock_code']} {entry['corp_name']:<14} "
            f"{entry['gics_sector']:<25} {entry['start_year']}-{entry['end_year']}",
            file=sys.stderr,
        )

    return MISSING_LEGACY_CSV
