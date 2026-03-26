#!/usr/bin/env python3
"""통합 수집 파이프라인.

엑셀 파일을 data/etc/에 배치한 뒤, 섹터와 기업 상태를 지정하면
기업 목록 생성 → 재무제표 수집 → S3 업로드를 한 번에 수행한다.

사전 준비
─────────
1. data/etc/상장폐지현황.xlsx     (KRX 상장폐지현황)
2. data/etc/corp_codes.xml       (DART 고유번호 전체 — 상폐 기업 사용 시)
3. data/input/krx_all_companies.xlsx  (KRX 전종목 — 정상 기업 사용 시)

사용 예시
─────────
# 정상 기업만, IT + Communication Services 섹터
python3 run_pipeline.py --status normal \
    --sectors "Information Technology" "Communication Services" \
    --member hann

# 상폐 기업만, Consumer Discretionary 섹터
python3 run_pipeline.py --status delisted \
    --sectors "Consumer Discretionary" \
    --member hann

# 정상 + 상폐 모두, 전체 섹터
python3 run_pipeline.py --status all --member hann

# S3 업로드 건너뛰기 (수집만)
python3 run_pipeline.py --status normal \
    --sectors "Information Technology" \
    --member hann --skip-s3

# dry-run: 수집 대상 기업 목록만 확인
python3 run_pipeline.py --status delisted \
    --sectors "Information Technology" \
    --member hann --dry-run
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import pandas as pd

# ── 경로 설정 ──────────────────────────────────────────────────
DATA_DIR = Path("data")
ETC_DIR = DATA_DIR / "etc"
INPUT_DIR = DATA_DIR / "input"

KRX_XLSX = INPUT_DIR / "krx_all_companies.xlsx"
DELISTED_XLSX = ETC_DIR / "상장폐지현황.xlsx"
CORP_CODES_XML = ETC_DIR / "corp_codes.xml"
INDUTY_CACHE = ETC_DIR / "delisted_induty_codes.csv"

# 파이프라인이 생성하는 임시 기업 목록
PIPELINE_CSV = INPUT_DIR / "pipeline_companies.csv"

COLUMNS = ["stock_code", "corp_name", "label", "gics_sector", "start_year", "end_year"]


def _run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    print(f"  $ {' '.join(cmd)}")
    return subprocess.run(cmd, check=check, text=True)


# ── 정상 기업 목록 생성 ──────────────────────────────────────────
def _build_normal_companies(sectors: list[str] | None) -> pd.DataFrame:
    """KRX 엑셀에서 정상 기업 목록을 생성 (f_make_A_input 로직 내장)."""
    if not KRX_XLSX.exists():
        print(f"KRX 전종목 파일이 없습니다: {KRX_XLSX}")
        print("KRX 정보데이터시스템에서 다운로드하세요.")
        sys.exit(1)

    # f_make_A_input.py의 매핑 로직을 임포트
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import f_make_A_input as fma

    df = pd.read_excel(str(KRX_XLSX), dtype={"종목코드": str})
    df = df[~df["회사명"].str.contains("스펙", na=False)]
    df = df[df["업종"].apply(fma.is_A_sector)]
    df = df[~df["업종"].isin(fma.EXCLUDE_INDUSTRIES_STRICT)]

    df = df[["종목코드", "회사명", "업종"]].copy()
    df.columns = ["stock_code", "corp_name", "industry"]
    df["gics_sector"] = df["industry"].apply(fma.map_gics)
    df = df[df["gics_sector"].notna()]
    df = df[df["stock_code"].str.match(r"^\d{6}$")]

    df["label"] = 0
    df["start_year"] = fma.START_YEAR
    df["end_year"] = fma.END_YEAR

    df = df[COLUMNS]

    if sectors:
        df = df[df["gics_sector"].isin(sectors)]

    return df


# ── 상폐 기업 목록 생성 ──────────────────────────────────────────
def _ensure_induty_cache() -> None:
    """업종코드 캐시가 없으면 f_fetch_induty_codes.py 실행."""
    if INDUTY_CACHE.exists():
        print(f"  업종코드 캐시 사용: {INDUTY_CACHE}")
        return

    print("  업종코드 캐시 없음 → f_fetch_induty_codes.py 실행 (DART API 호출, 시간 소요)")
    if not CORP_CODES_XML.exists():
        print(f"  corp_codes.xml이 없습니다: {CORP_CODES_XML}")
        print("  DART OpenAPI에서 다운로드하세요.")
        sys.exit(1)

    _run(["python3", "f_fetch_induty_codes.py"])

    if not INDUTY_CACHE.exists():
        print("  업종코드 캐시 생성 실패.")
        sys.exit(1)


def _build_delisted_companies(sectors: list[str] | None) -> pd.DataFrame:
    """상폐 기업 목록을 생성 (f_make_delisted_input 로직 내장)."""
    if not DELISTED_XLSX.exists():
        print(f"상장폐지현황 파일이 없습니다: {DELISTED_XLSX}")
        sys.exit(1)

    _ensure_induty_cache()

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import f_make_delisted_input as fmd

    # 업종코드 로드
    df_induty = pd.read_csv(str(INDUTY_CACHE), dtype={"종목코드": str, "induty_code": str})

    # 상폐 기업 로드
    df_del = pd.read_excel(str(DELISTED_XLSX), dtype={"종목코드": str})
    df_del = df_del[["종목코드", "폐지일자", "폐지사유"]]

    # 병합
    df = df_induty.merge(df_del, on="종목코드", how="left")

    # SPAC 제거
    SPAC_KEYWORDS = ["스펙", "기업인수목적", "SPAC"]
    df = df[~df["회사명"].str.contains("|".join(SPAC_KEYWORDS), na=False)]

    # 재무적 리스크 필터링
    df = df[df["폐지사유"].apply(fmd.is_financial_risk)]

    # GICS 매핑
    df["gics_sector"] = df["induty_code"].apply(fmd.map_gics_by_code)
    df = df[df["gics_sector"].notna()]

    # end_year
    df["end_year"] = pd.to_datetime(
        df["폐지일자"], errors="coerce"
    ).dt.year.fillna(2025).astype(int)

    # start_year (2015 이전 폐지 기업 보정)
    START_YEAR = fmd.START_YEAR
    df["start_year"] = df["end_year"].apply(
        lambda ey: min(START_YEAR, ey) if ey < START_YEAR else START_YEAR
    )

    df = df.copy()
    df["stock_code"] = df["종목코드"]
    df["corp_name"] = df["회사명"]
    df["label"] = 1

    df = df[COLUMNS]

    if sectors:
        df = df[df["gics_sector"].isin(sectors)]

    return df


# ── 메인 파이프라인 ──────────────────────────────────────────────
def main() -> int:
    parser = argparse.ArgumentParser(
        description="통합 수집 파이프라인: 기업 목록 생성 → 재무제표 수집 → S3 업로드",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--status",
        required=True,
        choices=["normal", "delisted", "all"],
        help="수집 대상: normal(정상), delisted(상폐), all(전체)",
    )
    parser.add_argument(
        "--sectors",
        nargs="+",
        default=None,
        help="수집할 GICS 섹터 (예: 'Information Technology' 'Consumer Discretionary'). 미지정 시 전체",
    )
    parser.add_argument(
        "--member",
        required=True,
        help="작업자 이름 (S3 로그 기록용)",
    )
    parser.add_argument(
        "--skip-s3",
        action="store_true",
        help="S3 업로드 건너뛰기 (수집만 수행)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="이미 수집된 데이터도 재수집 + S3 덮어쓰기",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="수집 대상 기업 목록만 출력하고 종료",
    )
    args = parser.parse_args()

    # ── 1) 기업 목록 생성 ──
    print("=" * 60)
    print("[1/3] 기업 목록 생성")
    print("=" * 60)

    frames: list[pd.DataFrame] = []

    if args.status in ("normal", "all"):
        print("\n정상 기업 목록 생성 중...")
        df_normal = _build_normal_companies(args.sectors)
        print(f"  정상 기업: {len(df_normal)}개")
        frames.append(df_normal)

    if args.status in ("delisted", "all"):
        print("\n상폐 기업 목록 생성 중...")
        df_delisted = _build_delisted_companies(args.sectors)
        print(f"  상폐 기업: {len(df_delisted)}개")
        frames.append(df_delisted)

    if not frames:
        print("수집 대상 기업이 없습니다.")
        return 1

    df_all = pd.concat(frames, ignore_index=True)
    df_all = df_all.drop_duplicates(subset=["stock_code"], keep="first")

    print(f"\n총 수집 대상: {len(df_all)}개")
    print("\n섹터별 분포:")
    for sector, group in df_all.groupby("gics_sector"):
        normal_cnt = (group["label"] == 0).sum()
        delisted_cnt = (group["label"] == 1).sum()
        print(f"  {sector}: 정상 {normal_cnt} / 상폐 {delisted_cnt}")

    if args.dry_run:
        print(f"\n[dry-run] 기업 목록 미리보기 (상위 20개):")
        print(df_all.head(20).to_string(index=False))
        return 0

    # 기업 목록 CSV 저장
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    df_all.to_csv(str(PIPELINE_CSV), index=False, encoding="utf-8-sig")
    print(f"\n기업 목록 저장: {PIPELINE_CSV}")

    # ── 2) 재무제표 수집 ──
    print("\n" + "=" * 60)
    print("[2/3] 재무제표 수집 (collect.py)")
    print("=" * 60)

    collect_cmd = [
        "python3", "collect.py", "collect",
        "--companies", str(PIPELINE_CSV),
        "--save-raw",
    ]
    if args.force:
        collect_cmd.append("--force")

    result = _run(collect_cmd, check=False)
    if result.returncode != 0:
        print("\n재무제표 수집 중 오류 발생. 로그를 확인하세요.")
        # 수집 실패해도 S3 업로드는 시도 (이미 수집된 데이터가 있을 수 있음)

    # ── 3) S3 업로드 ──
    if args.skip_s3:
        print("\n[skip] S3 업로드 건너뜀 (--skip-s3)")
    else:
        print("\n" + "=" * 60)
        print("[3/3] S3 업로드 (s3_uploader_v2)")
        print("=" * 60)

        # 섹터별로 업로드
        upload_sectors = args.sectors or df_all["gics_sector"].unique().tolist()
        for sector in upload_sectors:
            print(f"\n  섹터: {sector}")
            s3_cmd = [
                "python3", "-m", "src.s3_uploader_v2",
                "--member", args.member,
                "--sector", sector,
            ]
            if args.force:
                s3_cmd.append("--force")
            _run(s3_cmd, check=False)

    # ── 완료 ──
    print("\n" + "=" * 60)
    print("파이프라인 완료")
    print("=" * 60)
    print(f"  대상: {args.status} ({len(df_all)}개 기업)")
    if args.sectors:
        print(f"  섹터: {', '.join(args.sectors)}")
    print(f"  CSV: data/output/{{sector}}/{{ticker}}_{{year}}.csv")
    print(f"  Raw: data/raw/{{ticker}}_{{year}}_{{quarter}}_{{fs_div}}.json")
    if not args.skip_s3:
        print(f"  S3:  s3://kw0ss-raw-data-s3/{{healthy|delisted}}/{{sector}}/...")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
