# feat: data + structure update

## 개요
- PR 타입: `both`
- 비교 기준: `main...18-feat-collect-data-before-2015year`
- 총 변경: 143개 파일 (143 files changed, 72929 insertions(+), 62 deletions(-))
- 설명: 데이터 수집과 구조 변경이 함께 포함된 PR입니다.

## 변경 요약

### 변경 배경/동기
DART OpenAPI(`fnlttSinglAcntAll`)는 2015년 이후 데이터만 지원하기 때문에, 기존 파이프라인으로는 2015년 이전 상폐 기업의 재무제표를 수집할 수 없었다. 상폐 예측 모델의 학습 데이터 충분성을 위해 `dart-fss` 라이브러리를 활용한 legacy 데이터 수집 경로를 파이프라인에 통합하고, 누락 데이터를 독립적으로 관리할 수 있는 retry 프로세스를 추가했다.

### 주요 변경 사항
- **2015년 이전 데이터 수집 지원**: `run_pipeline.py`에 `--start-year`/`--end-year` 옵션 추가. 2015 미만 연도는 `dart-fss`(XBRL/HTML 사업보고서) 기반 ANNUAL 단위로 자동 전환 수집
- **누락 데이터 독립 재수집 (`collect.py retry`)**: `companies_collected.csv` vs `data/output/` 비교로 누락 자동 탐지 → `--check-only`로 목록만 확인하거나, `--stock-codes`로 특정 종목만 선택적 재수집 가능. 파이프라인(`run_pipeline.py`)과 분리된 독립 프로세스
- **S3 업로드 경로에 정상/상폐 구분 추가**: S3 키 구조를 `{sector}/...` → `{healthy|delisted}/{sector}/...`로 변경. `label` 필드(0=정상, 1=상폐)를 S3 업로드 큐에 전달하여 자동 분류
- **Industrials 섹터 legacy 데이터 수집**: 11개 상폐 기업의 2000~2014년 데이터 62건 수집 완료 (원본 JSON 포함)
- **AGENTS.md에 수집 테스트 라우팅 추가**: `collect_test` 태스크 정의 및 수집 명령어 추천 3단계 정책 문서화
- **README.md 업데이트**: 추천 실행 순서 가이드, legacy 수집 설명, `collect.py retry` 사용법 추가, 파이프라인 흐름도 3단계로 정리

### 주의할 점
- `dart-fss>=0.4.0` 의존성 추가됨 (`requirements.txt`). 기존 환경에서 `pip install dart-fss` 필요
- S3 키 구조 변경(`healthy/`/`delisted/` 접두어)으로 기존 S3에 업로드된 데이터와 경로가 달라짐. 기존 데이터 마이그레이션이 필요할 수 있음
- `automation_non_s3` 체크에서 log_schema 검증 실패 (2건) — 수집 로그 JSON 스키마 불일치 확인 필요

### 영향 범위
- `run_pipeline.py`: 기존 기능 유지, `--start-year`/`--end-year` 미지정 시 기본값(2015~2025) 그대로 동작
- `collect.py`: 기존 `collect`, `search`, `check-missing`, `collect-legacy` 서브커맨드에 영향 없음. `retry` 서브커맨드만 신규 추가
- `src/s3_uploader.py`: `upload_raw_to_s3()`에 `label` 파라미터 추가 (기본값 `"0"` = healthy). 기존 호출 코드 호환

<details>
<summary>커밋 히스토리</summary>

| hash | date | author | message |
|---|---|---|---|
| `8d99d0f` | 2026-03-29 | hann | test : collect data of before 15 #18 |
| `8fdaafc` | 2026-03-27 | hann | chore : test 2013 Industrial data #18 |
| `07b171e` | 2026-03-27 | hann | docs : AGENTS.md file for adding routing method (collect data) #18 |
| `0808370` | 2026-03-27 | hann | refactor : edit process about missing files for dividing healthy&delisted #18 |
| `5dcd741` | 2026-03-27 | hann | refactor : sync pipeline of adding data before 14 year #18 |

</details>

<details>
<summary>변경 파일 상세</summary>

**src/**
  - `src/collector.py` (수정)
  - `src/s3_uploader.py` (수정)
**data/input/**
  - `data/input/companies_collected.csv` (수정)
  - `data/input/companies_missing.csv` (추가)
  - `data/input/companies_missing_legacy.csv` (수정)
  - `data/input/companies_missing_legacy_industrials_delisted.csv` (추가)
  - `data/input/companies_missing_legacy_industrials_remaining.csv` (추가)
**data/output/** (63건)
  - Industrials: 62건
  - test_single: 1건
**root/**
  - `AGENTS.md` (수정)
  - `README.md` (수정)
  - `collect.py` (수정)
  - `requirements.txt` (수정)
  - `run_pipeline.py` (수정)
**other/**
  - `agents/collect_test.md` (추가)
  - `data/raw/008830_2013_ANNUAL_OFS.json` (추가)
  - `data/raw/024810_2011_ANNUAL_OFS.json` (추가)
  - `data/raw/024810_2012_ANNUAL_CFS.json` (추가)
  - `data/raw/024810_2012_ANNUAL_OFS.json` (추가)
  - `data/raw/024810_2013_ANNUAL_CFS.json` (추가)
  - `data/raw/024810_2013_ANNUAL_OFS.json` (추가)
  - `data/raw/024810_2014_ANNUAL_OFS.json` (추가)
  - `data/raw/028040_2000_ANNUAL_OFS.json` (추가)
  - `data/raw/028040_2002_ANNUAL_CFS.json` (추가)
  - `data/raw/028040_2008_ANNUAL_OFS.json` (추가)
  - `data/raw/028040_2009_ANNUAL_OFS.json` (추가)
  - `data/raw/028040_2010_ANNUAL_OFS.json` (추가)
  - `data/raw/028040_2011_ANNUAL_OFS.json` (추가)
  - `data/raw/028040_2012_ANNUAL_OFS.json` (추가)
  - `data/raw/028040_2013_ANNUAL_OFS.json` (추가)
  - `data/raw/028040_2014_ANNUAL_OFS.json` (추가)
  - `data/raw/029960_2011_ANNUAL_OFS.json` (추가)
  - `data/raw/029960_2012_ANNUAL_OFS.json` (추가)
  - `data/raw/029960_2013_ANNUAL_OFS.json` (추가)
  - `data/raw/029960_2014_ANNUAL_OFS.json` (추가)
  - `data/raw/033430_2000_ANNUAL_OFS.json` (추가)
  - `data/raw/033430_2008_ANNUAL_OFS.json` (추가)
  - `data/raw/033430_2009_ANNUAL_OFS.json` (추가)
  - `data/raw/033430_2010_ANNUAL_OFS.json` (추가)
  - `data/raw/033430_2011_ANNUAL_OFS.json` (추가)
  - `data/raw/033430_2012_ANNUAL_OFS.json` (추가)
  - `data/raw/033430_2013_ANNUAL_OFS.json` (추가)
  - `data/raw/033430_2014_ANNUAL_OFS.json` (추가)
  - `data/raw/044060_2011_ANNUAL_OFS.json` (추가)
  - `data/raw/044060_2012_ANNUAL_OFS.json` (추가)
  - `data/raw/044060_2013_ANNUAL_OFS.json` (추가)
  - `data/raw/044060_2014_ANNUAL_OFS.json` (추가)
  - `data/raw/045890_2008_ANNUAL_OFS.json` (추가)
  - `data/raw/045890_2009_ANNUAL_OFS.json` (추가)
  - `data/raw/045890_2010_ANNUAL_OFS.json` (추가)
  - `data/raw/045890_2011_ANNUAL_OFS.json` (추가)
  - `data/raw/045890_2012_ANNUAL_OFS.json` (추가)
  - `data/raw/045890_2013_ANNUAL_OFS.json` (추가)
  - `data/raw/045890_2014_ANNUAL_OFS.json` (추가)
  - `data/raw/050320_2008_ANNUAL_OFS.json` (추가)
  - `data/raw/050320_2009_ANNUAL_OFS.json` (추가)
  - `data/raw/050320_2010_ANNUAL_OFS.json` (추가)
  - `data/raw/050320_2011_ANNUAL_OFS.json` (추가)
  - `data/raw/050320_2012_ANNUAL_OFS.json` (추가)
  - `data/raw/050320_2013_ANNUAL_OFS.json` (추가)
  - `data/raw/050320_2014_ANNUAL_OFS.json` (추가)
  - `data/raw/050540_2008_ANNUAL_OFS.json` (추가)
  - `data/raw/050540_2009_ANNUAL_OFS.json` (추가)
  - `data/raw/050540_2010_ANNUAL_OFS.json` (추가)
  - `data/raw/050540_2011_ANNUAL_OFS.json` (추가)
  - `data/raw/050540_2012_ANNUAL_OFS.json` (추가)
  - `data/raw/050540_2013_ANNUAL_OFS.json` (추가)
  - `data/raw/050540_2014_ANNUAL_OFS.json` (추가)
  - `data/raw/102210_2009_ANNUAL_OFS.json` (추가)
  - `data/raw/102210_2010_ANNUAL_OFS.json` (추가)
  - `data/raw/102210_2011_ANNUAL_OFS.json` (추가)
  - `data/raw/102210_2012_ANNUAL_OFS.json` (추가)
  - `data/raw/102210_2013_ANNUAL_OFS.json` (추가)
  - `data/raw/102210_2014_ANNUAL_OFS.json` (추가)
  - `data/raw/126870_2012_ANNUAL_OFS.json` (추가)
  - `data/raw/126870_2013_ANNUAL_OFS.json` (추가)
  - `data/raw/126870_2014_ANNUAL_OFS.json` (추가)
  - `data/raw/141070_2012_ANNUAL_OFS.json` (추가)
  - `data/raw/141070_2013_ANNUAL_OFS.json` (추가)
  - `data/raw/141070_2014_ANNUAL_OFS.json` (추가)
  - `logs/legacy_industrials_delisted_20260327_173509.json` (추가)
  - `logs/legacy_industrials_remaining_cfs_20260327_174224.json` (추가)

</details>

<details>
<summary>추가한 기업 목록</summary>

| stock_code | corp_name | gics_sector | start_year | end_year |
|---|---|---|---|---|
| 008830 | 대동기어 | Industrials | 2013 | 2013 |
| 024810 |  |  | 2011 | 2014 |
| 028040 |  |  | 2000 | 2014 |
| 029960 |  |  | 2011 | 2014 |
| 033430 |  |  | 2000 | 2014 |
| 044060 |  |  | 2011 | 2014 |
| 045890 |  |  | 2008 | 2014 |
| 050320 |  |  | 2008 | 2014 |
| 050540 |  |  | 2008 | 2014 |
| 102210 |  |  | 2009 | 2014 |
| 126870 |  |  | 2012 | 2014 |
| 141070 |  |  | 2012 | 2014 |

</details>

## 점검 결과 (S3 제외)
- 요약: PASS 5 / WARN 0 / FAIL 1
| check | status | summary |
|---|---|---|
| pr_type_alignment | PASS | auto selected -> both |
| automation_non_s3 | FAIL | automation non-s3 checks failed |
| collect_help | PASS | python collect.py search --stock-code 019440 |
| s3_uploader_v2_help | PASS |                         로컬 로그 JSON을 S3에 업로드. 이 옵션만 있으면 raw data 업로드는 생략 |
| py_compile | PASS | ok |
| output_filename_pattern | PASS | Output filename pattern valid (checked=63) |

## 점검 상세
### ❌ automation_non_s3 (FAIL)
- automation non-s3 checks failed
  - [automation] mode=non-s3 checks=3 config=automation/config.json
  - - input_schema: PASS (1ms) Input CSV schema is valid
  - - local_data: WARN (18ms) Local data checked with warnings (missing_raw=50, empty_csv=0)
  - - log_schema: FAIL (2ms) Log schema validation failed (2 errors)
  - [automation] report json: automation/reports/20260329_150303_289410_non-s3_report.json
  - [automation] report md  : automation/reports/20260329_150303_289410_non-s3_report.md
  - [automation] overall=FAIL

## 앞으로 진행할 내용
- 필요 시 `python3 -m automation.run_checks --mode s3-only`로 S3 무결성 별도 점검
- PR 리뷰 반영 후 커밋 정리 및 머지
