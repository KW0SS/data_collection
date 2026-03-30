# feat: data + structure update

## 개요
- PR 타입: `both`
- 비교 기준: `main...22-feat-check-s3-using-boto3`
- 총 변경: 147개 파일 (147 files changed, 74168 insertions(+), 134 deletions(-))
- 설명: 데이터 수집과 구조 변경이 함께 포함된 PR입니다.

## 변경 요약
<!-- 에이전트에게 'PR 분석해줘'라고 요청하면 이 섹션을 자동 작성합니다 -->
_(에이전트 분석 대기 중)_

<details>
<summary>커밋 히스토리</summary>

| hash | date | author | message |
|---|---|---|---|
| `d878a2a` | 2026-03-30 | hann | feat : f_s3_stats.py for check s3 #22 |
| `ca3d13d` | 2026-03-28 | JEONGHAN | Merge pull request #20 from KW0SS/18-feat-collect-data-before-2015year |
| `d10fdb4` | 2026-03-29 | hann | refactor : edit error if it would be issued when merge to main #18 |
| `3a6a516` | 2026-03-29 | hann | docs : write 18 md file #18 |
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
  - `.gitignore` (수정)
  - `AGENTS.md` (수정)
  - `README.md` (수정)
  - `collect.py` (수정)
  - `f_s3_stats.py` (추가)
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
  - `prs/18_data-structure-change.md` (추가)
  - `prs/context.json` (수정)

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
  - - input_schema: PASS (0ms) Input CSV schema is valid
  - - local_data: WARN (248ms) Local data checked with warnings (missing_raw=50, empty_csv=0)
  - - log_schema: FAIL (3ms) Log schema validation failed (2 errors)
  - [automation] report json: automation/reports/20260330_214347_982093_non-s3_report.json
  - [automation] report md  : automation/reports/20260330_214347_982093_non-s3_report.md
  - [automation] overall=FAIL

## 앞으로 진행할 내용
- 필요 시 `python3 -m automation.run_checks --mode s3-only`로 S3 무결성 별도 점검
- PR 리뷰 반영 후 커밋 정리 및 머지
