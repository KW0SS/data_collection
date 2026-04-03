# feat: data + structure update

## 개요
- PR 타입: `both`
- 비교 기준: `main...21-refactor-edit-processadd-batch-system`
- 총 변경: 8개 파일 (8 files changed, 584 insertions(+), 138 deletions(-))
- 설명: 데이터 수집과 구조 변경이 함께 포함된 PR입니다.

## 변경 요약
<!-- 에이전트에게 'PR 분석해줘'라고 요청하면 이 섹션을 자동 작성합니다 -->
**변경 배경/동기**: Materials 정상기업 수집 범위를 확장하는 과정에서 수집 시간이 길고, 중단 시 처음부터 다시 확인해야 하는 비효율이 있었습니다. 이번 변경은 수집 파이프라인에 병렬 워커와 진행 상태 복원 로직을 넣어 재실행 비용을 줄이고, 반복적으로 수행되는 corp code 조회와 상폐 업종코드 조회의 병목을 완화하려는 목적입니다.

**주요 변경 사항**
- 수집 CLI와 통합 파이프라인에 `--workers` 옵션을 추가해 배치 수집을 병렬 실행할 수 있도록 정리했습니다. 단일 수집, legacy 수집, 누락 재수집, 파이프라인 실행이 같은 워커 옵션을 공유합니다.
- 수집기 내부를 태스크 기반 실행 구조로 바꾸고, `.collect_progress.json`에 진행 상태를 저장해 중단 후 재시작 시 이미 처리한 `(종목, 연도, 분기)`를 사전 필터링하도록 했습니다.
- DART corp code XML 파싱 결과와 stock code 인덱스를 프로세스 메모리에 캐시해 반복 조회 비용을 줄였습니다. 종목코드 단독 검색은 선형 탐색 대신 인덱스 조회를 사용합니다.
- 상폐 기업 업종코드 조회 스크립트는 기존 전체 재조회 방식에서 캐시 재사용, 100건 단위 중간 저장, 3개 워커 병렬 조회로 변경해 재실행과 장시간 작업에 대응하도록 했습니다.
- PR 정리 과정에서 실행 산출물인 `companies_collected.csv`는 제거하고, PR 설명 파일을 별도로 추가해 브랜치 문서화를 분리했습니다.

**주의할 점**
- 진행 상태 파일은 현재 `data/output/.collect_progress.json` 단일 경로를 사용하므로, 서로 다른 배치를 동시에 돌리면 체크포인트가 섞일 수 있습니다. 병렬화 자체보다 운영 방식에 주의가 필요합니다.
- 병렬 수집은 속도는 개선하지만 DART API 호출 수를 동시에 늘리므로, 환경에 따라 rate limit이나 일시 실패가 더 자주 보일 수 있습니다. 워커 수는 보수적으로 운영해야 합니다.
- 정상기업 수집 범위는 여전히 전역 `start_year/end_year`를 그대로 사용하므로, 최근 상장 종목까지 과거 연도 구간을 조회하게 됩니다. 이 경우 수집은 수행되지만 실제 데이터가 없어 CSV 저장이 생략될 수 있습니다.
- 상폐 업종코드 캐시는 존재 여부와 누락률 기준으로 재사용되므로, 부분 실패 상태의 캐시가 남아 있으면 이후 상폐 대상 분류 품질에 영향을 줄 수 있습니다.

**영향 범위**: 실행 경로 기준으로는 `collect.py` 직접 실행, `run_pipeline.py` 기반 일괄 수집, `collect.py retry` 기반 누락 재수집, 상폐 기업 업종코드 사전 준비 단계까지 모두 영향을 받습니다. 기능 추가 성격이지만, 병렬 처리와 체크포인트 도입으로 수집 실행 방식과 운영상의 주의점이 함께 바뀌는 변경입니다.

<details>
<summary>커밋 히스토리</summary>

| hash | date | author | message |
|---|---|---|---|
| `62fde47` | 2026-04-01 | hann | docs : write pr md file #21 |
| `7117168` | 2026-03-30 | hann | refactor : apply async in pipeline #21 |
| `468f844` | 2026-03-30 | hann | docs : write companies_collected.csv for datat collection of healthy Material Companies #21 |
| `b222e4f` | 2026-03-29 | hann | refactor : edit delay #21 |

</details>

<details>
<summary>변경 파일 상세</summary>

**src/**
  - `src/collector.py` (수정)
  - `src/dart_api.py` (수정)
**data/input/**
  - `data/input/companies_collected.csv` (삭제)
**root/**
  - `.gitignore` (수정)
  - `collect.py` (수정)
  - `f_fetch_induty_codes.py` (수정)
  - `run_pipeline.py` (수정)
**other/**
  - `prs/21_data-structure-change.md` (추가)

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
| output_filename_pattern | PASS | Output filename pattern valid (checked=0) |

## 점검 상세
### ❌ automation_non_s3 (FAIL)
- automation non-s3 checks failed
  - [automation] mode=non-s3 checks=3 config=automation/config.json
  - - input_schema: PASS (0ms) Input CSV schema is valid
  - - local_data: WARN (234ms) Local data checked with warnings (missing_raw=50, empty_csv=0)
  - - log_schema: FAIL (3ms) Log schema validation failed (2 errors)
  - [automation] report json: automation/reports/20260401_133648_393087_non-s3_report.json
  - [automation] report md  : automation/reports/20260401_133648_393087_non-s3_report.md
  - [automation] overall=FAIL

## 앞으로 진행할 내용
- 필요 시 `python3 -m automation.run_checks --mode s3-only`로 S3 무결성 별도 점검
- PR 리뷰 반영 후 커밋 정리 및 머지
