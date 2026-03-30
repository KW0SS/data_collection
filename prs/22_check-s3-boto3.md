# feat: pipeline/structure update

## 개요
- PR 타입: `structure`
- 비교 기준: `origin/main...22-feat-check-s3-using-boto3`
- 총 변경: 2개 파일 (2 files changed, 373 insertions(+))
- 설명: 수집/파이프라인 구조 변경 중심 PR입니다.

## 변경 요약
- 핵심 변경: `f_s3_stats.py` 신규 추가
- 목적: S3 버킷 내 raw JSON 파일을 `healthy/delisted × sector × year` 기준으로 집계/가시화
- 입력 소스: `.env` 또는 CLI 인자
  - 인증: `S3_ACCESS_KEY`, `S3_PRIVATE_KEY`
  - 버킷: `S3_BUCKET_NAME` (또는 `--bucket`)
  - 리전: `S3_REGION` (기본 `ap-northeast-2`, 또는 `--region`)
- 동작 방식:
  - `list_objects_v2` paginator로 버킷 전체 키 수집
  - `status/sector/stock_year_quarter.json` 패턴 파싱
  - 상태별 파일 수/기업 수, 섹터별 분포, 연도별 분포 출력
- 기대 효과:
  - 수집 완료 후 S3 적재 상태를 빠르게 점검 가능
  - 누락/편중(특정 섹터, 특정 연도) 여부를 운영 관점에서 즉시 확인 가능
- 확인 필요 사항(리스크):
  - 버킷 전체 스캔 특성상 오브젝트 수가 매우 많으면 실행 시간이 길어질 수 있음
  - 파일명 규칙(`{stock}_{year}_{quarter}.json`)을 벗어난 객체는 집계에서 제외됨

<details>
<summary>커밋 히스토리</summary>

| hash | date | author | message |
|---|---|---|---|
| `72cae14` | 2026-03-30 | hann | docs : 22 pr md file #22 |
| `d878a2a` | 2026-03-30 | hann | feat : f_s3_stats.py for check s3 #22 |

</details>

<details>
<summary>변경 파일 상세</summary>

**root/**
  - `f_s3_stats.py` (추가)
**other/**
  - `prs/22_check-s3-boto3.md` (추가)

</details>

## 점검 결과 (S3 제외)
- 요약: PASS 4 / WARN 0 / FAIL 1
| check | status | summary |
|---|---|---|
| pr_type_alignment | PASS | auto selected -> structure |
| automation_non_s3 | FAIL | automation non-s3 checks failed |
| collect_help | PASS | python collect.py search --stock-code 019440 |
| s3_uploader_v2_help | PASS |                         로컬 로그 JSON을 S3에 업로드. 이 옵션만 있으면 raw data 업로드는 생략 |
| py_compile | PASS | ok |

## 점검 상세
### ❌ automation_non_s3 (FAIL)
- automation non-s3 checks failed
  - [automation] mode=non-s3 checks=3 config=automation/config.json
  - - input_schema: PASS (0ms) Input CSV schema is valid
  - - local_data: WARN (245ms) Local data checked with warnings (missing_raw=50, empty_csv=0)
  - - log_schema: FAIL (2ms) Log schema validation failed (2 errors)
  - [automation] report json: automation/reports/20260330_214914_141210_non-s3_report.json
  - [automation] report md  : automation/reports/20260330_214914_141210_non-s3_report.md
  - [automation] overall=FAIL

## 앞으로 진행할 내용
- 필요 시 `python3 -m automation.run_checks --mode s3-only`로 S3 무결성 별도 점검
- PR 리뷰 반영 후 커밋 정리 및 머지
