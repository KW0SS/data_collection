# feat: pipeline/structure update

## 개요
- PR 타입: `structure`
- 비교 기준: `main...16-refactor-edit-automation-of-checking-pr`
- 설명: 수집/파이프라인 구조 변경 중심 PR입니다.

## 주요 업무
- 수집/자동화 구조 코드 변경 (구조 파일 변경 2건)
- PR 단계에서는 S3 무결성 검증 생략 (비용/IO 절감)

## 추가한 기업 목록
- 추가 기업 정보 추출 결과 없음

## 점검 결과 (S3 제외)
- 요약: PASS 4 / WARN 1 / FAIL 0
| check | status | summary |
|---|---|---|
| pr_type_alignment | PASS | auto selected -> structure |
| automation_non_s3 | WARN | automation overall=WARN |
| collect_help | PASS | python collect.py search --stock-code 019440 |
| s3_uploader_v2_help | PASS |                         로컬 로그 JSON을 S3에 업로드. 이 옵션만 있으면 raw data 업로드는 생략 |
| py_compile | PASS | ok |

## 앞으로 진행할 내용
- 필요 시 `python3 -m automation.run_checks --mode s3-only`로 S3 무결성 별도 점검
- PR 리뷰 반영 후 커밋 정리 및 머지
