# feat: pipeline/structure update

## 개요
- PR 타입: `structure`
- 비교 기준: `main...16-refactor-edit-automation-of-checking-pr`
- 총 변경: 10개 파일 (10 files changed, 689 insertions(+), 41 deletions(-))
- 설명: 수집/파이프라인 구조 변경 중심 PR입니다.

## 변경 요약
<!-- Claude Code에게 'PR 분석해줘'라고 요청하면 이 섹션을 자동 작성합니다 -->
_(에이전트 분석 대기 중)_

<details>
<summary>커밋 히스토리</summary>

| hash | date | author | message |
|---|---|---|---|
| `d7c1589` | 2026-03-24 | hann | doc : write AGENTS.md for automation using codex agent #16 |
| `476585e` | 2026-03-24 | hann | doc : write CLAUDE.md for automation using agent #16 |
| `c177aa1` | 2026-03-24 | hann | refactor : add AI in automation of PR writing #16 |
| `e741758` | 2026-03-23 | hann | refactor : edit pr_pipeline.py for comparison another branch in local #16 |

</details>

<details>
<summary>변경 파일 상세</summary>

**scripts/**
  - `scripts/README_PR_PIPELINE.md` (수정)
  - `scripts/pr_pipeline.py` (수정)
**root/**
  - `AGENTS.md` (추가)
  - `CLAUDE.md` (추가)
  - `CODEX_BRANCH_COMPARE.md` (삭제)
  - `CODEX_PR_CREATE.md` (삭제)
**other/**
  - `.claude/settings.json` (수정)
  - `agents/branch_compare.md` (추가)
  - `agents/pr_create.md` (추가)
  - `prs/16_structure-change.md` (추가)

</details>

## 점검 결과 (S3 제외)
- 요약: PASS 4 / WARN 1 / FAIL 0
| check | status | summary |
|---|---|---|
| pr_type_alignment | PASS | auto selected -> structure |
| automation_non_s3 | WARN | automation overall=WARN |
| collect_help | PASS | python collect.py search --stock-code 019440 |
| s3_uploader_v2_help | PASS |                         로컬 로그 JSON을 S3에 업로드. 이 옵션만 있으면 raw data 업로드는 생략 |
| py_compile | PASS | ok |

## 점검 상세
### ⚠️ automation_non_s3 (WARN)
- automation overall=WARN

## 앞으로 진행할 내용
- 필요 시 `python3 -m automation.run_checks --mode s3-only`로 S3 무결성 별도 점검
- PR 리뷰 반영 후 커밋 정리 및 머지
