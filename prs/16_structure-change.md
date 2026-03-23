# feat: pipeline/structure update

## 개요
- PR 타입: `structure`
- 비교 기준: `main...16-refactor-edit-automation-of-checking-pr`
- 총 변경: 4개 파일 (3 files changed, 108 insertions(+), 15 deletions(-))
- 설명: 수집/파이프라인 구조 변경 중심 PR입니다.

## 변경 요약
- **변경 배경/동기**  
  PR 비교 시 기본 `HEAD` 대신 원하는 로컬 브랜치나 특정 ref를 지정해 분석할 수 있도록 하여, 체크아웃 없이도 다양한 브랜치 간 변경사항을 쉽게 비교하고 자동 분류 및 점검을 수행하기 위함입니다.

- **주요 변경 사항**  
  - `pr_pipeline.py`에 `--head-ref` 옵션 추가로, 기본 `HEAD` 외에 임의의 로컬 브랜치나 ref를 비교 대상으로 지정할 수 있도록 기능 확장함.  
  - `--head-ref`가 현재 체크아웃된 브랜치가 아닐 경우 `--include-worktree` 옵션 사용 불가하도록 제약 추가 및 관련 에러 메시지 출력.  
  - 내부 git diff 호출 시 `base...head-ref` 형태로 변경하여 두 ref 간 차이만 정확히 비교하도록 수정.  
  - 변경된 브랜치가 체크아웃되어 있지 않으면 구조 관련 런타임 점검을 건너뛰고 경고 메시지를 출력하도록 로직 보완.  
  - `README_PR_PIPELINE.md` 문서에 `--head-ref` 옵션 사용법과 동작 방식을 상세히 추가하여 사용자가 쉽게 이해할 수 있도록 개선.  
  - PR 생성 시 `--head-ref`가 기본값이 아닐 경우 푸시 여부를 경고 메시지로 안내.  
  - `prs/16_structure-change.md` 삭제 및 `.claude/settings.json` 일부 수정 포함.

- **주의할 점**  
  - `--head-ref`로 지정한 브랜치가 현재 체크아웃된 상태가 아니면 워킹트리 변경 포함 옵션(`--include-worktree`)을 사용할 수 없으며, 이 경우 구조 런타임 체크가 생략되어 일부 점검이 누락될 수 있음.  
  - PR 생성 시 `--head-ref`가 기본 `HEAD`가 아닐 경우 반드시 원격에 푸시되어 있어야 하며, 그렇지 않으면 PR 생성 실패 가능성 있음.  
  - 기존에는 `HEAD` 기준으로만 비교하던 점이 확장되면서, 사용자가 옵션을 잘못 지정할 경우 의도하지 않은 결과가 나올 수 있으므로 옵션 사용 시 주의 필요.

- **영향 범위**  
  - PR 자동 분류 및 비-S3 점검 파이프라인의 유연성이 크게 향상되어, 다양한 로컬 브랜치 간 비교 및 분석이 가능해짐.  
  - 기존 `HEAD` 기준 워크플로우에는 영향이 없으나, 새로운 옵션 사용 시 워킹트리 포함 여부와 체크아웃 상태에 따른 제한사항을 인지해야 함.  
  - 구조 점검 자동화 과정에서 체크아웃되지 않은 브랜치 비교 시 일부 점검이 생략될 수 있으므로, 신뢰도에 영향을 줄 수 있음.

<details>
<summary>커밋 히스토리</summary>

| hash | date | author | message |
|---|---|---|---|
| `e741758` | 2026-03-23 | hann | refactor : edit pr_pipeline.py for comparison another branch in local #16 |

</details>

<details>
<summary>변경 파일 상세</summary>

**scripts/**
  - `scripts/README_PR_PIPELINE.md` (수정)
  - `scripts/pr_pipeline.py` (수정)
**other/**
  - `prs/16_structure-change.md` (삭제)
  - `.claude/settings.json` (수정)

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
