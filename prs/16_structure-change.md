# feat: pipeline/structure update

## 개요
- PR 타입: `structure`
- 비교 기준: `main...16-refactor-edit-automation-of-checking-pr`
- 총 변경: 10개 파일 (10 files changed, 757 insertions(+), 41 deletions(-))
- 설명: 수집/파이프라인 구조 변경 중심 PR입니다.

## 변경 요약
- **변경 배경/동기**
  기존 PR 자동화 파이프라인이 OpenAI API에 의존하여 요약을 생성하거나 기계적 파일 목록만 나열하는 방식이었습니다. API 키 관리 부담을 없애고, Claude Code / Codex 등 에이전트가 직접 diff를 분석하여 사람이 읽기 좋은 PR 설명을 작성할 수 있도록 구조를 전환합니다.

- **주요 변경 사항**
  - `scripts/pr_pipeline.py`에서 OpenAI API 호출 코드(`_get_openai_key`, `_ask_openai_for_summary`, `--no-agent`)를 전면 제거하고, `--output-json` 옵션으로 구조화된 분석 컨텍스트를 JSON 출력하도록 변경.
  - `--head-ref` 옵션 추가로 HEAD 외 임의 로컬 브랜치 간 비교가 가능하도록 확장. 체크아웃되지 않은 브랜치에서는 `--include-worktree` 사용 불가 제약 및 구조 런타임 점검 스킵 로직 추가.
  - 커밋 로그 조회를 `git log base...head`(three-dot) → `base..head`(two-dot)로 변경하여 head 쪽 고유 커밋만 정확히 추출.
  - 파일별 변경 통계(+/- lines) 및 변경된 함수/클래스 이름 추출 기능 추가 (`_git_diff_summary_for_file`).
  - `CLAUDE.md`(Claude 전용)와 `AGENTS.md`(Codex 전용)를 분리 작성하여 각 에이전트가 PR 분석 워크플로우를 독립적으로 참조할 수 있도록 구성.
  - `agents/branch_compare.md`, `agents/pr_create.md` 태스크 스펙 파일 추가.

- **주의할 점**
  - 기존 `--no-agent` 플래그가 삭제되었으므로, 해당 옵션을 사용하던 스크립트나 CI 설정이 있다면 제거 필요.
  - PR 요약 생성이 더 이상 파이프라인 실행 시 자동으로 이루어지지 않음. 에이전트에게 별도로 "PR 분석해줘"를 요청해야 변경 요약이 채워짐.
  - `prs/context.json`은 `.gitignore`에 추가되어 추적되지 않으나, `prs/context_route1.json`은 이전 실행 산출물로 아직 추적 중.

- **영향 범위**
  - 외부 API 의존성 완전 제거 — OpenAI SDK, API 키 불필요.
  - 기존 `python3 scripts/pr_pipeline.py` 기본 실행 흐름은 동일하게 동작하며, AI 요약 대신 에이전트 분석 대기 플레이스홀더가 삽입됨.
  - Codex와 Claude 모두 동일한 파이프라인을 사용하되, 각자의 설정 파일(AGENTS.md / CLAUDE.md)을 통해 워크플로우를 안내받음.

<details>
<summary>커밋 히스토리</summary>

| hash | date | author | message |
|---|---|---|---|
| `fc1afa6` | 2026-03-24 | hann | doc : for codex & claude dividing #16 |
| `59223a9` | 2026-03-24 | hann | refactor : edit issue would be problem by using agent #16 |
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
  - `.gitignore` (수정)
  - `AGENTS.md` (추가)
  - `CLAUDE.md` (추가)
**other/**
  - `.claude/settings.json` (수정)
  - `agents/branch_compare.md` (추가)
  - `agents/pr_create.md` (추가)
  - `prs/16_structure-change.md` (추가)
  - `prs/context_route1.json` (추가)

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
