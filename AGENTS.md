# AGENTS.md (Codex)

## Project
DART 재무제표 수집 파이프라인. `data/output/`에 재무비율 CSV, `data/raw/`에 원본 JSON 저장 후 S3 업로드.

## Task routing
Codex는 요청을 아래 두 태스크로 라우팅한다.

| task | spec | trigger |
|---|---|---|
| `branch_compare` | `agents/branch_compare.md` | 브랜치 비교, main과 차이, 변경점 분석 |
| `pr_create` | `agents/pr_create.md` | PR 파일 생성, PR 요약, prs/ 저장 |

- 둘 다 요청되면 `branch_compare` → `pr_create` 순서.
- 불명확하면 `branch_compare`부터 시작.

## Defaults
- Base: `main`, Head: `HEAD`
- 다른 로컬 브랜치 비교: `--head-ref <branch>`
- `--include-worktree`는 head가 현재 체크아웃된 경우만 사용 가능.

## PR analysis flow
1. `python3 scripts/pr_pipeline.py --output-json prs/context.json` 실행
2. `prs/context.json` + `git diff main..HEAD` 읽기
3. 변경된 주요 파일 직접 읽어 코드 의도 파악
4. `prs/*.md`의 "## 변경 요약" 섹션을 한국어로 작성

### 변경 요약 구조
- **변경 배경/동기**: 왜 이 변경이 필요했는지 (커밋 메시지 + 코드에서 추론)
- **주요 변경 사항**: 핵심 변경을 bullet point로 (무엇을, 왜, 어떻게)
- **주의할 점**: breaking change, 새 의존성, 설계 변경
- **영향 범위**: 기존 기능에 미치는 영향

### 작성 규칙
- 한국어, 기술적이되 읽기 쉽게
- 파일 목록 나열 금지 (별도 섹션에 존재)
- 코드 변경의 "의도"에 집중

## Output
- `branch_compare`: commit delta, file diff, short risk notes.
- `pr_create`: `prs/*.md` 경로, PASS/WARN/FAIL 요약, 실패 시 재실행 명령.
