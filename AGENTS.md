# AGENTS.md (Codex)

## Project
DART 재무제표 수집 파이프라인. `data/output/`에 재무비율 CSV, `data/raw/`에 원본 JSON 저장 후 S3 업로드.

## Task routing
Codex는 요청을 아래 태스크로 라우팅한다.

| task | spec | trigger |
|---|---|---|
| `branch_compare` | `agents/branch_compare.md` | 브랜치 비교, main과 차이, 변경점 분석 |
| `pr_create` | `agents/pr_create.md` | PR 파일 생성, PR 요약, prs/ 저장 |
| `collect_test` | `agents/collect_test.md` | 데이터 수집 혹은 테스트 해줘, 데이터 수집해줘, 수집 테스트 해줘 |

- 수집/테스트 요청이면 `collect_test` 우선.
- 브랜치 비교 + PR 생성이 같이 요청되면 `branch_compare` → `pr_create` 순서.
- 불명확하면 `branch_compare`부터 시작.

## Defaults
- Base: `main`, Head: `HEAD`
- 다른 로컬 브랜치 비교: `--head-ref <branch>`
- `--include-worktree`는 head가 현재 체크아웃된 경우만 사용 가능.

## Collection command recommendation policy
데이터 수집 관련 명령어를 추천할 때는 항상 아래 3단계를 기본 포맷으로 제시한다.

1. 리스트 확인 (수집 실행 X)
2. 단일 테스트 (단일 종목으로 로컬+S3 동작 확인)
3. 본 수집 실행 (리스트 기준 로컬+S3 수집)

각 단계마다 반드시 다음을 함께 명시한다.
- `로컬 수집`: `O/X`
- `S3 업로드`: `O/X`
- 즉시 실행 가능한 실제 명령어 1개 이상

기본 예시는 아래를 사용한다.

```bash
# 1) 리스트 확인 (로컬 X / S3 X)
python3 run_pipeline.py --status normal --sectors "Materials" --member hann --dry-run

# 2) 단일 테스트 (로컬 O / S3 O)
python3 collect.py collect --stock-codes 019440 --years 2023 --quarters ANNUAL --save-raw --upload-s3

# 3) 본 수집 실행 (로컬 O / S3 O)
python3 run_pipeline.py --status normal --sectors "Materials" --member hann
```

사용자 요청에 맞게 `--status`, `--sectors`, `--stock-codes`, `--years`, `--member` 값을 치환해서 제공한다.

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
