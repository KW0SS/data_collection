# pr_create

## 트리거
- "PR 파일 만들어", "PR 정리", "PR 생성", "prs에 저장", "PR 분석해줘"

## 목표
`scripts/pr_pipeline.py`로 PR 설명 파일을 생성하고, 에이전트가 diff를 분석하여 변경 요약을 작성한다.

## 입력 규칙
- base 기본값: `main`
- head 기본값: `HEAD`
- 파일명 제어가 필요하면 `--issue`, `--work-label` 사용

## 실행 명령
기본 (컨텍스트 JSON 포함):
```bash
python3 scripts/pr_pipeline.py --type auto --base <base> --output-json prs/context.json
```

타 브랜치 대상:
```bash
python3 scripts/pr_pipeline.py --type auto --base <base> --head-ref <head> --output-json prs/context.json
```

미커밋 포함(현재 브랜치만):
```bash
python3 scripts/pr_pipeline.py --type auto --base <base> --include-worktree --output-json prs/context.json
```

PR 파일명 지정:
```bash
python3 scripts/pr_pipeline.py --type auto --base <base> --issue <issue> --work-label <label> --output-json prs/context.json
```

GitHub PR 생성:
```bash
python3 scripts/pr_pipeline.py --type auto --base <base> --create-pr --draft
```

## 에이전트 분석 흐름
1. 파이프라인 실행 → `prs/*.md` + `prs/context.json` 생성
2. `prs/context.json` 읽기 + `git diff <base>...<head>` 분석
3. 변경된 주요 파일 직접 읽어 코드 의도 파악
4. `prs/*.md`의 "## 변경 요약" 섹션을 한국어로 작성

## 결과 형식
1. 생성 파일 경로 (`prs/*.md`)
2. PR 타입 (`data|structure|both`)
3. 점검 결과 (`PASS/WARN/FAIL`)
4. 실패 시 재실행 명령 1개 제시
