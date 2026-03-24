# pr_create

## 트리거
- "PR 파일 만들어", "PR 정리", "PR 생성", "prs에 저장"

## 목표
`scripts/pr_pipeline.py`로 PR 설명 파일을 생성하고, 필요 시 GitHub PR까지 생성한다.

## 입력 규칙
- base 기본값: `main`
- head 기본값: `HEAD`
- 파일명 제어가 필요하면 `--issue`, `--work-label` 사용
- AI 비활성 요청 시 `--no-agent` 사용

## 실행 명령
기본:
```bash
python3 scripts/pr_pipeline.py --type auto --base <base>
```

타 브랜치 대상:
```bash
python3 scripts/pr_pipeline.py --type auto --base <base> --head-ref <head>
```

미커밋 포함(현재 브랜치만):
```bash
python3 scripts/pr_pipeline.py --type auto --base <base> --include-worktree
```

PR 파일명 지정:
```bash
python3 scripts/pr_pipeline.py --type auto --base <base> --issue <issue> --work-label <label>
```

GitHub PR 생성:
```bash
python3 scripts/pr_pipeline.py --type auto --base <base> --create-pr --draft
```

## 결과 형식
1. 생성 파일 경로 (`prs/*.md`)
2. PR 타입 (`data|structure|both`)
3. 점검 결과 (`PASS/WARN/FAIL`)
4. 실패 시 재실행 명령 1개 제시

