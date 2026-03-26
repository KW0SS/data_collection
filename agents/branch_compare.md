# branch_compare

## 트리거
- "브랜치 비교", "main과 차이", "타 브랜치와 비교", "변경점 분석"

## 목표
기준 브랜치와 대상 브랜치의 차이를 빠르게 파악하고, 위험 요소를 짧게 정리한다.

## 입력 규칙
- base 기본값: `main`
- head 기본값: `HEAD`
- 사용자가 브랜치를 지정하면 `--head-ref <branch>` 사용

## 실행 명령
```bash
git rev-list --left-right --count <base>...<head>
git diff --name-status <base>...<head>
python3 scripts/pr_pipeline.py --type auto --base <base> --head-ref <head> --dry-run
```

현재 체크아웃 브랜치 비교 + 미커밋 포함이 필요하면:
```bash
python3 scripts/pr_pipeline.py --type auto --base <base> --head-ref HEAD --include-worktree --dry-run
```

## 결과 형식
1. 비교 기준: `<base>...<head>`
2. 커밋 차이 숫자: `left/right`
3. 파일 변경 상위 N개
4. 리스크 요약 (최대 3줄)

