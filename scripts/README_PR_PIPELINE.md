# PR Pipeline (No S3 Check in PR)

PR 단계에서는 S3 무결성 검증을 생략하고, 변경 유형에 따라 점검을 라우팅합니다.

## PR 유형

- `data`: 단순 데이터 수집 PR
- `structure`: 구조 변경 PR
- `both`: 데이터 + 구조 혼합 PR
- `auto`: 변경 파일을 기준으로 자동 분류

## 실행 예시

```bash
# 커밋된 변경 기준 자동 분류
python3 scripts/pr_pipeline.py --type auto --base main

# 원하는 로컬 브랜치를 직접 비교 (체크아웃 없이 분석)
python3 scripts/pr_pipeline.py --type auto --base main --head-ref feature.input-pipeline

# 커밋 전 변경(워킹트리)까지 포함해서 자동 분류
python3 scripts/pr_pipeline.py --type auto --base main --include-worktree

# PR 본문 생성 + PR 생성(draft)
python3 scripts/pr_pipeline.py --type auto --base main --create-pr --draft

# 파일명 직접 제어 (이슈번호/핵심 work)
python3 scripts/pr_pipeline.py --type auto --base main --issue 14 --work-label input-pipeline
```

기본 저장 경로/파일명:

- 폴더: `prs/`
- 파일명: `{issue_number}_{work_label}.md`
  - 예: `prs/14_structure-change.md`

## 동작 요약

1. `base...head-ref` 변경 파일 분석 (`head-ref` 기본값: `HEAD`)
2. `--include-worktree` 사용 시, 현재 체크아웃된 `HEAD`의 워킹트리 변경까지 포함
3. PR 유형 분류 (`data/structure/both`)
4. 비-S3 체크 실행
5. `prs/{issue_number}_{work_label}.md` 자동 생성
6. 필요 시 `gh pr create` 실행

S3 무결성 검증은 PR 외부에서 별도 실행:

```bash
python3 -m automation.run_checks --mode s3-only --config automation/config.json
```
