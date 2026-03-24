# AGENTS.md

## Goal
Route agent requests into two tasks:
- `branch_compare`
- `pr_create`

Task specs:
- `agents/branch_compare.md`
- `agents/pr_create.md`

## Routing
1. Use `branch_compare` for: branch diff, compare with main, change analysis.
2. Use `pr_create` for: PR file generation, PR summary, `pr_pipeline`, `prs/`.
3. If both are requested: run `branch_compare` first, then `pr_create`.
4. If unclear: start with `branch_compare`.

## Defaults
- Base ref: `main`
- Head ref: `HEAD`
- For another local branch: `--head-ref <branch>`
- Use `--include-worktree` only when head is currently checked out.

## Agent-based analysis
- PR 요약은 외부 API 없이 에이전트가 직접 diff를 분석하여 작성.
- `--output-json prs/context.json` 옵션으로 구조화된 컨텍스트를 JSON 출력 가능.
- 에이전트는 context JSON + `git diff`를 읽고 "변경 요약" 섹션을 채움.

## Output
- `branch_compare`: commit delta, file diff, short risk notes.
- `pr_create`: generated `prs/*.md` path, PASS/WARN/FAIL summary, rerun command on failure.
