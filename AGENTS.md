# AGENTS.md

## Goal
Route Codex requests into two tasks:
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

## AI
- Default: enabled (needs `OPENAI_API_KEY`)
- Disable when requested: `--no-agent`

## Output
- `branch_compare`: commit delta, file diff, short risk notes.
- `pr_create`: generated `prs/*.md` path, PASS/WARN/FAIL summary, rerun command on failure.
