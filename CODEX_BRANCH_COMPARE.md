# Codex Guide: Branch Compare

## Goal
Compare a local branch against a base branch and review scope/risk fast.

## Rules
- Use 3-dot diff: `base...head-ref`
- For another local branch, pass `--head-ref <branch>`
- `--include-worktree` only works when that head is currently checked out

## Commands
```bash
# Raw git comparison
git rev-list --left-right --count main...feature/my-branch
git diff --name-status main...feature/my-branch

# Pipeline comparison + report draft
python3 scripts/pr_pipeline.py --type auto --base main --head-ref feature/my-branch --dry-run
```

```bash
# Include uncommitted changes (current branch only)
python3 scripts/pr_pipeline.py --type auto --base main --head-ref HEAD --include-worktree --dry-run
```

## Output hints
- `No diff entries found`: no effective change in `base...head-ref`
- `checks failed`: at least one FAIL check
- `automation overall=WARN`: warning only, not a hard failure
