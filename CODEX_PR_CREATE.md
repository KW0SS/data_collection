# Codex Guide: PR File Creation

## Goal
Generate PR description files from branch diffs, then optionally create a GitHub PR.

## Basic
```bash
python3 scripts/pr_pipeline.py --type auto --base main
```
- Output dir: `prs/`
- Default name: `{issue}_{work-label}.md`

## Useful variants
```bash
# Control filename
python3 scripts/pr_pipeline.py --type auto --base main --issue 16 --work-label automation-update

# Compare another local branch
python3 scripts/pr_pipeline.py --type auto --base main --head-ref feature/input-pipeline

# Include uncommitted changes (current branch only)
python3 scripts/pr_pipeline.py --type auto --base main --include-worktree
```

## AI summary
- Default: tries AI summary (`OPENAI_API_KEY`)
- Disable AI:
```bash
python3 scripts/pr_pipeline.py --type auto --base main --no-agent
```

## Create GitHub PR
```bash
python3 scripts/pr_pipeline.py --type auto --base main --create-pr --draft
```

## Common errors
1. `Base ref not found` / `Head ref not found`: check branch/ref name
2. `Cannot use --include-worktree ...`: head branch is not checked out
3. `checks failed`: open `prs/*.md`, fix FAIL items, rerun
