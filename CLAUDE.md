# Project Context

This project is a DART financial statement collection pipeline.

- Save financial ratio CSV files by stock code to `data/output/`
- Save raw JSON files to `data/raw/`
- Upload raw JSON files to S3 after local storage

## PR Analysis Workflow

When the user asks something like "Analyze this PR" or "Summarize this PR", follow this workflow.

### Step 1: Run the pipeline (mechanical checks)

```bash
python3 scripts/pr_pipeline.py --output-json prs/context.json
```
This command:

- generates a base PR description markdown file under prs/
- saves structured analysis context to prs/context.json
- includes commit history, changed files, and check results

### Step 2: Analyze the diff

- Read prs/context.json
- Run git diff main...HEAD to inspect actual code changes
- Open and read the key changed files directly
- Infer the purpose and intent of the changes from both code and commits

### Step 3: Write the PR description

- Fill the ## Summary of Changes section in the generated markdown with the following structure:
- Background / Motivation
(1) Why this change was needed
(2) Infer from commit messages and code changes
- Key Changes
(1) Summarize the main changes in bullet points
(2) Explain what changed, why it changed, and how it was implemented
- Caveats
(1) Mention breaking changes, new dependencies, design changes, or operational concerns
- Impact Scope
(1) Explain how the change affects existing behavior or related functionality

### Writing Rules

- Write the PR description in Korean
- Keep the writing technical but easy to read
- Do not list changed files in the summary section
-- File lists already exist in a separate section
- Focus on the intent of the code changes, not just the raw diff
- Avoid repeating mechanical check output unless it is important for understanding the change

### Options

--head-ref <branch> : Compare against another branch instead of HEAD

--type data|structure|both : Manually specify the PR type

--create-pr : Automatically create a GitHub PR

--draft : Create the PR as a draft
