#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DATA_PREFIXES = ("data/input/", "data/output/", "logs/")
STRUCTURE_PREFIXES = ("src/", "automation/", "scripts/")
STRUCTURE_ROOT_FILES = {
    "collect.py",
    "requirements.txt",
    ".gitignore",
    "README.md",
    "DOPPLER_GUIDE.md",
    "NAMING_CONVENTION.md",
}

COMPANY_CSV_CANDIDATES = [
    "data/input/companies_collected.csv",
    "data/input/companies_template.csv",
]


@dataclass
class DiffEntry:
    status: str
    path: str
    old_path: str | None = None


@dataclass
class CheckItem:
    name: str
    status: str
    summary: str
    details: list[str]


def _run(
    args: list[str],
    check: bool = True,
    capture: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        check=check,
        text=True,
        capture_output=capture,
    )


def _git_current_branch() -> str:
    cp = _run(["git", "rev-parse", "--abbrev-ref", "HEAD"])
    return cp.stdout.strip()


def _git_diff_entries(base: str) -> list[DiffEntry]:
    cp = _run(["git", "diff", "--name-status", "--find-renames", f"{base}...HEAD"])
    entries: list[DiffEntry] = []
    for raw in cp.stdout.splitlines():
        if not raw.strip():
            continue
        parts = raw.split("\t")
        status = parts[0]
        if status.startswith("R") and len(parts) >= 3:
            entries.append(DiffEntry(status="R", old_path=parts[1], path=parts[2]))
        elif len(parts) >= 2:
            entries.append(DiffEntry(status=status[:1], path=parts[1]))
    return entries


def _git_worktree_entries() -> list[DiffEntry]:
    cp = _run(["git", "status", "--porcelain"])
    entries: list[DiffEntry] = []
    for raw in cp.stdout.splitlines():
        if not raw.strip():
            continue
        status_xy = raw[:2]
        path_part = raw[3:]
        status = "M"
        if "A" in status_xy:
            status = "A"
        elif "D" in status_xy:
            status = "D"
        elif "R" in status_xy and " -> " in path_part:
            old_p, new_p = path_part.split(" -> ", 1)
            entries.append(DiffEntry(status="R", old_path=old_p, path=new_p))
            continue
        entries.append(DiffEntry(status=status, path=path_part))
    return entries


def _merge_entries(*groups: list[DiffEntry]) -> list[DiffEntry]:
    # same path appears in both commit diff and worktree; keep last status.
    merged: dict[tuple[str | None, str], DiffEntry] = {}
    for g in groups:
        for e in g:
            key = (e.old_path, e.path)
            merged[key] = e
    return list(merged.values())


def _is_data_path(path: str) -> bool:
    return path.startswith(DATA_PREFIXES)


def _is_structure_path(path: str) -> bool:
    if path in STRUCTURE_ROOT_FILES:
        return True
    if path.startswith(STRUCTURE_PREFIXES):
        return True
    if path.endswith(".py") and "/" not in path:
        return True
    return False


def _classify_pr(entries: list[DiffEntry]) -> tuple[str, dict[str, int]]:
    counts = {
        "data": 0,
        "structure": 0,
        "other": 0,
    }
    for e in entries:
        p = e.path
        if _is_data_path(p):
            counts["data"] += 1
        elif _is_structure_path(p):
            counts["structure"] += 1
        else:
            counts["other"] += 1

    has_data = counts["data"] > 0
    has_structure = counts["structure"] > 0

    if has_data and has_structure:
        return "both", counts
    if has_data:
        return "data", counts
    return "structure", counts


def _read_csv_rows_from_text(text: str) -> list[dict[str, str]]:
    text = text.lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(text))
    rows: list[dict[str, str]] = []
    for row in reader:
        clean = {str(k).strip(): (v or "").strip() for k, v in row.items()}
        rows.append(clean)
    return rows


def _load_csv_from_git(ref: str, path: str) -> list[dict[str, str]]:
    try:
        cp = _run(["git", "show", f"{ref}:{path}"], check=True)
    except subprocess.CalledProcessError:
        return []
    return _read_csv_rows_from_text(cp.stdout)


def _load_csv_from_worktree(path: str) -> list[dict[str, str]]:
    p = Path(path)
    if not p.exists():
        return []
    return _read_csv_rows_from_text(p.read_text(encoding="utf-8-sig"))


def _rows_to_company_map(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        code = (row.get("stock_code") or "").strip()
        if not code:
            continue
        out[code] = {
            "stock_code": code,
            "corp_name": (row.get("corp_name") or "").strip(),
            "gics_sector": (row.get("gics_sector") or "").strip(),
            "start_year": (row.get("start_year") or "").strip(),
            "end_year": (row.get("end_year") or "").strip(),
            "label": (row.get("label") or "").strip(),
        }
    return out


def _extract_added_companies(base: str) -> list[dict[str, str]]:
    added: dict[str, dict[str, str]] = {}
    for path in COMPANY_CSV_CANDIDATES:
        base_map = _rows_to_company_map(_load_csv_from_git(base, path))
        head_map = _rows_to_company_map(_load_csv_from_worktree(path))
        for code, row in head_map.items():
            if code not in base_map:
                added.setdefault(code, row)
    return [added[k] for k in sorted(added.keys())]


def _extract_output_added(entries: list[DiffEntry]) -> dict[str, list[int]]:
    by_code: dict[str, list[int]] = {}
    for e in entries:
        if e.status not in {"A", "M", "R"}:
            continue
        p = e.path
        if not p.startswith("data/output/") or not p.endswith(".csv"):
            continue
        stem = Path(p).stem
        parts = stem.rsplit("_", 1)
        if len(parts) != 2:
            continue
        code, year = parts
        if not code.isdigit() or len(code) != 6 or not year.isdigit():
            continue
        by_code.setdefault(code, []).append(int(year))
    for code in list(by_code.keys()):
        by_code[code] = sorted(set(by_code[code]))
    return by_code


def _validate_output_filename_patterns(entries: list[DiffEntry]) -> CheckItem:
    bad: list[str] = []
    checked = 0
    for e in entries:
        if not e.path.startswith("data/output/") or not e.path.endswith(".csv"):
            continue
        checked += 1
        stem = Path(e.path).stem
        parts = stem.rsplit("_", 1)
        if len(parts) != 2:
            bad.append(e.path)
            continue
        code, year = parts
        if not code.isdigit() or len(code) != 6 or not year.isdigit() or len(year) != 4:
            bad.append(e.path)
    if bad:
        return CheckItem(
            name="output_filename_pattern",
            status="FAIL",
            summary=f"Invalid output filename pattern in {len(bad)} files",
            details=bad[:30],
        )
    return CheckItem(
        name="output_filename_pattern",
        status="PASS",
        summary=f"Output filename pattern valid (checked={checked})",
        details=[],
    )


def _run_cmd_check(name: str, cmd: list[str]) -> CheckItem:
    try:
        cp = _run(cmd, check=True)
        summary = "ok"
        if cp.stdout.strip():
            summary = cp.stdout.strip().splitlines()[-1][:200]
        return CheckItem(name=name, status="PASS", summary=summary, details=[])
    except subprocess.CalledProcessError as e:
        details = []
        if e.stdout:
            details.extend(e.stdout.strip().splitlines()[-20:])
        if e.stderr:
            details.extend(e.stderr.strip().splitlines()[-20:])
        return CheckItem(
            name=name,
            status="FAIL",
            summary=f"command failed: {' '.join(cmd)}",
            details=details[:40],
        )


def _run_structure_checks(diff_entries: list[DiffEntry]) -> list[CheckItem]:
    checks: list[CheckItem] = []
    checks.append(_run_cmd_check("collect_help", ["python3", "collect.py", "--help"]))
    checks.append(
        _run_cmd_check("s3_uploader_v2_help", ["python3", "-m", "src.s3_uploader_v2", "--help"])
    )

    py_files_set: set[str] = set()
    for e in diff_entries:
        p = Path(e.path)
        if p.is_dir():
            for sub in p.rglob("*.py"):
                py_files_set.add(str(sub))
        elif e.path.endswith(".py") and p.exists():
            py_files_set.add(e.path)

    py_files = sorted(py_files_set)
    if py_files:
        checks.append(_run_cmd_check("py_compile", ["python3", "-m", "py_compile", *py_files]))
    else:
        checks.append(
            CheckItem(
                name="py_compile",
                status="PASS",
                summary="no changed python files",
                details=[],
            )
        )
    return checks


def _run_non_s3_checks(check_config: str) -> CheckItem:
    cmd = ["python3", "-m", "automation.run_checks", "--mode", "non-s3", "--config", check_config]
    try:
        cp = _run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        details = []
        if e.stdout:
            details.extend(e.stdout.strip().splitlines()[-30:])
        if e.stderr:
            details.extend(e.stderr.strip().splitlines()[-30:])
        return CheckItem(
            name="automation_non_s3",
            status="FAIL",
            summary="automation non-s3 checks failed",
            details=details[:60],
        )

    overall = "PASS"
    for line in reversed(cp.stdout.splitlines()):
        if line.startswith("[automation] overall="):
            overall = line.split("=", 1)[1].strip().upper()
            break
    if overall not in {"PASS", "WARN", "FAIL"}:
        overall = "PASS"
    return CheckItem(
        name="automation_non_s3",
        status=overall,
        summary=f"automation overall={overall}",
        details=[],
    )


def _build_title(pr_type: str) -> str:
    if pr_type == "data":
        return "chore: data collection update"
    if pr_type == "structure":
        return "feat: pipeline/structure update"
    return "feat: data + structure update"


def _extract_issue_number(branch: str, explicit_issue: str) -> str:
    if explicit_issue:
        return explicit_issue
    m = re.match(r"^(\d+)[-_].*", branch)
    if m:
        return m.group(1)
    return "no-issue"


def _sanitize_slug(text: str) -> str:
    slug = text.strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    return slug or "update"


def _default_work_label(pr_type: str) -> str:
    if pr_type == "data":
        return "data-collection"
    if pr_type == "structure":
        return "structure-change"
    return "data-structure-change"


def _build_body_file_path(
    pr_dir: str,
    issue_number: str,
    work_label: str,
) -> Path:
    folder = Path(pr_dir)
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"{issue_number}_{work_label}.md"


def _summarize_major_tasks(pr_type: str, diff_entries: list[DiffEntry]) -> list[str]:
    output_files = sum(1 for e in diff_entries if e.path.startswith("data/output/") and e.path.endswith(".csv"))
    input_files = sum(1 for e in diff_entries if e.path.startswith("data/input/"))
    code_files = sum(1 for e in diff_entries if _is_structure_path(e.path))

    tasks: list[str] = []
    if pr_type in {"data", "both"}:
        tasks.append(f"입력/결과 데이터 갱신 (input 변경 {input_files}건, output CSV 변경 {output_files}건)")
    if pr_type in {"structure", "both"}:
        tasks.append(f"수집/자동화 구조 코드 변경 (구조 파일 변경 {code_files}건)")
    tasks.append("PR 단계에서는 S3 무결성 검증 생략 (비용/IO 절감)")
    return tasks


def _format_company_table(companies: list[dict[str, str]], output_added: dict[str, list[int]]) -> str:
    if not companies and not output_added:
        return "- 추가 기업 정보 추출 결과 없음"

    lines: list[str] = []
    lines.append("| stock_code | corp_name | gics_sector | start_year | end_year |")
    lines.append("|---|---|---|---|---|")

    added_codes = set(output_added.keys())
    for row in companies:
        added_codes.add(row["stock_code"])

    for code in sorted(added_codes):
        row = next((c for c in companies if c["stock_code"] == code), None)
        corp_name = row["corp_name"] if row else ""
        gics = row["gics_sector"] if row else ""
        sy = row["start_year"] if row else ""
        ey = row["end_year"] if row else ""
        if not sy and code in output_added and output_added[code]:
            sy = str(min(output_added[code]))
        if not ey and code in output_added and output_added[code]:
            ey = str(max(output_added[code]))
        lines.append(f"| {code} | {corp_name} | {gics} | {sy} | {ey} |")
    return "\n".join(lines)


def _build_check_section(items: list[CheckItem]) -> str:
    lines: list[str] = []
    lines.append("| check | status | summary |")
    lines.append("|---|---|---|")
    for i in items:
        lines.append(f"| {i.name} | {i.status} | {i.summary} |")
    return "\n".join(lines)


def _type_alignment_check(selected_type: str, auto_type: str) -> CheckItem:
    if selected_type == "auto":
        return CheckItem(
            name="pr_type_alignment",
            status="PASS",
            summary=f"auto selected -> {auto_type}",
            details=[],
        )
    if selected_type == auto_type:
        return CheckItem(
            name="pr_type_alignment",
            status="PASS",
            summary=f"selected type matches detected type ({auto_type})",
            details=[],
        )
    return CheckItem(
        name="pr_type_alignment",
        status="WARN",
        summary=f"selected={selected_type}, detected={auto_type}",
        details=["선택한 PR 타입과 실제 변경 패턴이 다릅니다."],
    )


def _write_pr_description(
    path: Path,
    pr_type: str,
    base: str,
    branch: str,
    diff_entries: list[DiffEntry],
    checks: list[CheckItem],
    companies: list[dict[str, str]],
    output_added: dict[str, list[int]],
) -> None:
    overview = {
        "data": "기업코드/수집 결과 중심의 단순 데이터 수집 PR입니다.",
        "structure": "수집/파이프라인 구조 변경 중심 PR입니다.",
        "both": "데이터 수집과 구조 변경이 함께 포함된 PR입니다.",
    }[pr_type]

    major_tasks = _summarize_major_tasks(pr_type, diff_entries)
    company_table = _format_company_table(companies, output_added)
    check_table = _build_check_section(checks)
    pass_count = sum(1 for c in checks if c.status == "PASS")
    warn_count = sum(1 for c in checks if c.status == "WARN")
    fail_count = sum(1 for c in checks if c.status == "FAIL")

    lines: list[str] = []
    lines.append(f"# { _build_title(pr_type) }")
    lines.append("")
    lines.append("## 개요")
    lines.append(f"- PR 타입: `{pr_type}`")
    lines.append(f"- 비교 기준: `{base}...{branch}`")
    lines.append(f"- 설명: {overview}")
    lines.append("")
    lines.append("## 주요 업무")
    for task in major_tasks:
        lines.append(f"- {task}")
    lines.append("")
    lines.append("## 추가한 기업 목록")
    lines.append(company_table)
    lines.append("")
    lines.append("## 점검 결과 (S3 제외)")
    lines.append(f"- 요약: PASS {pass_count} / WARN {warn_count} / FAIL {fail_count}")
    lines.append(check_table)
    lines.append("")
    lines.append("## 앞으로 진행할 내용")
    lines.append("- 필요 시 `python3 -m automation.run_checks --mode s3-only`로 S3 무결성 별도 점검")
    lines.append("- PR 리뷰 반영 후 커밋 정리 및 머지")

    # 실패 상세 출력
    failed = [c for c in checks if c.status == "FAIL"]
    if failed:
        lines.append("")
        lines.append("## 실패 상세")
        for item in failed:
            lines.append(f"### {item.name}")
            if item.details:
                for d in item.details:
                    lines.append(f"- {d}")
            else:
                lines.append("- detail 없음")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _create_pr(title: str, body_file: Path, base: str, branch: str, draft: bool) -> None:
    cmd = [
        "gh",
        "pr",
        "create",
        "--base",
        base,
        "--head",
        branch,
        "--title",
        title,
        "--body-file",
        str(body_file),
    ]
    if draft:
        cmd.append("--draft")
    _run(cmd, check=True, capture=False)


def main() -> int:
    parser = argparse.ArgumentParser(description="PR pipeline (no S3 checks)")
    parser.add_argument("--base", default="main", help="Base branch/ref for diff")
    parser.add_argument(
        "--type",
        choices=["auto", "data", "structure", "both"],
        default="auto",
        help="PR type routing",
    )
    parser.add_argument("--check-config", default="automation/config.json")
    parser.add_argument("--pr-dir", default="prs", help="Directory for PR description files")
    parser.add_argument("--issue", default="", help="Issue number for PR description filename")
    parser.add_argument(
        "--work-label",
        default="",
        help="Short work label for PR description filename (e.g. input-pipeline)",
    )
    parser.add_argument(
        "--include-worktree",
        action="store_true",
        help="Include uncommitted changes in PR analysis",
    )
    parser.add_argument("--title", default="")
    parser.add_argument("--body-file", default="")
    parser.add_argument("--create-pr", action="store_true")
    parser.add_argument("--draft", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    branch = _git_current_branch()
    head_entries = _git_diff_entries(args.base)
    worktree_entries = _git_worktree_entries() if args.include_worktree else []
    diff_entries = _merge_entries(head_entries, worktree_entries)
    if not diff_entries:
        print("No diff entries found. Nothing to build for PR.")
        return 1

    auto_type, counts = _classify_pr(diff_entries)
    pr_type = auto_type if args.type == "auto" else args.type
    issue_number = _extract_issue_number(branch, args.issue)
    work_label = _sanitize_slug(args.work_label or _default_work_label(pr_type))
    print(f"[pr-pipeline] base={args.base} branch={branch} type={pr_type} auto={auto_type}")
    print(f"[pr-pipeline] changed files: data={counts['data']} structure={counts['structure']} other={counts['other']}")

    checks: list[CheckItem] = []
    checks.append(_type_alignment_check(args.type, auto_type))
    checks.append(_run_non_s3_checks(args.check_config))

    if pr_type in {"structure", "both"}:
        checks.extend(_run_structure_checks(diff_entries))
    if pr_type in {"data", "both"}:
        checks.append(_validate_output_filename_patterns(diff_entries))

    companies = _extract_added_companies(args.base)
    output_added = _extract_output_added(diff_entries)

    body_path = (
        Path(args.body_file)
        if args.body_file
        else _build_body_file_path(args.pr_dir, issue_number, work_label)
    )
    _write_pr_description(
        path=body_path,
        pr_type=pr_type,
        base=args.base,
        branch=branch,
        diff_entries=diff_entries,
        checks=checks,
        companies=companies,
        output_added=output_added,
    )
    print(f"[pr-pipeline] wrote {body_path}")

    failed = [c for c in checks if c.status == "FAIL"]
    if failed:
        print("[pr-pipeline] checks failed; PR create skipped.")
        return 1

    title = args.title or _build_title(pr_type)
    if args.create_pr:
        if args.dry_run:
            print(
                "[dry-run] gh pr create --base "
                f"{args.base} --head {branch} --title '{title}' --body-file {body_path}"
            )
        else:
            _create_pr(title=title, body_file=body_path, base=args.base, branch=branch, draft=args.draft)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
