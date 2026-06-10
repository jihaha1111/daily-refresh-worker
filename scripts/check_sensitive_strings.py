#!/usr/bin/env python3
"""Fail if public-runner files contain private repository identifiers."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EXCLUDED_DIRS = {".git", "__pycache__", ".pytest_cache"}
EXCLUDED_FILES = {Path("scripts/check_sensitive_strings.py")}

PATTERNS = [
    "PARKJONGMlN",
    "threads-objects-merge",
    "objects-merge",
    "drive.google.com/drive/folders/",
    "gdrive:" + "dev/" + "threads",
    "dev/" + "threads",
    "12l5UhhoRngIxcy-X5DzhkaMwxiy-0-uR",
    "17M-ZHMeysgVbJuue0Pj4oNNWDEmJZYKv",
    "1FbWcl1ym_5Oaw_Hz-MZTN2dAx9gf9cVr",
]


def should_scan(path: Path) -> bool:
    rel = path.relative_to(ROOT)
    if rel in EXCLUDED_FILES:
        return False
    if any(part in EXCLUDED_DIRS for part in rel.parts):
        return False
    if path.is_dir():
        return False
    return True


def main() -> int:
    failures = []
    for path in ROOT.rglob("*"):
        if not should_scan(path):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        rel = path.relative_to(ROOT)
        for pattern in PATTERNS:
            if pattern in text:
                failures.append(f"{rel}: contains forbidden pattern {pattern!r}")

    if failures:
        print("Sensitive string scan failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1
    print("Sensitive string scan passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
