#!/usr/bin/env python3
from __future__ import annotations

from collections import Counter
from pathlib import Path
import re
import sys


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
BUILD_FILE = REPOSITORY_ROOT / "bin" / "buildfile"
GROUPS = (
    "unit_test_sources",
    "component_test_sources",
    "integration_test_sources",
)


def fail(message: str) -> None:
    print(f"test layout check failed: {message}", file=sys.stderr)
    raise SystemExit(1)


def parse_group(build_text: str, name: str) -> list[str]:
    match = re.search(
        rf"(?ms)^{re.escape(name)}\s*=\s*\.\./tests/cxx\{{(.*?)\}}",
        build_text,
    )
    if match is None:
        fail(f"missing {name} in bin/buildfile")
    body = match.group(1).replace("\\\n", " ")
    return body.split()


def main() -> None:
    build_text = BUILD_FILE.read_text(encoding="utf-8")
    groups = {name: parse_group(build_text, name) for name in GROUPS}

    for name, sources in groups.items():
        if sources.count("test_main") != 1:
            fail(f"{name} must contain test_main exactly once")
        repeated = sorted(
            source for source, count in Counter(sources).items() if count > 1
        )
        if repeated:
            fail(f"{name} repeats sources: {', '.join(repeated)}")

    assignments = Counter(
        source
        for sources in groups.values()
        for source in sources
        if source != "test_main"
    )
    duplicated = sorted(source for source, count in assignments.items() if count > 1)
    if duplicated:
        fail(f"test sources belong to multiple targets: {', '.join(duplicated)}")

    expected = {path.stem for path in (REPOSITORY_ROOT / "tests").glob("*_test.cpp")}
    assigned = set(assignments)
    missing = sorted(expected - assigned)
    extra = sorted(assigned - expected)
    if missing:
        fail(f"unassigned test sources: {', '.join(missing)}")
    if extra:
        fail(f"unknown test sources in groups: {', '.join(extra)}")

    print(
        "test layout check passed "
        f"({len(expected)} sources across {len(groups)} targets)"
    )


if __name__ == "__main__":
    main()
