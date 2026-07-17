#!/usr/bin/env python3
from __future__ import annotations

from collections import Counter
import argparse
from pathlib import Path
import hashlib
import json
import os
import re
import subprocess
import sys


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
BUILD_FILE = REPOSITORY_ROOT / "bin" / "buildfile"
GROUPS = (
    "unit_test_sources",
    "component_test_sources",
    "integration_test_sources",
)
BINARY_BY_GROUP = {
    "unit_test_sources": "unit-tests",
    "component_test_sources": "component-tests",
    "integration_test_sources": "integration-tests",
}
TEST_PATTERN = re.compile(
    r"\bTEST(?:_F)?\s*\(\s*([A-Za-z_]\w*)\s*,\s*([A-Za-z_]\w*)\s*\)",
    re.MULTILINE,
)
SOURCE_HASH_STATE = ".dagforge-test-source-hashes.json"


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


def expected_tests(sources: list[str]) -> tuple[set[str], dict[str, str]]:
    tests: set[str] = set()
    source_by_test: dict[str, str] = {}
    for source in sources:
        if source == "test_main":
            continue
        path = REPOSITORY_ROOT / "tests" / f"{source}.cpp"
        text = path.read_text(encoding="utf-8")
        for suite, test in TEST_PATTERN.findall(text):
            name = f"{suite}.{test}"
            if name in source_by_test:
                fail(
                    f"duplicate GoogleTest declaration {name} in "
                    f"{source_by_test[name]} and {source}"
                )
            tests.add(name)
            source_by_test[name] = source
    return tests, source_by_test


def listed_tests(binary: Path) -> set[str]:
    if not binary.is_file() or not binary.stat().st_mode & 0o111:
        fail(f"test binary is missing or not executable: {binary}")
    completed = subprocess.run(
        [str(binary), "--gtest_list_tests"],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        fail(
            f"{binary.name} --gtest_list_tests exited {completed.returncode}: "
            f"{completed.stderr.strip()}"
        )
    tests: set[str] = set()
    suite: str | None = None
    for raw_line in completed.stdout.splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line:
            continue
        if not raw_line.startswith(" "):
            suite = line.removesuffix(".")
            continue
        if suite is not None:
            tests.add(f"{suite}.{line.strip()}")
    return tests


def remove_stale_test_artifacts(
    build_dir: Path,
    binary_name: str,
    sources: set[str],
) -> None:
    tests_dir = build_dir / "tests"
    for source in sorted(sources):
        for artifact in tests_dir.glob(f"{source}.o*"):
            if artifact.is_file() or artifact.is_symlink():
                artifact.unlink()
    binary = build_dir / "bin" / binary_name
    if binary.exists() or binary.is_symlink():
        binary.unlink()


def source_hashes(sources: list[str]) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for source in sources:
        path = REPOSITORY_ROOT / "tests" / f"{source}.cpp"
        hashes[source] = hashlib.sha256(path.read_bytes()).hexdigest()
    return hashes


def load_hash_state(build_dir: Path) -> dict[str, dict[str, str]]:
    path = build_dir / SOURCE_HASH_STATE
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    if not isinstance(value, dict):
        return {}
    state: dict[str, dict[str, str]] = {}
    for target, hashes in value.items():
        if not isinstance(target, str) or not isinstance(hashes, dict):
            continue
        if all(isinstance(name, str) and isinstance(digest, str)
               for name, digest in hashes.items()):
            state[target] = dict(hashes)
    return state


def prepare_build(
    groups: dict[str, list[str]],
    build_dir: Path,
    targets: list[str],
) -> None:
    groups_by_binary = {BINARY_BY_GROUP[group]: group for group in GROUPS}
    state = load_hash_state(build_dir)
    for binary_name in targets:
        group = groups_by_binary[binary_name]
        current = source_hashes(groups[group])
        previous = state.get(binary_name, {})
        changed = {
            source
            for source, digest in current.items()
            if previous.get(source) != digest
        }
        removed = set(previous) - set(current)
        if removed:
            changed.update(source for source in groups[group])
        if changed:
            remove_stale_test_artifacts(build_dir, binary_name, changed)
            print(
                f"test source hash changed ({binary_name}: "
                f"{', '.join(sorted(changed))}); invalidated stale artifacts"
            )


def record_hash_state(
    groups: dict[str, list[str]],
    build_dir: Path,
    targets: list[str],
) -> None:
    state = load_hash_state(build_dir)
    groups_by_binary = {BINARY_BY_GROUP[group]: group for group in GROUPS}
    for binary_name in targets:
        state[binary_name] = source_hashes(groups[groups_by_binary[binary_name]])
    build_dir.mkdir(parents=True, exist_ok=True)
    path = build_dir / SOURCE_HASH_STATE
    temporary = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    temporary.write_text(
        json.dumps(state, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def verify_binaries(
    groups: dict[str, list[str]],
    bin_dir: Path,
    targets: list[str],
    repair_build_dir: Path | None,
    record_build_dir: Path | None,
) -> None:
    groups_by_binary = {BINARY_BY_GROUP[group]: group for group in GROUPS}
    unknown = sorted(set(targets) - set(groups_by_binary))
    if unknown:
        fail(f"unknown test binaries: {', '.join(unknown)}")

    repaired = False
    for binary_name in targets:
        group = groups_by_binary[binary_name]
        expected, source_by_test = expected_tests(groups[group])
        actual = listed_tests(bin_dir / binary_name)
        missing = sorted(expected - actual)
        unexpected = sorted(actual - expected)
        if not missing and not unexpected:
            print(
                f"test binary check passed ({binary_name}: {len(actual)} tests)"
            )
            continue

        details: list[str] = []
        if missing:
            details.append(f"missing {len(missing)}: {', '.join(missing[:8])}")
        if unexpected:
            details.append(
                f"unexpected {len(unexpected)}: {', '.join(unexpected[:8])}"
            )
        print(
            f"test binary check failed for {binary_name}: {'; '.join(details)}",
            file=sys.stderr,
        )
        if repair_build_dir is None:
            raise SystemExit(1)
        stale_sources = {source_by_test[name] for name in missing}
        if unexpected:
            stale_sources.update(
                source for source in groups[group] if source != "test_main"
            )
        remove_stale_test_artifacts(
            repair_build_dir, binary_name, stale_sources
        )
        repaired = True

    if repaired:
        print(
            "removed stale test build artifacts; rebuild and verify again",
            file=sys.stderr,
        )
        raise SystemExit(2)
    if record_build_dir is not None:
        record_hash_state(groups, record_build_dir, targets)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify-binaries", type=Path)
    parser.add_argument("--repair-build-dir", type=Path)
    parser.add_argument("--record-build-dir", type=Path)
    parser.add_argument("--prepare-build", type=Path)
    parser.add_argument(
        "--targets",
        nargs="+",
        choices=sorted(BINARY_BY_GROUP.values()),
    )
    args = parser.parse_args()
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
    targets = args.targets or sorted(BINARY_BY_GROUP.values())
    if args.prepare_build is not None:
        prepare_build(groups, args.prepare_build, targets)
    if args.verify_binaries is not None:
        verify_binaries(
            groups,
            args.verify_binaries,
            targets,
            args.repair_build_dir,
            args.record_build_dir,
        )
    elif args.repair_build_dir is not None or args.record_build_dir is not None:
        fail("--repair-build-dir and --record-build-dir require --verify-binaries")
    if args.targets is not None and args.prepare_build is None and args.verify_binaries is None:
        fail("--targets requires --prepare-build or --verify-binaries")


if __name__ == "__main__":
    main()
