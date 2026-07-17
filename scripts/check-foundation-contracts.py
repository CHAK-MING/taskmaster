#!/usr/bin/env python3
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from pathlib import Path
import shlex
import subprocess
import sys


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
PUBLIC_HEADERS = sorted((REPOSITORY_ROOT / "include" / "dagforge").rglob("*.hpp"))
MODULE_GUARD = "#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE"
LAYER_ZERO_HEADERS = {
    "dagforge/core/contract.hpp",
    "dagforge/core/error.hpp",
    "dagforge/core/error_domain.hpp",
    "dagforge/core/scope_exit.hpp",
}
FORBIDDEN_LAYER_ZERO_INCLUDES = (
    "<boost/",
    "<glaze/",
    '"dagforge/http/',
    '"dagforge/workflow/',
    "<filesystem>",
)
SCOPE_EXIT_SEAM_FILES = {
    "include/dagforge/core/scope_exit.hpp",
    "src/modules/base.cppm",
}
FORBIDDEN_PUBLIC_FOUNDATION_TOKENS = {
    "<absl/": "Abseil headers must not enter DAGForge public interfaces",
    '"absl/': "Abseil headers must not enter DAGForge public interfaces",
    "absl::": "Abseil types must not enter DAGForge public interfaces",
    "function_ref": "function_ref requires a measured caller and a separate decision",
    "<stacktrace>": "stacktrace belongs behind a dedicated diagnostics seam",
    "std::stacktrace": "stacktrace belongs behind a dedicated diagnostics seam",
}


FEATURE_PROBE = r"""
#include <chrono>
#include <expected>
#include <flat_map>
#include <format>
#include <functional>
#include <print>
#include <ranges>
#include <source_location>
#include <spanstream>
#include <sstream>
#include <string>
#include <vector>

static_assert(__cplusplus >= 202302L);
static_assert(__cpp_lib_expected >= 202211L);
static_assert(__cpp_lib_move_only_function >= 202110L);
static_assert(__cpp_lib_source_location >= 201907L);

auto probe() -> void {
  std::expected<int, int> value{1};
  auto mapped = value.and_then([](int current) -> std::expected<int, int> {
    return current + 1;
  });
  std::move_only_function<int(int)> function = [](int current) {
    return current + 1;
  };
  auto bound = std::bind_back([](int lhs, int rhs) { return lhs + rhs; }, 2);
  std::flat_map<int, int> flat{{1, 2}};
  auto values = std::views::iota(0, 4) | std::ranges::to<std::vector>();
  auto zipped = std::views::zip(values, values);
  auto chunks = values | std::views::chunk(2);
  std::spanstream stream{std::span<char>{}};
  std::chrono::sys_seconds parsed{};
  std::istringstream input{"2026-01-01T00:00:00Z"};
  input >> std::chrono::parse("%FT%TZ", parsed);
  const auto *zone = std::chrono::locate_zone("UTC");
  std::chrono::zoned_time zoned{zone, parsed};
  auto formatted = std::format("{:%FT%TZ}", parsed);
  auto origin = std::source_location::current();
  std::println("{} {} {} {} {} {} {} {}", *mapped, function(1), bound(1),
               flat.at(1), values.size(), zipped.size(), chunks.size(),
               formatted.size() + origin.line() + zoned.get_info().abbrev.size() +
                   stream.span().size());
}
"""


def fail(message: str) -> None:
    print(f"foundation contract check failed: {message}", file=sys.stderr)
    raise SystemExit(1)


def matching_guard_end(lines: list[str], start: int) -> int | None:
    depth = 0
    for index in range(start, len(lines)):
        token = lines[index].strip()
        if token.startswith(("#if ", "#ifdef ", "#ifndef ")):
            depth += 1
        elif token.startswith("#endif"):
            depth -= 1
            if depth == 0:
                return index
    return None


def validate_header_shape(path: Path) -> list[str]:
    relative = path.relative_to(REPOSITORY_ROOT / "include").as_posix()
    lines = path.read_text(encoding="utf-8").splitlines()
    failures: list[str] = []
    if "#pragma once" not in lines:
        failures.append(f"{relative}: missing #pragma once")

    starts = [index for index, line in enumerate(lines) if line.strip() == MODULE_GUARD]
    if len(starts) != 1:
        failures.append(
            f"{relative}: expected one module-interface include guard, found {len(starts)}"
        )
        return failures

    guard_start = starts[0]
    guard_end = matching_guard_end(lines, guard_start)
    if guard_end is None:
        failures.append(f"{relative}: unterminated module-interface include guard")
        return failures

    for index, line in enumerate(lines):
        token = line.strip()
        if not token.startswith("#include"):
            continue
        if guard_start < index < guard_end:
            continue
        if token.endswith('.inc"'):
            continue
        failures.append(
            f"{relative}:{index + 1}: dependency include must be inside the module guard"
        )

    if relative in LAYER_ZERO_HEADERS:
        text = "\n".join(lines[guard_start : guard_end + 1])
        for forbidden in FORBIDDEN_LAYER_ZERO_INCLUDES:
            if forbidden in text:
                failures.append(
                    f"{relative}: Layer 0 contract depends on forbidden include {forbidden}"
                )
    return failures


def compiler_command(compiler: str) -> list[str]:
    command = shlex.split(compiler)
    if not command:
        fail("empty compiler command")
    return command


def common_compile_command(compiler: str) -> list[str]:
    return compiler_command(compiler) + [
        "-std=c++23",
        "-Wall",
        "-Wextra",
        "-Wpedantic",
        "-Werror",
        "-fsyntax-only",
        "-x",
        "c++",
        f"-I{REPOSITORY_ROOT / 'include'}",
        f"-I{REPOSITORY_ROOT / 'third_party' / 'CLI11' / 'include'}",
        f"-I{REPOSITORY_ROOT / 'third_party' / 'prometheus-cpp-core' / 'include'}",
        f"-I{REPOSITORY_ROOT / 'third_party' / 'unordered_dense' / 'include'}",
        f"-I{REPOSITORY_ROOT / 'third_party' / 'glaze' / 'include'}",
        "-DDAGFORGE_USE_STDLIB_MODULES=0",
        "-",
    ]


def compile_source(compiler: str, source: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        common_compile_command(compiler),
        input=source,
        text=True,
        capture_output=True,
        cwd=REPOSITORY_ROOT,
        check=False,
    )


def compile_header(compiler: str, path: Path) -> tuple[Path, str]:
    relative = path.relative_to(REPOSITORY_ROOT / "include").as_posix()
    source = f'#include "{relative}"\nint main() {{}}\n'
    result = compile_source(compiler, source)
    return path, "" if result.returncode == 0 else result.stderr


def run_static_checks() -> None:
    failures = [
        failure
        for header in PUBLIC_HEADERS
        for failure in validate_header_shape(header)
    ]
    if failures:
        fail("\n" + "\n".join(failures))

    forbidden_scope_uses: list[str] = []
    for root in ("include", "src", "tests"):
        for path in (REPOSITORY_ROOT / root).rglob("*"):
            if path.suffix not in {".hpp", ".cpp", ".cppm", ".inc"}:
                continue
            relative = path.relative_to(REPOSITORY_ROOT).as_posix()
            if relative in SCOPE_EXIT_SEAM_FILES:
                continue
            text = path.read_text(encoding="utf-8")
            if "<experimental/scope>" in text or "std::experimental::scope_exit" in text:
                forbidden_scope_uses.append(relative)
    if forbidden_scope_uses:
        fail(
            "experimental scope guards must stay behind core/scope_exit.hpp: "
            + ", ".join(sorted(forbidden_scope_uses))
        )

    forbidden_public_uses: list[str] = []
    for path in PUBLIC_HEADERS:
        relative = path.relative_to(REPOSITORY_ROOT).as_posix()
        text = path.read_text(encoding="utf-8")
        for token, reason in FORBIDDEN_PUBLIC_FOUNDATION_TOKENS.items():
            if token in text:
                forbidden_public_uses.append(f"{relative}: {reason} ({token})")
    if forbidden_public_uses:
        fail("\n" + "\n".join(sorted(forbidden_public_uses)))

    io_context = (
        REPOSITORY_ROOT / "include" / "dagforge" / "core" / "detail" /
        "io_context.inc"
    ).read_text(encoding="utf-8")
    if "operator native_type" in io_context or "operator boost::asio::io_context" in io_context:
        fail("IoContext must not provide an implicit native-context conversion")

    repository_sources = "\n".join(
        path.read_text(encoding="utf-8")
        for root in ("include", "src", "tests")
        for path in (REPOSITORY_ROOT / root).rglob("*")
        if path.suffix in {".hpp", ".cpp", ".cppm", ".inc"}
    )
    if "ScopedMemoryResourceOverride" in repository_sources:
        fail("memory overrides must use the explicit thread-bound guard")
    if "override_memory_resource" in repository_sources:
        fail("the legacy unqualified memory-resource override must not return")
    if "dump_json(" in repository_sources:
        fail("JSON serialization failures must not be hidden behind dump_json")
    if "from_validated(" in repository_sources:
        fail("typed IDs must use parse() or the explicit from_trusted() entry")

    json_contract = (
        REPOSITORY_ROOT / "include" / "dagforge" / "util" / "json.hpp"
    ).read_text(encoding="utf-8")
    if "Id::parse" not in json_contract:
        fail("the TypedId JSON adapter must validate input through Id::parse")
    if "id = Id{" in json_contract:
        fail("the TypedId JSON adapter must not bypass validation")


def run_compile_checks(compiler: str, jobs: int) -> None:
    feature_result = compile_source(compiler, FEATURE_PROBE)
    if feature_result.returncode != 0:
        fail(f"C++23 feature probe failed with {compiler}:\n{feature_result.stderr}")

    failures: list[tuple[Path, str]] = []
    with ThreadPoolExecutor(max_workers=jobs) as executor:
        futures = {
            executor.submit(compile_header, compiler, header): header
            for header in PUBLIC_HEADERS
        }
        for future in as_completed(futures):
            path, diagnostics = future.result()
            if diagnostics:
                failures.append((path, diagnostics))

    if failures:
        details = []
        for path, diagnostics in sorted(failures):
            relative = path.relative_to(REPOSITORY_ROOT).as_posix()
            details.append(f"\n--- {relative} ---\n{diagnostics.rstrip()}")
        fail(
            f"{len(failures)}/{len(PUBLIC_HEADERS)} public headers failed strict compile:"
            + "".join(details)
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate DAGForge public foundation contracts."
    )
    parser.add_argument("--static-only", action="store_true")
    parser.add_argument("--compiler", default=os.environ.get("CXX", "g++"))
    parser.add_argument(
        "--jobs",
        type=int,
        default=int(os.environ.get("DAGFORGE_HEADER_CHECK_JOBS", "4")),
    )
    args = parser.parse_args()
    if args.jobs < 1:
        fail("--jobs must be positive")

    run_static_checks()
    if not args.static_only:
        run_compile_checks(args.compiler, args.jobs)
    mode = "static" if args.static_only else "strict compile"
    print(f"foundation contract check passed ({len(PUBLIC_HEADERS)} headers, {mode})")


if __name__ == "__main__":
    main()
