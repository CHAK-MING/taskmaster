# VS Code and clangd setup

DAGForge uses build2 and C++20 modules. The authoritative build remains GCC 15,
but clangd uses a separate Clang build2 configuration because Clang cannot load
GCC `.gcm` files.

The repository setup creates a matching set of:

- Clang-generated `.pcm` module files;
- a real `compile_commands.json` captured from build2;
- a workspace-local `clangd` shim pointing to the matching clangd version.

Do not point clangd at the GCC build directory.

## Prerequisites

Install Clang, clangd, and Bear. The Clang compiler and clangd must have the same
major version. Clang 21 or newer is recommended.

On Ubuntu or Debian:

```bash
sudo apt install clang-21 clangd-21 bear
```

The normal DAGForge build prerequisites are also required:

- build2 and bdep 0.17 or newer;
- project system libraries such as Boost and OpenSSL;
- Python 3.

## Generate the IDE build

From the repository root:

```bash
scripts/setup-clangd.sh
```

The same command is available in VS Code through **Tasks: Run Task** →
**DAGForge: Refresh clangd database**.

The script selects the newest installed `clang++-22`, `clang++-21`, or
`clang++-20`, then selects a clangd executable with the same major version.
Explicit paths can be provided when several LLVM installations exist:

```bash
scripts/setup-clangd.sh \
  --compiler /usr/bin/clang++-21 \
  --clangd /usr/bin/clangd-21
```

The script intentionally recreates the dedicated `@clangd` build2
configuration and builds it as shared-library-only. This produces one canonical
PCM graph instead of parallel static and shared BMI variants.

Generated local artifacts are ignored by Git:

```text
compile_commands.json
.clangd-tools/
~/.local/share/build2-configs/dagforge-clangd/
```

The workspace setting automatically restarts clangd when the compilation
database changes. Run **clangd: Restart language server** manually if the
extension does not pick up the workspace-local clangd path immediately, or
reopen the workspace once.

## When to regenerate

Run `scripts/setup-clangd.sh` again after changes to:

- module declarations or imports;
- `src/buildfile`, `bin/buildfile`, or `tests/buildfile`;
- compiler flags or include paths;
- the selected Clang major version;
- newly added C, C++, or module source files.

Ordinary edits do not require regenerating the database. clangd indexes source
changes in the background.

## Why experimental module support is disabled

The build2 compilation database already contains explicit `-fmodule-file`
arguments for every imported module. clangd can therefore load the exact PCM
files produced by the IDE build.

On clangd 21, enabling `--experimental-modules-support` causes the module
dependency builder to rescan the same graph and can report false
`fe_pch_file_overridden` diagnostics for libstdc++ headers. The project VS Code
configuration deliberately omits that flag.

This is different from disabling C++ modules. Modules remain enabled by the
captured C++23 build commands and are validated by the setup script using both a
module interface and a module consumer.

## Troubleshooting

### `compile_commands.json` is missing

Run:

```bash
scripts/setup-clangd.sh
```

A plain GCC build does not generate the Clang compilation database.

### Clang and clangd versions do not match

The setup script rejects mixed major versions because PCM files are
compiler-version-specific. Select matching executables explicitly.

### VS Code still uses a global clangd

Check the clangd extension output. The executable should resolve through:

```text
${workspaceFolder}/.clangd-tools/clangd
```

The setup script refreshes this symlink on every run.

### Duplicate diagnostics appear

The repository disables the Microsoft C/C++ IntelliSense engine while retaining
the extension for debugger support. Ensure workspace settings are not overridden
by a more specific VS Code profile or remote setting.

### Third-party headers show diagnostics

The project `.clangd` suppresses diagnostics and background indexing under
`third_party/`. Diagnostics in DAGForge source that originate from a third-party
template instantiation may still be shown because the primary source file is
part of the project.
