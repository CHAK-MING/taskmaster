# Shared build2 locking and build helpers. This file is sourced by scripts.

if [[ -n "${DAGFORGE_BUILD2_COMMON_LOADED:-}" ]]; then
  return 0
fi
readonly DAGFORGE_BUILD2_COMMON_LOADED=1

build2_lock_root() {
  printf '%s\n' "${BUILD2_LOCK_ROOT:-${XDG_RUNTIME_DIR:-/tmp}/dagforge-build2-${UID}}"
}

acquire_build2_lock() {
  local lock_name="$1"
  local lock_root
  local lock_fd
  local wait_seconds="${BUILD2_LOCK_WAIT_SECONDS:-}"

  if ! command -v flock >/dev/null 2>&1; then
    echo "missing required tool: flock" >&2
    return 1
  fi

  lock_root=$(build2_lock_root)
  mkdir -p "$lock_root"
  chmod 700 "$lock_root"

  lock_name="${lock_name//[^[:alnum:]_.-]/_}"
  exec {lock_fd}>"${lock_root}/${lock_name}.lock"

  if flock -n "$lock_fd"; then
    return 0
  fi

  echo "waiting for build2 lock: ${lock_name}" >&2
  if [[ -n "$wait_seconds" ]]; then
    flock -w "$wait_seconds" "$lock_fd"
  else
    flock "$lock_fd"
  fi
}

run_build2_config() {
  local repo_root="$1"
  local config_name="${2#@}"
  shift 2

  local config_root="${BUILD2_CONFIG_ROOT:-${XDG_DATA_HOME:-$HOME/.local/share}/build2-configs}"
  local config_dir="${BUILD2_CONFIG_DIR:-${config_root}/dagforge-${config_name}}"
  local jobs="${BUILD2_JOBS:-$(nproc)}"
  local quiet="${BUILD2_QUIET:-0}"

  if [[ "${DAGFORGE_SKIP_MODULE_GRAPH_CHECK:-0}" != "1" ]]; then
    bash "${repo_root}/scripts/check-module-graph.sh"
  fi
  if [[ "${DAGFORGE_SKIP_AGENT_CONVENTION_CHECK:-0}" != "1" ]]; then
    bash "${repo_root}/scripts/check-agent-conventions.sh"
  fi

  acquire_build2_lock "build-${config_name}"

  BUILD2_CONFIG_NAME="$config_name" "${repo_root}/scripts/setup-build2.sh"

  cd "$repo_root"

  if [[ -n "${BUILD2_TARGETS:-}" ]]; then
    local -a target_specs
    local -a targets=()
    local target
    read -r -a target_specs <<<"${BUILD2_TARGETS}"
    for target in "${target_specs[@]}"; do
      if [[ "$target" == /* ]]; then
        targets+=("$target")
      else
        targets+=("${config_dir}/dagforge/${target}")
      fi
    done

    if [[ "$quiet" == "1" ]]; then
      exec b -q -j "$jobs" "${targets[@]}" "$@"
    fi
    exec b -j "$jobs" "${targets[@]}" "$@"
  fi

  if [[ "$quiet" == "1" ]]; then
    exec bdep update "@${config_name}" -q -j "$jobs" "$@"
  fi
  exec bdep update "@${config_name}" -j "$jobs" "$@"
}
