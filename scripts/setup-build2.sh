#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
source "${repo_root}/scripts/build2-common.sh"
acquire_build2_lock setup

config_name="${BUILD2_CONFIG_NAME:-gcc}"
config_alias="@${config_name#@}"
cfg_root="${BUILD2_CONFIG_ROOT:-${XDG_DATA_HOME:-$HOME/.local/share}/build2-configs}"
cfg_dir="${BUILD2_CONFIG_DIR:-${cfg_root}/dagforge-${config_name#@}}"
compiler="${CXX:-g++}"
default_default=0
if [[ "$config_alias" == "@gcc" ]]; then
  default_default=1
fi
config_default="${BUILD2_CONFIG_DEFAULT:-$default_default}"
config_forward="${BUILD2_CONFIG_FORWARD:-1}"
cc_coptions="${BUILD2_CC_COPTIONS:-}"
cc_loptions="${BUILD2_CC_LOPTIONS:-}"
bin_lib="${BUILD2_BIN_LIB:-}"
recreate_config="${BUILD2_RECREATE_CONFIG:-0}"

cd "$repo_root"

for tool in b bdep bpkg; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "missing required tool: $tool" >&2
    exit 1
  fi
done

mkdir -p "$cfg_root"

remove_generated_config_dir() {
  local path="${1%/}"
  case "$path" in
  "$cfg_root"/dagforge-* | /tmp/build2-configs/dagforge-*)
    rm -rf "$path"
    ;;
  *)
    echo "refusing to remove non-DAGForge build directory: $path" >&2
    return 1
    ;;
  esac
}

current_cfg_path=$(
  bdep config list 2>/dev/null \
    | awk -v alias="$config_alias" '$1 == alias {print $2; exit}' \
    || true
)

if [[ "$recreate_config" == "1" && -n "${current_cfg_path}" ]]; then
  bdep deinit --force "$config_alias" || true
  bdep config remove "$config_alias" || true
  remove_generated_config_dir "$current_cfg_path"
  current_cfg_path=
fi

if [[ -n "${current_cfg_path}" && ! -d "${current_cfg_path%/}" ]]; then
  bdep deinit --force "$config_alias" || true
  bdep config remove "$config_alias" || true
  current_cfg_path=
fi

if [[ -n "${current_cfg_path}" && "${current_cfg_path%/}" != "${cfg_dir}" ]]; then
  if [[ "${current_cfg_path}" == /tmp/* ]] && bdep status "$config_alias" >/dev/null 2>&1; then
    if [[ ! -e "$cfg_dir" ]]; then
      mv "${current_cfg_path%/}" "$cfg_dir"
    fi
    bdep config move "$config_alias" "$cfg_dir"
    bdep sync "$config_alias"
  elif [[ "${current_cfg_path}" == /tmp/* ]]; then
    bdep deinit --force "$config_alias" || true
    bdep config remove "$config_alias" || true
  fi
fi

if ! bdep config list 2>/dev/null | awk -v alias="$config_alias" '$1 == alias {found = 1} END {exit(found ? 0 : 1)}'; then
  init_args=(init -C "$cfg_dir" "$config_alias" cc "config.cxx=${compiler}")
  if [[ -d "$cfg_dir" ]]; then
    remove_generated_config_dir "$cfg_dir"
  fi
  if [[ -n "$cc_coptions" ]]; then
    init_args+=("config.cc.coptions=${cc_coptions}")
  fi
  if [[ -n "$cc_loptions" ]]; then
    init_args+=("config.cc.loptions=${cc_loptions}")
  fi
  if [[ -n "$bin_lib" ]]; then
    init_args+=("config.bin.lib=${bin_lib}")
  fi
  bdep "${init_args[@]}"
else
  if ! set_output=$(bdep config set "$config_alias" "config.cxx=${compiler}" 2>&1 >/dev/null); then
    if [[ "$set_output" != *"nothing to set"* ]]; then
      printf '%s\n' "$set_output" >&2
      exit 1
    fi
  fi
  if [[ -n "$cc_coptions" ]]; then
    if ! set_output=$(bdep config set "$config_alias" "config.cc.coptions=${cc_coptions}" 2>&1 >/dev/null); then
      if [[ "$set_output" != *"nothing to set"* ]]; then
        printf '%s\n' "$set_output" >&2
        exit 1
      fi
    fi
  fi
  if [[ -n "$cc_loptions" ]]; then
    if ! set_output=$(bdep config set "$config_alias" "config.cc.loptions=${cc_loptions}" 2>&1 >/dev/null); then
      if [[ "$set_output" != *"nothing to set"* ]]; then
        printf '%s\n' "$set_output" >&2
        exit 1
      fi
    fi
  fi
  if [[ -n "$bin_lib" ]]; then
    if ! set_output=$(bdep config set "$config_alias" "config.bin.lib=${bin_lib}" 2>&1 >/dev/null); then
      if [[ "$set_output" != *"nothing to set"* ]]; then
        printf '%s\n' "$set_output" >&2
        exit 1
      fi
    fi
  fi
fi

cfg_line=$(bdep config list 2>/dev/null | awk -v alias="$config_alias" '$1 == alias {print; exit}')
if [[ "$config_default" == "1" && "$cfg_line" != *"default"* ]]; then
  bdep config set "$config_alias" --default >/dev/null
fi
if [[ "$config_default" == "0" && "$cfg_line" == *"default"* ]]; then
  bdep config set "$config_alias" --no-default >/dev/null
fi
if [[ "$config_forward" == "1" && "$cfg_line" != *"forwarded"* ]]; then
  bdep config set "$config_alias" --forward >/dev/null
fi
if [[ "$config_forward" == "0" && "$cfg_line" == *"forwarded"* ]]; then
  bdep config set "$config_alias" --no-forward >/dev/null
fi

printf 'build2 config ready: %s -> %s\n' "$config_alias" "$cfg_dir"
