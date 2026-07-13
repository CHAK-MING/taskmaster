#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/workflow/workflow_types.hpp"

#endif

namespace glz {
template <> struct meta<dagforge::workflow::KeyValue> {
  using T = dagforge::workflow::KeyValue;
  static constexpr auto value = object("key", &T::key, "value", &T::value);
};

template <> struct meta<dagforge::workflow::CommandNodeConfig> {
  using T = dagforge::workflow::CommandNodeConfig;
  static constexpr auto value = object(
      "program", &T::program, "arguments", &T::arguments, "env", &T::env,
      "input_env", &T::input_env);
};

template <> struct meta<dagforge::workflow::InputEnvironmentBinding> {
  using T = dagforge::workflow::InputEnvironmentBinding;
  static constexpr auto value = object(
      "input", &T::input, "environment", &T::environment);
};
} // namespace glz
