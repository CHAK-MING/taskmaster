#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <glaze/json.hpp>

#include <cstdint>
#include <string>
#endif

namespace dagforge::executor_dto {

struct SensorExecutorConfigJson {
  std::string type;
  std::string target;
  std::int64_t poke_interval{30};
  bool soft_fail{false};
  std::int64_t expected_status{200};
  std::string http_method{"GET"};
};

struct DockerExecutorConfigJson {
  std::string image;
  std::string socket{"/var/run/docker.sock"};
  std::string pull_policy;
};

struct LuaExecutorConfigJson {
  std::string script;
  std::string script_file;
  std::uint64_t max_instructions{100000};
  std::uint64_t max_memory_bytes{8ULL * 1024ULL * 1024ULL};
};

} // namespace dagforge::executor_dto

namespace glz {

template <> struct meta<dagforge::executor_dto::SensorExecutorConfigJson> {
  using T = dagforge::executor_dto::SensorExecutorConfigJson;
  static constexpr auto value =
      object("type", &T::type, "target", &T::target, "poke_interval",
             &T::poke_interval, "soft_fail", &T::soft_fail, "expected_status",
             &T::expected_status, "http_method", &T::http_method);
};

template <> struct meta<dagforge::executor_dto::DockerExecutorConfigJson> {
  using T = dagforge::executor_dto::DockerExecutorConfigJson;
  static constexpr auto value = object("image", &T::image, "socket", &T::socket,
                                       "pull_policy", &T::pull_policy);
};

template <> struct meta<dagforge::executor_dto::LuaExecutorConfigJson> {
  using T = dagforge::executor_dto::LuaExecutorConfigJson;
  static constexpr auto value =
      object("script", &T::script, "script_file", &T::script_file,
             "max_instructions", &T::max_instructions, "max_memory_bytes",
             &T::max_memory_bytes);
};

} // namespace glz
