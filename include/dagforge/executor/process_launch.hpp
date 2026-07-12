#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/io/context.hpp"
#endif

#include <boost/process/v2/environment.hpp>
#include <boost/process/v2/process.hpp>
#include <boost/process/v2/start_dir.hpp>
#include <boost/process/v2/stdio.hpp>
#include <boost/system/error_code.hpp>
#if defined(BOOST_PROCESS_V2_POSIX)
#include <boost/process/v2/posix/default_launcher.hpp>
#include <boost/process/v2/posix/vfork_launcher.hpp>
#include <unistd.h>
#endif

#include <cerrno>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace dagforge {

namespace bp = boost::process::v2;

#if defined(BOOST_PROCESS_V2_POSIX)
struct NewProcessGroupInit {
  template <typename Launcher, typename PathLike>
  auto on_exec_setup(Launcher &, const PathLike &, const char *const *&)
      -> boost::system::error_code {
    if (::setpgid(0, 0) != 0) {
      return boost::system::error_code(errno, boost::system::generic_category());
    }
    return {};
  }
};
#endif

template <typename Map>
[[nodiscard]] inline auto build_process_env(const Map &custom)
    -> bp::process_environment {
  std::vector<std::pair<std::string, std::string>> env_entries;
  env_entries.reserve(64);
  for (const auto &entry : bp::environment::current()) {
    auto key_sv = entry.key();
    if (custom.contains(std::string(key_sv.data(), key_sv.size()))) {
      continue;
    }
    auto value_sv = entry.value();
    env_entries.emplace_back(
        std::string(key_sv.data(), key_sv.size()),
        std::string(value_sv.data(), value_sv.size()));
  }

  for (const auto &[k, v] : custom) {
    env_entries.emplace_back(k, v);
  }
  return bp::process_environment(std::move(env_entries));
}

struct ProcessLaunchSpec {
  std::string program;
  std::vector<std::string> args;
  std::optional<bp::process_stdio> stdio;
  std::optional<bp::process_environment> env;
  std::string working_dir;
};

template <typename Launcher, typename... Inits>
auto invoke_process(Launcher &&launch, io::IoContext &io,
                    ProcessLaunchSpec spec, Inits &&... extra)
    -> bp::process {
  auto program = std::move(spec.program);

  if (spec.stdio && spec.env && !spec.working_dir.empty()) {
    return launch(io, program, std::move(spec.args), std::move(*spec.stdio),
                  bp::process_start_dir{std::move(spec.working_dir)},
                  std::move(*spec.env), std::forward<Inits>(extra)...);
  }
  if (spec.stdio && spec.env) {
    return launch(io, program, std::move(spec.args), std::move(*spec.stdio),
                  std::move(*spec.env), std::forward<Inits>(extra)...);
  }
  if (spec.stdio && !spec.working_dir.empty()) {
    return launch(io, program, std::move(spec.args), std::move(*spec.stdio),
                  bp::process_start_dir{std::move(spec.working_dir)},
                  std::forward<Inits>(extra)...);
  }
  if (spec.stdio) {
    return launch(io, program, std::move(spec.args), std::move(*spec.stdio),
                  std::forward<Inits>(extra)...);
  }
  if (spec.env && !spec.working_dir.empty()) {
    return launch(io, program, std::move(spec.args),
                  bp::process_start_dir{std::move(spec.working_dir)},
                  std::move(*spec.env), std::forward<Inits>(extra)...);
  }
  if (spec.env) {
    return launch(io, program, std::move(spec.args), std::move(*spec.env),
                  std::forward<Inits>(extra)...);
  }
  if (!spec.working_dir.empty()) {
    return launch(io, program, std::move(spec.args),
                  bp::process_start_dir{std::move(spec.working_dir)},
                  std::forward<Inits>(extra)...);
  }
  return launch(io, program, std::move(spec.args),
                std::forward<Inits>(extra)...);
}

template <typename... Inits>
auto launch_process(io::IoContext &io, ProcessLaunchSpec spec,
                    Inits &&... extra) -> bp::process {
#if defined(BOOST_PROCESS_V2_POSIX)
  bp::posix::vfork_launcher launcher;
  return invoke_process(launcher, io, std::move(spec), NewProcessGroupInit{},
                        std::forward<Inits>(extra)...);
#else
  auto launcher = [](auto &ctx, const char *command, auto args, auto &&... init) {
    return bp::process(ctx, command, std::move(args),
                       std::forward<decltype(init)>(init)...);
  };
  return invoke_process(launcher, io, std::move(spec),
                        std::forward<Inits>(extra)...);
#endif
}

} // namespace dagforge
