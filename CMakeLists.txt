cmake_minimum_required(VERSION 3.25)

project(TaskMaster VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Enable ccache if available
find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    set(CMAKE_C_COMPILER_LAUNCHER ${CCACHE_PROGRAM})
    set(CMAKE_CXX_COMPILER_LAUNCHER ${CCACHE_PROGRAM})
    message(STATUS "Using ccache: ${CCACHE_PROGRAM}")
endif()

# Parallel compilation
include(ProcessorCount)
ProcessorCount(CPU_COUNT)
if(CPU_COUNT GREATER 0)
    set(CMAKE_BUILD_PARALLEL_LEVEL ${CPU_COUNT})
endif()

option(TASKMASTER_ENABLE_TESTS "Build tests" OFF)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(ProjectOptions)
include(Dependencies)

add_library(taskmaster_lib
    src/taskmaster/core/shard.cpp
    src/taskmaster/core/runtime.cpp
    src/taskmaster/io/context.cpp
    src/taskmaster/io/async_fd.cpp
    src/taskmaster/scheduler/cron.cpp
    src/taskmaster/scheduler/engine.cpp
    src/taskmaster/dag/dag.cpp
    src/taskmaster/dag/dag_run.cpp
    src/taskmaster/dag/dag_manager.cpp
    src/taskmaster/storage/persistence.cpp
    src/taskmaster/storage/recovery.cpp
    src/taskmaster/executor/shell_executor.cpp
    src/taskmaster/executor/noop_executor.cpp
    src/taskmaster/executor/config_builder.cpp
    src/taskmaster/app/application.cpp
    src/taskmaster/app/api/api_server.cpp
    src/taskmaster/app/services/execution_service.cpp
    src/taskmaster/app/services/scheduler_service.cpp
    src/taskmaster/app/services/persistence_service.cpp
    src/taskmaster/app/services/event_service.cpp
    src/taskmaster/app/http/http_types.cpp
    src/taskmaster/app/http/http_parser.cpp
    src/taskmaster/app/http/router.cpp
    src/taskmaster/app/http/http_server.cpp
    src/taskmaster/app/http/http_client.cpp
    src/taskmaster/app/http/websocket.cpp
    src/taskmaster/config/dag_definition.cpp
    src/taskmaster/config/dag_file_loader.cpp
    src/taskmaster/config/config_watcher.cpp
    src/taskmaster/config/config.cpp
    src/taskmaster/cli/serve.cpp
    src/taskmaster/cli/run.cpp
    src/taskmaster/cli/list.cpp
    src/taskmaster/cli/status.cpp
    src/taskmaster/cli/validate.cpp
    src/taskmaster/util/daemon.cpp
    src/taskmaster/xcom/xcom_extractor.cpp
    src/taskmaster/xcom/template_resolver.cpp
)

# Precompiled headers for faster compilation (20-40% improvement)
target_precompile_headers(taskmaster_lib PRIVATE
    <cstdint>
    <cstddef>
    <string>
    <string_view>
    <vector>
    <array>
    <span>
    <unordered_map>
    <unordered_set>
    <map>
    <set>
    <memory>
    <optional>
    <variant>
    <expected>
    <functional>
    <utility>
    <atomic>
    <mutex>
    <condition_variable>
    <thread>
    <chrono>
    <ranges>
    <coroutine>
    <system_error>
    <format>
)
add_library(TaskMaster::lib ALIAS taskmaster_lib)

target_include_directories(taskmaster_lib
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(taskmaster_lib
    PUBLIC
        nlohmann_json::nlohmann_json
        yaml-cpp::yaml-cpp
        SQLite::SQLite3
        llhttp::llhttp
        OpenSSL::SSL
        OpenSSL::Crypto
    PRIVATE
        CLI11::CLI11
)

taskmaster_configure_target(taskmaster_lib)

# Main executable
add_executable(taskmaster
    src/main.cpp
)

target_link_libraries(taskmaster PRIVATE TaskMaster::lib CLI11::CLI11)
taskmaster_configure_target(taskmaster)

# Tests
if(TASKMASTER_ENABLE_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

