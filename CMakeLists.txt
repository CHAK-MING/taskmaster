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
    src/taskmaster/core/io_ring.cpp
    src/taskmaster/core/shard.cpp
    src/taskmaster/core/runtime.cpp
    src/taskmaster/scheduler/cron.cpp
    src/taskmaster/scheduler/engine.cpp
    src/taskmaster/dag/dag.cpp
    src/taskmaster/dag/dag_run.cpp
    src/taskmaster/dag/dag_manager.cpp
    src/taskmaster/storage/config.cpp
    src/taskmaster/storage/persistence.cpp
    src/taskmaster/storage/recovery.cpp
    src/taskmaster/executor/shell_executor.cpp
    src/taskmaster/app/application.cpp
    src/taskmaster/app/api/api_server.cpp
    src/taskmaster/app/api/websocket_hub.cpp
    src/taskmaster/app/services/persistence_service.cpp
    src/taskmaster/app/services/event_service.cpp
    src/taskmaster/app/services/scheduler_service.cpp
    src/taskmaster/app/services/execution_service.cpp
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

# Mark Crow includes as SYSTEM to suppress warnings from third-party headers
get_target_property(CROW_INCLUDE_DIRS Crow::Crow INTERFACE_INCLUDE_DIRECTORIES)
if(CROW_INCLUDE_DIRS)
    target_include_directories(taskmaster_lib SYSTEM PUBLIC ${CROW_INCLUDE_DIRS})
endif()

target_link_libraries(taskmaster_lib
    PUBLIC
        nlohmann_json::nlohmann_json
        yaml-cpp::yaml-cpp
        SQLite::SQLite3
        Crow::Crow
)

if(UNIX AND NOT APPLE AND LIBURING_FOUND)
    target_link_libraries(taskmaster_lib PUBLIC ${LIBURING_LIBRARIES})
    target_include_directories(taskmaster_lib PUBLIC ${LIBURING_INCLUDE_DIRS})
endif()

taskmaster_configure_target(taskmaster_lib)

# Unity build option (30-50% faster first build)
option(TASKMASTER_UNITY_BUILD "Enable unity build" OFF)
if(TASKMASTER_UNITY_BUILD)
    set_target_properties(taskmaster_lib PROPERTIES
        UNITY_BUILD ON
        UNITY_BUILD_MODE BATCH
        UNITY_BUILD_BATCH_SIZE 8
    )
endif()

add_executable(taskmaster src/main.cpp)
target_link_libraries(taskmaster PRIVATE taskmaster_lib)
taskmaster_configure_target(taskmaster)

if(TASKMASTER_ENABLE_TESTS)
    add_subdirectory(tests)
endif()

install(TARGETS taskmaster taskmaster_lib
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)

install(DIRECTORY include/taskmaster
    DESTINATION include
)
