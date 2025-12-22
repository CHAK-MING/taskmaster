cmake_minimum_required(VERSION 3.25)

project(TaskMaster VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(TASKMASTER_ENABLE_TESTS "Build tests" OFF)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(ProjectOptions)
include(Dependencies)

add_library(taskmaster_lib
    src/taskmaster/application.cpp
    src/taskmaster/config.cpp
    src/taskmaster/cron.cpp
    src/taskmaster/dag.cpp
    src/taskmaster/dag_manager.cpp
    src/taskmaster/dag_run.cpp
    src/taskmaster/engine.cpp
    src/taskmaster/persistence.cpp
    src/taskmaster/recovery.cpp
    src/taskmaster/shell_executor.cpp
    src/taskmaster/runtime.cpp
    src/taskmaster/api/api_server.cpp
    src/taskmaster/api/websocket_hub.cpp
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
        Crow::Crow
)

if(UNIX AND NOT APPLE AND LIBURING_FOUND)
    target_link_libraries(taskmaster_lib PUBLIC ${LIBURING_LIBRARIES})
    target_include_directories(taskmaster_lib PUBLIC ${LIBURING_INCLUDE_DIRS})
endif()

taskmaster_configure_target(taskmaster_lib)

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
