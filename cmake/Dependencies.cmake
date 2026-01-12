include(FetchContent)

# CMake 3.25+ supports SYSTEM in FetchContent
if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.25")
    set(FETCHCONTENT_SYSTEM TRUE)
endif()

# nlohmann_json
find_package(nlohmann_json QUIET)
if(NOT nlohmann_json_FOUND)
    message(STATUS "nlohmann_json not found, fetching from GitHub...")
    FetchContent_Declare(
        nlohmann_json
        GIT_REPOSITORY https://github.com/nlohmann/json.git
        GIT_TAG v3.11.3
        GIT_SHALLOW TRUE
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
        SYSTEM
    )
    set(JSON_BuildTests OFF CACHE INTERNAL "")
    FetchContent_MakeAvailable(nlohmann_json)
else()
    message(STATUS "Found system nlohmann_json: ${nlohmann_json_VERSION}")
endif()

# yaml-cpp
find_package(yaml-cpp QUIET)
if(NOT yaml-cpp_FOUND)
    message(STATUS "yaml-cpp not found, fetching from GitHub...")
    FetchContent_Declare(
        yaml-cpp
        GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
        GIT_TAG 0.8.0
        GIT_SHALLOW TRUE
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
        SYSTEM
    )
    set(YAML_CPP_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(YAML_CPP_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
    set(YAML_CPP_BUILD_CONTRIB OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(yaml-cpp)
else()
    message(STATUS "Found system yaml-cpp: ${yaml-cpp_VERSION}")
endif()

# CLI11
find_package(CLI11 QUIET)
if(NOT CLI11_FOUND)
    message(STATUS "CLI11 not found, fetching from GitHub...")
    FetchContent_Declare(
        CLI11
        GIT_REPOSITORY https://github.com/CLIUtils/CLI11.git
        GIT_TAG v2.4.2
        GIT_SHALLOW TRUE
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
        SYSTEM
    )
    set(CLI11_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(CLI11_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
    set(CLI11_BUILD_DOCS OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(CLI11)
else()
    message(STATUS "Found system CLI11: ${CLI11_VERSION}")
endif()

# SQLite3
find_package(SQLite3 QUIET)
if(NOT SQLite3_FOUND)
    message(STATUS "SQLite3 not found, fetching amalgamation...")
    FetchContent_Declare(
        sqlite3_fetch
        URL https://www.sqlite.org/2025/sqlite-amalgamation-3510100.zip
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
        SYSTEM
    )
    FetchContent_MakeAvailable(sqlite3_fetch)

    add_library(sqlite3_lib STATIC ${sqlite3_fetch_SOURCE_DIR}/sqlite3.c)
    target_include_directories(sqlite3_lib SYSTEM PUBLIC ${sqlite3_fetch_SOURCE_DIR})
    target_compile_definitions(sqlite3_lib PRIVATE
        SQLITE_THREADSAFE=1
        SQLITE_ENABLE_FTS5
        SQLITE_ENABLE_JSON1
    )
    add_library(SQLite::SQLite3 ALIAS sqlite3_lib)
else()
    message(STATUS "Found system SQLite3: ${SQLite3_VERSION}")
endif()

# liburing (Linux only)
if(UNIX AND NOT APPLE)
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(LIBURING QUIET liburing)
    endif()
    
    if(LIBURING_FOUND)
        message(STATUS "Found system liburing: ${LIBURING_VERSION}")
    else()
        message(WARNING "liburing not found. Please install: sudo apt install liburing-dev")
    endif()
endif()

# llhttp (HTTP parser)
find_package(llhttp QUIET)
if(NOT llhttp_FOUND)
    message(STATUS "llhttp not found, fetching from GitHub...")
    FetchContent_Declare(
        llhttp
        GIT_REPOSITORY https://github.com/nodejs/llhttp.git
        GIT_TAG release/v9.2.1
        GIT_SHALLOW TRUE
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
        SYSTEM
    )
    FetchContent_MakeAvailable(llhttp)
else()
    message(STATUS "Found system llhttp: ${llhttp_VERSION}")
endif()

# OpenSSL (for WebSocket handshake SHA1)
find_package(OpenSSL REQUIRED)
if(OpenSSL_FOUND)
    message(STATUS "Found OpenSSL: ${OPENSSL_VERSION}")
endif()

# GoogleTest (tests only)
if(TASKMASTER_ENABLE_TESTS)
    find_package(GTest QUIET)
    if(NOT GTest_FOUND)
        message(STATUS "GTest not found, fetching from GitHub...")
        FetchContent_Declare(
            googletest
            GIT_REPOSITORY https://github.com/google/googletest.git
            GIT_TAG v1.14.0
            GIT_SHALLOW TRUE
            DOWNLOAD_EXTRACT_TIMESTAMP TRUE
            SYSTEM
        )
        set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
        FetchContent_MakeAvailable(googletest)
    else()
        message(STATUS "Found system GTest: ${GTest_VERSION}")
    endif()
endif()

# Google Benchmark (tests only)
if(TASKMASTER_ENABLE_TESTS)
    find_package(benchmark QUIET)
    if(NOT benchmark_FOUND)
        message(STATUS "Google Benchmark not found, fetching from GitHub...")
        FetchContent_Declare(
            benchmark
            GIT_REPOSITORY https://github.com/google/benchmark.git
            GIT_TAG v1.8.3
            GIT_SHALLOW TRUE
            DOWNLOAD_EXTRACT_TIMESTAMP TRUE
            SYSTEM
        )
        set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
        set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
        FetchContent_MakeAvailable(benchmark)
    else()
        message(STATUS "Found system benchmark: ${benchmark_VERSION}")
    endif()
endif()

function(taskmaster_configure_target target_name)
    target_compile_features(${target_name} PRIVATE cxx_std_23)
    
    if(UNIX AND NOT APPLE AND LIBURING_FOUND)
        target_link_directories(${target_name} PRIVATE ${LIBURING_LIBRARY_DIRS})
        target_link_libraries(${target_name} PRIVATE ${LIBURING_LIBRARIES})
        target_include_directories(${target_name} SYSTEM PRIVATE ${LIBURING_INCLUDE_DIRS})
    endif()
    
    set_target_properties(${target_name} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
    )
endfunction()
