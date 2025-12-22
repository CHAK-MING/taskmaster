# cmake/ProjectOptions.cmake

# 通用编译器警告
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(
        -Wall
        -Wextra
        -Wpedantic
        -Wshadow
        -Wnon-virtual-dtor
        -Wold-style-cast
        -Wcast-align
        -Wunused
        -Woverloaded-virtual
        -Wnull-dereference
        -Wdouble-promotion
        -Wformat=2
    )
endif()

# MSVC 特定选项
if(MSVC)
    add_compile_options(/W4 /permissive-)
endif()

# 优化选项（Release 模式）
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    include(CheckCXXCompilerFlag)
    check_cxx_compiler_flag("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
    if(COMPILER_SUPPORTS_MARCH_NATIVE)
        add_compile_options(-march=native)
    endif()
endif()

# 设置 C++23（已在主文件中设置，但这里作为保障）
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 导出编译命令（已在主文件中设置）
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
