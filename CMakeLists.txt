cmake_minimum_required(VERSION 3.22)
project(hft_ome VERSION 0.1.0 LANGUAGES CXX)

# ─── C++20 Standard ───────────────────────────────────────────────────────────
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ─── Export compile commands for clangd ───────────────────────────────────────
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ─── Base compile options ─────────────────────────────────────────────────────
add_compile_options(
    -Wall -Wextra -Wpedantic -Wconversion -Wshadow
    -Wno-unused-parameter
)

# ─── Build type defaults ──────────────────────────────────────────────────────
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# ─── Release flags: max performance ───────────────────────────────────────────
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -mtune=native -flto -DNDEBUG")

# ─── Debug flags ──────────────────────────────────────────────────────────────
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g3 -fno-omit-frame-pointer")

# ─── Dependencies via FetchContent ────────────────────────────────────────────
include(FetchContent)

FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.14.0
)

set(BENCHMARK_ENABLE_WERROR OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
    googlebenchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG        v1.8.3
)

set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(googletest googlebenchmark)

# ─── liburing (system package expected: sudo apt install liburing-dev) ────────
find_library(URING_LIB uring)
find_path(URING_INCLUDE liburing.h)

if(URING_LIB AND URING_INCLUDE)
    message(STATUS "liburing found: ${URING_LIB}")
    add_library(liburing INTERFACE)
    target_link_libraries(liburing INTERFACE ${URING_LIB})
    target_include_directories(liburing INTERFACE ${URING_INCLUDE})
else()
    message(WARNING "liburing not found — io_uring targets will be skipped")
endif()

# ─── Core interface library (header-only) ─────────────────────────────────────
add_library(hft_core INTERFACE)
target_include_directories(hft_core INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_compile_features(hft_core INTERFACE cxx_std_20)

# ─── Sanitizer build targets ──────────────────────────────────────────────────
function(add_sanitizer_target name flags)
    add_library(${name}_flags INTERFACE)
    target_compile_options(${name}_flags INTERFACE ${flags} ${ARGN})
    target_link_options(${name}_flags INTERFACE ${flags} ${ARGN})
endfunction()

add_sanitizer_target(asan  "-fsanitize=address,undefined" "-fno-omit-frame-pointer")
add_sanitizer_target(tsan  "-fsanitize=thread" "-fno-omit-frame-pointer")
add_sanitizer_target(ubsan "-fsanitize=undefined" "-fno-omit-frame-pointer")

# ─── Tests ────────────────────────────────────────────────────────────────────
enable_testing()
add_subdirectory(tests)

# ─── Benchmarks ───────────────────────────────────────────────────────────────
add_subdirectory(benchmarks)

# ─── Main engine executable ───────────────────────────────────────────────────
add_subdirectory(src)
