cmake_minimum_required(VERSION 3.22)

# ─── Discover all test sources automatically ──────────────────────────────────
file(GLOB TEST_SOURCES CONFIGURE_DEPENDS "*.cpp")

# ─── Helper: create one test executable per source file ──────────────────────
# We create three variants of each test:
#   <name>            — plain Release/Debug build
#   <name>_asan       — AddressSanitizer + UBSan
#   <name>_tsan       — ThreadSanitizer
#
# Each variant is registered with CTest so `ctest` can run all of them.

include(GoogleTest)
foreach(test_src ${TEST_SOURCES})
    # Strip directory and extension to get target name
    get_filename_component(test_name ${test_src} NAME_WE)

    # ── Plain build ────────────────────────────────────────────────────────────
    add_executable(${test_name} ${test_src})
    target_link_libraries(${test_name}
        PRIVATE
            hft_core
            GTest::gtest_main
    )
    target_compile_options(${test_name} PRIVATE -O2)
    gtest_discover_tests(${test_name}
        DISCOVERY_TIMEOUT 30
    )

    # ── ASan + UBSan variant ──────────────────────────────────────────────────
    add_executable(${test_name}_asan ${test_src})
    target_link_libraries(${test_name}_asan
        PRIVATE
            hft_core
            GTest::gtest_main
            asan_flags
    )
    target_compile_options(${test_name}_asan PRIVATE
        -O1
        -fsanitize=address,undefined
        -fno-omit-frame-pointer
        -fno-optimize-sibling-calls
    )
    target_link_options(${test_name}_asan PRIVATE
        -fsanitize=address,undefined
    )
    gtest_discover_tests(${test_name}_asan
        TEST_SUFFIX _asan
        DISCOVERY_TIMEOUT 30
    )

    # ── TSan variant ──────────────────────────────────────────────────────────
    # TSan is incompatible with ASan — must be a separate binary.
    add_executable(${test_name}_tsan ${test_src})
    target_link_libraries(${test_name}_tsan
        PRIVATE
            hft_core
            GTest::gtest_main
            tsan_flags
    )
    target_compile_options(${test_name}_tsan PRIVATE
        -O1
        -fsanitize=thread
        -fno-omit-frame-pointer
    )
    target_link_options(${test_name}_tsan PRIVATE
        -fsanitize=thread
    )
    gtest_discover_tests(${test_name}_tsan
        TEST_SUFFIX _tsan
        DISCOVERY_TIMEOUT 60
    )
endforeach()
