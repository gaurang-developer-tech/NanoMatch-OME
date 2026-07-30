cmake_minimum_required(VERSION 3.22)

file(GLOB BENCH_SOURCES CONFIGURE_DEPENDS "*.cpp")

foreach(bench_src ${BENCH_SOURCES})
    get_filename_component(bench_name ${bench_src} NAME_WE)

    add_executable(${bench_name} ${bench_src})

    target_include_directories(${bench_name} PRIVATE
        ${CMAKE_SOURCE_DIR}/src
    )

    target_link_libraries(${bench_name}
        PRIVATE
            hft_core          # Order, OrderPool, types
            benchmark::benchmark_main   # provides main(); BENCHMARK_MAIN() calls it
    )

    # -O3 -march=native: enable full vectorisation + AVX2 auto-vectoriser.
    # -fno-omit-frame-pointer: keeps stack frames intact for perf record.
    # -fno-inline: optional — uncomment to measure un-inlined call overhead.
    target_compile_options(${bench_name} PRIVATE
        -O3
        -march=native
        -fno-omit-frame-pointer
        -funroll-loops
    )

    # Name the benchmark binary with a _bench suffix for clarity.
    set_target_properties(${bench_name} PROPERTIES OUTPUT_NAME "${bench_name}")

endforeach()
