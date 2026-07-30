cmake_minimum_required(VERSION 3.22)

find_package(Threads REQUIRED)

# Main engine entry point — stub for now, grows as phases complete.
add_executable(hft_engine main.cpp)
target_link_libraries(hft_engine PRIVATE hft_core Threads::Threads uring)
if(TARGET liburing)
    target_link_libraries(hft_engine PRIVATE liburing)
endif()
target_compile_options(hft_engine PRIVATE -O3 -march=native)
