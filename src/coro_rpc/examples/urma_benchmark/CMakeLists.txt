set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output/examples/coro_rpc)
# --no-urma also supports local verification without an installed URMA library.
add_executable(coro_rpc_urma_benchmark urma_benchmark.cpp)
