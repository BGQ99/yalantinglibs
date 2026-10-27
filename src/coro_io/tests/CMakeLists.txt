set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output/tests)
add_executable(coro_io_test
        test_io_context_pool.cpp
        test_corofile.cpp
        test_shared_file_handle.cpp
        test_load_balancer.cpp
        test_client_pool.cpp
        test_rate_limiter.cpp
        test_coro_channel.cpp
        test_cancel.cpp
        main.cpp
        )
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_SYSTEM_NAME MATCHES "Windows") # mingw-w64
    target_link_libraries(coro_io_test PRIVATE ws2_32 mswsock)
endif()
add_test(NAME coro_io_test COMMAND coro_io_test)
set_tests_properties(coro_io_test PROPERTIES RESOURCE_LOCK network_ports)

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_executable(coro_file_error_test
            test_file_error_paths.cpp
            main.cpp
            )
    if (CMAKE_SIZEOF_VOID_P EQUAL 8)
        target_link_options(coro_file_error_test PRIVATE
                "-Wl,--wrap=_Znwm"
                "-Wl,--wrap=_Znam"
                )
    elseif (CMAKE_SIZEOF_VOID_P EQUAL 4)
        target_link_options(coro_file_error_test PRIVATE
                "-Wl,--wrap=_Znwj"
                "-Wl,--wrap=_Znaj"
                )
    endif()
    add_test(NAME coro_file_error_test COMMAND coro_file_error_test)
endif()

add_subdirectory(ibverbs)
add_subdirectory(networkdirect)

if(YLT_ENABLE_URMA AND CMAKE_SYSTEM_NAME STREQUAL "Linux" AND
   CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang" AND CMAKE_SIZEOF_VOID_P EQUAL 8)
    # Use real SDK declarations and test-only C API implementations; no device
    # or liburma is needed by this executable.
    add_executable(urma_buffer_ownership_test
            urma/test_urma_buffer_ownership.cpp
            urma/mock_urma.cpp
            main.cpp)
    get_property(urma_test_libraries DIRECTORY PROPERTY LINK_LIBRARIES)
    list(REMOVE_ITEM urma_test_libraries -lurma)
    set_property(TARGET urma_buffer_ownership_test PROPERTY
                 LINK_LIBRARIES ${urma_test_libraries})
    target_link_options(urma_buffer_ownership_test PRIVATE "-Wl,--wrap=_Znwm")
    add_test(NAME urma_buffer_ownership_test COMMAND urma_buffer_ownership_test)
    set_tests_properties(urma_buffer_ownership_test PROPERTIES TIMEOUT 60)
endif()

if(YLT_HAS_OWN_RING)
    add_executable(own_ring_test test_own_ring.cpp)
    target_compile_definitions(own_ring_test PRIVATE OWN_RING_FAULT_INJECT)
    add_test(NAME own_ring_test COMMAND own_ring_test --duration=true --no-colors=true)
    set_tests_properties(own_ring_test PROPERTIES TIMEOUT 90)
endif()
