# M0 unit-test executable, wired into ctest.
#
# Reached via add_subdirectory(tests/unit) from a top-level CMakeLists that has
# already set CMAKE_CXX_STANDARD=17 and called include(CTest). We deliberately
# set warnings-as-errors locally on this target rather than relying on parent
# scope, and mark the doctest include dir SYSTEM so the vendored header is
# exempt from -Werror.

add_executable(egrpc_unit_tests
  test_sanity.cc
  test_timer_heap.cc
  test_wakeup.cc
  test_event_thread.cc
)

target_include_directories(egrpc_unit_tests SYSTEM PRIVATE
  ${PROJECT_SOURCE_DIR}/third_party)
# Unit tests exercise internal headers directly (src/transport, src/core).
target_include_directories(egrpc_unit_tests PRIVATE ${PROJECT_SOURCE_DIR}/src)

target_link_libraries(egrpc_unit_tests PRIVATE egrpc_static)

target_compile_options(egrpc_unit_tests PRIVATE -Wall -Wextra -Werror)

add_test(NAME egrpc_unit_tests COMMAND egrpc_unit_tests)
