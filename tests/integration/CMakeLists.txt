# M1 integration probe: driven by the pytest harness (tests/test_tls.py),
# not registered with ctest — it needs a live server.

add_executable(egrpc_tls_connect tls_connect_main.cc)
target_include_directories(egrpc_tls_connect PRIVATE ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(egrpc_tls_connect PRIVATE egrpc_static egrpc_warnings)
