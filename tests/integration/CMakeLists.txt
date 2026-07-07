# M1 integration probe: driven by the pytest harness (tests/test_tls.py),
# not registered with ctest — it needs a live server.

add_executable(egrpc_tls_connect tls_connect_main.cc)
target_include_directories(egrpc_tls_connect PRIVATE ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(egrpc_tls_connect PRIVATE egrpc_static egrpc_warnings)

# M2 probe: session bring-up + keepalive, driven by tests/test_keepalive.py.
add_executable(egrpc_h2_keepalive h2_keepalive_main.cc)
target_include_directories(egrpc_h2_keepalive PRIVATE ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(egrpc_h2_keepalive PRIVATE egrpc_static egrpc_warnings)
