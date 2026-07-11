cmake_minimum_required(VERSION 3.16)
project(egrpc VERSION 0.1.0 LANGUAGES CXX)

# ---------------------------------------------------------------------------
# Language standard: C++17, no compiler extensions.
# ---------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(GNUInstallDirs)

# ---------------------------------------------------------------------------
# Sanitizer knob (applied globally to compile AND link when non-empty).
# ---------------------------------------------------------------------------
set(EGRPC_SANITIZE "" CACHE STRING
  "Comma-separated sanitizers, e.g. address,undefined or thread")
if(EGRPC_SANITIZE)
  add_compile_options(-fsanitize=${EGRPC_SANITIZE} -fno-omit-frame-pointer)
  add_link_options(-fsanitize=${EGRPC_SANITIZE} -fno-omit-frame-pointer)
endif()

# ---------------------------------------------------------------------------
# Dependencies — SYSTEM ONLY. Fail hard if missing (no vendoring/FetchContent).
# ---------------------------------------------------------------------------
find_package(OpenSSL REQUIRED)
find_package(Nghttp2 REQUIRED)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

find_package(PkgConfig REQUIRED)
pkg_check_modules(ProtobufLite REQUIRED IMPORTED_TARGET protobuf-lite)

# ---------------------------------------------------------------------------
# Warnings-as-errors interface target (reusable by the test dir).
# ---------------------------------------------------------------------------
add_library(egrpc_warnings INTERFACE)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(egrpc_warnings INTERFACE -Wall -Wextra -Werror)
endif()

# ---------------------------------------------------------------------------
# Sources (explicit — no globbing).
# ---------------------------------------------------------------------------
set(EGRPC_SOURCES
  src/transport/tls_socket.cc
  src/transport/http2_session.cc
  src/transport/timer_heap.cc
  src/transport/wakeup.cc
  src/core/event_thread.cc
  src/core/channel_impl.cc
  src/core/call_state.cc
  src/core/grpc_wire.cc
  src/core/backoff.cc
  src/api/serialization.cc
)

# ---------------------------------------------------------------------------
# One object library, compiled once with PIC, feeding both static and shared.
# ---------------------------------------------------------------------------
# Interface target carrying the public usage requirements (includes + system
# deps) shared by the object library and both final libraries.
add_library(egrpc_deps INTERFACE)
target_include_directories(egrpc_deps INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(egrpc_deps INTERFACE
  OpenSSL::SSL
  OpenSSL::Crypto
  nghttp2::nghttp2
  PkgConfig::ProtobufLite
  Threads::Threads
)

add_library(egrpc_objects OBJECT ${EGRPC_SOURCES})
set_target_properties(egrpc_objects PROPERTIES POSITION_INDEPENDENT_CODE ON)
# Internal headers live next to the sources (src/transport/..., src/core/...).
target_include_directories(egrpc_objects PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(egrpc_objects PUBLIC egrpc_deps)
target_link_libraries(egrpc_objects PRIVATE egrpc_warnings)

# Static and shared libraries built from the shared object set.
add_library(egrpc_static STATIC $<TARGET_OBJECTS:egrpc_objects>)
add_library(egrpc_shared SHARED $<TARGET_OBJECTS:egrpc_objects>)

foreach(_lib egrpc_static egrpc_shared)
  set_target_properties(${_lib} PROPERTIES OUTPUT_NAME egrpc)
  target_link_libraries(${_lib} PUBLIC egrpc_deps)
endforeach()

set_target_properties(egrpc_shared PROPERTIES
  VERSION ${PROJECT_VERSION}
  SOVERSION 0
)

# ---------------------------------------------------------------------------
# Tests: default ON only when egrpc is the top-level project (cmake 3.16 safe).
# ---------------------------------------------------------------------------
if(CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR)
  set(_egrpc_is_top ON)
else()
  set(_egrpc_is_top OFF)
endif()
option(EGRPC_BUILD_TESTS "Build unit tests" ${_egrpc_is_top})

if(EGRPC_BUILD_TESTS)
  include(CTest)
  add_subdirectory(tests/unit)
  add_subdirectory(tests/integration)
endif()

# ---------------------------------------------------------------------------
# Install rules.
# ---------------------------------------------------------------------------
install(TARGETS egrpc_static egrpc_shared
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

install(DIRECTORY include/grpcpp include/egrpc
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
