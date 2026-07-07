# FindNghttp2.cmake — locate the system nghttp2 library.
#
# Defines the IMPORTED target:
#   nghttp2::nghttp2
#
# And the classic result variables:
#   Nghttp2_FOUND
#   Nghttp2_INCLUDE_DIRS
#   Nghttp2_LIBRARIES
#   Nghttp2_VERSION

# Use pkg-config only to provide search hints; we never rely on it being present.
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
  pkg_check_modules(PC_NGHTTP2 QUIET libnghttp2)
endif()

find_path(Nghttp2_INCLUDE_DIR
  NAMES nghttp2/nghttp2.h
  HINTS
    ${PC_NGHTTP2_INCLUDEDIR}
    ${PC_NGHTTP2_INCLUDE_DIRS}
)

find_library(Nghttp2_LIBRARY
  NAMES nghttp2
  HINTS
    ${PC_NGHTTP2_LIBDIR}
    ${PC_NGHTTP2_LIBRARY_DIRS}
)

# Determine version: prefer pkg-config, else parse nghttp2ver.h.
set(Nghttp2_VERSION "")
if(PC_NGHTTP2_VERSION)
  set(Nghttp2_VERSION "${PC_NGHTTP2_VERSION}")
elseif(Nghttp2_INCLUDE_DIR AND EXISTS "${Nghttp2_INCLUDE_DIR}/nghttp2/nghttp2ver.h")
  file(STRINGS "${Nghttp2_INCLUDE_DIR}/nghttp2/nghttp2ver.h" _nghttp2_ver_line
    REGEX "^#[\t ]*define[\t ]+NGHTTP2_VERSION[\t ]+\"[^\"]+\"")
  if(_nghttp2_ver_line)
    string(REGEX REPLACE ".*\"([^\"]+)\".*" "\\1" Nghttp2_VERSION "${_nghttp2_ver_line}")
  endif()
  unset(_nghttp2_ver_line)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Nghttp2
  REQUIRED_VARS Nghttp2_LIBRARY Nghttp2_INCLUDE_DIR
  VERSION_VAR Nghttp2_VERSION
)

if(Nghttp2_FOUND)
  set(Nghttp2_INCLUDE_DIRS "${Nghttp2_INCLUDE_DIR}")
  set(Nghttp2_LIBRARIES "${Nghttp2_LIBRARY}")

  if(NOT TARGET nghttp2::nghttp2)
    add_library(nghttp2::nghttp2 UNKNOWN IMPORTED)
    set_target_properties(nghttp2::nghttp2 PROPERTIES
      IMPORTED_LOCATION "${Nghttp2_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${Nghttp2_INCLUDE_DIR}"
    )
  endif()
endif()

mark_as_advanced(Nghttp2_INCLUDE_DIR Nghttp2_LIBRARY)
