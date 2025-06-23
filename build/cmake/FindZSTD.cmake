#
# Find the Zstd compression library
#
# export:
#
# ZSTD_INCLUDE_DIR
# ZSTD_LIBRARIES
# ZSTD_FOUND
#

find_path(ZSTD_INCLUDE_DIR
          NAMES zstd.h
          HINTS ${ZSTD_ROOT_DIR}/include)

find_library(ZSTD_LIBRARIES
             NAMES zstd
             HINTS ${ZSTD_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ZSTD DEFAULT_MSG ZSTD_LIBRARIES ZSTD_INCLUDE_DIR)

mark_as_advanced(ZSTD_LIBRARIES ZSTD_INCLUDE_DIR)
