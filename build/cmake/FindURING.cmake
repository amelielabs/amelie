#
# Find the IO_URING library
#
# export:
#
# URING_INCLUDE_DIR
# URING_LIBRARIES
# URING_FOUND
#

find_path(URING_INCLUDE_DIR
          NAMES liburing.h
          HINTS ${URING_ROOT_DIR}/include)

find_library(URING_LIBRARIES
             NAMES uring
             HINTS ${URING_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(URING DEFAULT_MSG URING_LIBRARIES URING_INCLUDE_DIR)

mark_as_advanced(URING_LIBRARIES URING_INCLUDE_DIR)
