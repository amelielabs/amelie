#
# Find the PCRE brary
#
# export:
#
# PCRE_INCLUDE_DIR
# PCRE_LIBRARIES
# PCRE_FOUND
#

find_path(PCRE_INCLUDE_DIR
          NAMES pcre2.h
          HINTS ${PCRE_ROOT_DIR}/include)

find_library(PCRE_LIBRARIES
             NAMES pcre2-8
             HINTS ${PCRE_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(PCRE DEFAULT_MSG PCRE_LIBRARIES PCRE_INCLUDE_DIR)

mark_as_advanced(PCRE_LIBRARIES PCRE_INCLUDE_DIR)
