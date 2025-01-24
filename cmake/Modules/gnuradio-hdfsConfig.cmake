find_package(PkgConfig)

PKG_CHECK_MODULES(PC_GR_HDFS gnuradio-hdfs)

FIND_PATH(
    GR_HDFS_INCLUDE_DIRS
    NAMES gnuradio/hdfs/api.h
    HINTS $ENV{HDFS_DIR}/include
        ${PC_HDFS_INCLUDEDIR}
    PATHS ${CMAKE_INSTALL_PREFIX}/include
          /usr/local/include
          /usr/include
)

FIND_LIBRARY(
    GR_HDFS_LIBRARIES
    NAMES gnuradio-hdfs
    HINTS $ENV{HDFS_DIR}/lib
        ${PC_HDFS_LIBDIR}
    PATHS ${CMAKE_INSTALL_PREFIX}/lib
          ${CMAKE_INSTALL_PREFIX}/lib64
          /usr/local/lib
          /usr/local/lib64
          /usr/lib
          /usr/lib64
          )

include("${CMAKE_CURRENT_LIST_DIR}/gnuradio-hdfsTarget.cmake")

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(GR_HDFS DEFAULT_MSG GR_HDFS_LIBRARIES GR_HDFS_INCLUDE_DIRS)
MARK_AS_ADVANCED(GR_HDFS_LIBRARIES GR_HDFS_INCLUDE_DIRS)
