

cmake_minimum_required(VERSION 3.5)
project(UDX_BioUtils)

# For in-source builds (cmake .), redirect outputs to build/.
# For out-of-source builds (cmake -B build), the binary dir is already build/.
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_BINARY_DIR)
  set(LIBRARY_OUTPUT_PATH "build")
  set(EXECUTABLE_OUTPUT_PATH "build")
endif()

SET(CMAKE_CXX_STANDARD 20)
SET(CMAKE_CXX_STANDARD_REQUIRED ON)

# On standard RHEL8/AppStream (boost 1.66), system paths are used by default.
# For ubi8 and other environments, set BOOST_INCLUDEDIR and BOOST_LIBRARYDIR env vars
if(DEFINED ENV{BOOST_INCLUDEDIR} AND DEFINED ENV{BOOST_LIBRARYDIR})
    set(BOOST_INCLUDEDIR "$ENV{BOOST_INCLUDEDIR}" CACHE PATH "Boost include directory" FORCE)
    set(BOOST_LIBRARYDIR "$ENV{BOOST_LIBRARYDIR}" CACHE PATH "Boost library directory" FORCE)
    include_directories(SYSTEM ${BOOST_INCLUDEDIR})
    link_directories(${BOOST_LIBRARYDIR})
    message(STATUS "Using custom Boost paths: ${BOOST_INCLUDEDIR}, ${BOOST_LIBRARYDIR}")
endif()

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -pedantic")
# Hide symbols in UDF code by default - UDF functions should be explicitly exported.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fvisibility=hidden -fvisibility-inlines-hidden")
set(MARCH "cascadelake" CACHE STRING "Target architecture for -march")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=${MARCH}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -s")


# Build the UDA/UDFs into a shared library.
add_library(udfbioutils SHARED udf-bioutils.cc )
add_library(udfmathutils SHARED udf-mathutils.cc)
add_library(udabioutils SHARED uda-bioutils.cc)

# This is an example of how to use the test harness to help develop UDF and UDAs.
target_link_libraries(udfbioutils ImpalaUdf)
target_link_libraries(udfbioutils crypto)


target_link_libraries(udabioutils ImpalaUdf)
add_executable(uda-bioutils-test uda-bioutils-test.cc)
target_link_libraries(uda-bioutils-test udabioutils)

target_link_libraries(udfmathutils ImpalaUdf)

add_executable(udf-bioutils-test udf-bioutils-test.cc)
target_link_libraries(udf-bioutils-test udfbioutils)

if(NOT EXCLUDE_BENCHMARK)
add_executable(udf-bioutils-bmark udf-bioutils-bmark.cc)
add_custom_command(OUTPUT udf-bioutils-bmark.cc
    COMMAND benchmark/generate_bmark_code.py json/udf-bmark.json > udf-bioutils-bmark.cc
    DEPENDS json/udf-bmark.json)
target_link_libraries(udf-bioutils-bmark udfbioutils)
target_link_libraries(udf-bioutils-bmark pthread)
target_link_libraries(udf-bioutils-bmark benchmark)
endif()
