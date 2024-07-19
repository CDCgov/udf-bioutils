

cmake_minimum_required(VERSION 3.5)
project(UDX_BioUtils)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "build")
# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "build")

find_program(CLANG_EXECUTABLE clang++)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -ggdb -O2 -pedantic")
SET(CMAKE_CXX_STANDARD 20)
SET(CMAKE_CXX_STANDARD_REQUIRED ON)
# Hide symbols in UDF code by default - UDF functions should be explicitly exported.
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fvisibility=hidden -fvisibility-inlines-hidden")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=cascadelake")
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
