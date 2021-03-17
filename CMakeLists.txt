# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.1)
project(UDX_BioUtils)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "build")
# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "build")

find_program(CLANG_EXECUTABLE clang++)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -ggdb -O2 -pedantic")
SET(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# Hide symbols in UDF code by default - UDF functions should be explicitly exported.
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fvisibility=hidden -fvisibility-inlines-hidden")

# Function to generate rule to cross compile a source file to an IR module.
# This should be called with the .cc src file and it will generate a
# src-file-ir target that can be built.
# e.g. COMPILE_TO_IR(test.cc) generates the "test-ir" make target.
# Disable compiler optimizations because generated IR is optimized at runtime
# Added C++11 standard to the flags
set(IR_COMPILE_FLAGS "-emit-llvm" "-c" "-std=c++11")
function(COMPILE_TO_IR SRC_FILE)
  get_filename_component(BASE_NAME ${SRC_FILE} NAME_WE)
  set(OUTPUT_FILE "build/${BASE_NAME}.ll")
  add_custom_command(
    OUTPUT ${OUTPUT_FILE}
    COMMAND ${CLANG_EXECUTABLE} ${IR_COMPILE_FLAGS} ${SRC_FILE} -o ${OUTPUT_FILE}
    DEPENDS ${SRC_FILE})
  add_custom_target(${BASE_NAME}-ir ALL DEPENDS ${OUTPUT_FILE})
endfunction(COMPILE_TO_IR)

# Build the UDA/UDFs into a shared library.
add_library(udfbioutils SHARED udf-bioutils.cc )
add_library(udfmathutils SHARED udf-mathutils.cc)
add_library(udabioutils SHARED uda-bioutils.cc)
add_library(udf-sero SHARED udf-sero.cc)

# Custom targest to cross compile UDA/UDF to ir
if (CLANG_EXECUTABLE)
  COMPILE_TO_IR(udf-bioutils.cc )
  COMPILE_TO_IR(udf-mathutils.cc )
  COMPILE_TO_IR(uda-bioutils.cc )
  COMPILE_TO_IR(udf-sero.cc )
endif(CLANG_EXECUTABLE)

# This is an example of how to use the test harness to help develop UDF and UDAs.
target_link_libraries(udfbioutils ImpalaUdf)
target_link_libraries(udfbioutils crypto)

target_link_libraries(udabioutils ImpalaUdf)
add_executable(uda-bioutils-test uda-bioutils-test.cc)
target_link_libraries(uda-bioutils-test udabioutils)

target_link_libraries(udfmathutils ImpalaUdf)

target_link_libraries(udf-sero ImpalaUdf)

add_executable(udf-sero-test udf-sero-test.cc)
target_link_libraries(udf-sero-test udf-sero)
