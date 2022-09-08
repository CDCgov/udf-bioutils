// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Altered by Sam Shepard for use with udf-mathutils
// 2017-07-08

#ifndef UDF_BIOUTILS_H
#define UDF_BIOUTILS_H

#include <impala_udf/udf.h>
#include <string>
#include <vector>

using namespace impala_udf;

// private functions
DoubleVal qt(FunctionContext *context, const DoubleVal &confidence, const BigIntVal &sample_size,
             const BooleanVal &two_tailed);
DoubleVal ci_t(FunctionContext *context, const DoubleVal &confidence, const BigIntVal &sample_size,
               const DoubleVal &sample_std, const BooleanVal &two_tailed);
DoubleVal ci_t_twoSided(FunctionContext *context, const DoubleVal &confidence,
                        const BigIntVal &sample_size, const DoubleVal &sample_std);

#endif
