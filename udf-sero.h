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

// Altered by Sam Shepard for use with udf-bioutils
// 2017-07-08

#ifndef UDF_SERO_H
#define UDF_SERO_H

#include <impala_udf/udf.h>
#include <string>
#include <vector>

using namespace impala_udf;

// private functions
bool compare_cohorts(std::string a, std::string b);
inline std::vector<std::string> split_by_substr(const std::string &str, const std::string &delim);
inline StringVal to_StringVal(FunctionContext *context, const std::string &str);

StringVal Sort_Cohorts(FunctionContext *context, const StringVal &listVal,
                       const StringVal &delimVal);

#endif
