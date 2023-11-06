// Samuel S. Shepard, CDC

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

StringVal Sort_Cohorts(
    FunctionContext *context, const StringVal &listVal, const StringVal &delimVal
);

#endif
