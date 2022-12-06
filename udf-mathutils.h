// Samuel S. Shepard, CDC

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
