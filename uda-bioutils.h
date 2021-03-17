#ifndef UDA_BIOUTILS_H
#define UDA_BIOUTILS_H

#include <impala_udf/udf.h>

using namespace impala_udf;

void BoundedArrayInit(FunctionContext* context, StringVal* val);
void BoundedArrayUpdate(FunctionContext* context, const BigIntVal& input, StringVal* val);
void BoundedArrayMerge(FunctionContext* context, const StringVal& src, StringVal* dst);
StringVal StringStructSerialize(FunctionContext* context, const StringVal& val);	
StringVal BoundedArrayPrintFinalize(FunctionContext* context, const StringVal& val);
BigIntVal BoundedArrayCountFinalize(FunctionContext* context, const StringVal& val);
DoubleVal AgreementFinalize(FunctionContext* context, const StringVal& val);

void RunningMomentInit(FunctionContext* context, StringVal* val);
void RunningMomentUpdate(FunctionContext* ctx, const DoubleVal& B, StringVal* dst);
void RunningMomentUpdate(FunctionContext* ctx, const BigIntVal& B, StringVal* dst);
void RunningMomentMerge(FunctionContext* ctx, const StringVal& src, StringVal* dst);
DoubleVal RunningMomentPopulationVarianceFinalize(FunctionContext* context, const StringVal& rms);
DoubleVal RunningMomentSkewnessFinalize(FunctionContext* context, const StringVal& rms);
DoubleVal RunningMomentKurtosisFinalize(FunctionContext* context, const StringVal& rms);

// Bitwise Or Aggregate Function
void BitwiseOrInit(FunctionContext* context, BigIntVal* val);
void BitwiseOrUpdateMerge(FunctionContext* context, const BigIntVal& src, BigIntVal* dst);
BigIntVal BitwiseOrFinalize(FunctionContext* context, const BigIntVal& val);

#endif
