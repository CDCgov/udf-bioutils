// Samuel S. Shepard, CDC

#ifndef UDA_BIOUTILS_H
#define UDA_BIOUTILS_H

#include <impala_udf/udf.h>

using namespace impala_udf;

void BoundedArrayInit(FunctionContext *context, StringVal *val);
void BoundedArrayUpdate(FunctionContext *context, const BigIntVal &input, StringVal *val);
void BoundedArrayMerge(FunctionContext *context, const StringVal &src, StringVal *dst);
StringVal StringStructSerialize(FunctionContext *context, const StringVal &val);
StringVal BoundedArrayPrintFinalize(FunctionContext *context, const StringVal &val);
BigIntVal BoundedArrayCountFinalize(FunctionContext *context, const StringVal &val);
DoubleVal AgreementFinalize(FunctionContext *context, const StringVal &val);

void RunningMomentInit(FunctionContext *context, StringVal *val);
void RunningMomentUpdate(FunctionContext *ctx, const DoubleVal &B, StringVal *dst);
void RunningMomentUpdate(FunctionContext *ctx, const BigIntVal &B, StringVal *dst);
void RunningMomentMerge(FunctionContext *ctx, const StringVal &src, StringVal *dst);
DoubleVal RunningMomentPopulationVarianceFinalize(FunctionContext *context, const StringVal &rms);
DoubleVal RunningMomentSkewnessFinalize(FunctionContext *context, const StringVal &rms);
DoubleVal RunningMomentKurtosisFinalize(FunctionContext *context, const StringVal &rms);

// Bitwise Or Aggregate Function
void BitwiseOrInit(FunctionContext *context, BigIntVal *val);
void BitwiseOrUpdateMerge(FunctionContext *context, const BigIntVal &src, BigIntVal *dst);
BigIntVal BitwiseOrFinalize(FunctionContext *context, const BigIntVal &val);

// Entropy Calculation Function
void CalcCharEntropyInit(FunctionContext *context, StringVal *val);
void CalcCharEntropyUpdate(FunctionContext *context, const StringVal &input, StringVal *val);
void CalcCharEntropyMerge(FunctionContext *context, const StringVal &src, StringVal *dst);
StringVal CalcCharEntropySerialize(FunctionContext *context, const StringVal &val);
DoubleVal CalcCharEntropyFinalize(FunctionContext *context, const StringVal &val);

void CalcNTEntropyInit(FunctionContext *context, StringVal *val);
void CalcNTEntropyUpdate(FunctionContext *context, const StringVal &input, StringVal *val);
void CalcNTEntropyMerge(FunctionContext *context, const StringVal &src, StringVal *dst);
StringVal CalcNTEntropySerialize(FunctionContext *context, const StringVal &val);
DoubleVal CalcNTEntropyFinalize(FunctionContext *context, const StringVal &val);

void CalcAAEntropyInit(FunctionContext *context, StringVal *val);
void CalcAAEntropyUpdate(FunctionContext *context, const StringVal &input, StringVal *val);
void CalcAAEntropyMerge(FunctionContext *context, const StringVal &src, StringVal *dst);
StringVal CalcAAEntropySerialize(FunctionContext *context, const StringVal &val);
DoubleVal CalcAAEntropyFinalize(FunctionContext *context, const StringVal &val);

void CalcCDEntropyInit(FunctionContext *context, StringVal *val);
void CalcCDEntropyUpdate(FunctionContext *context, const StringVal &input, StringVal *val);
void CalcCDEntropyMerge(FunctionContext *context, const StringVal &src, StringVal *dst);
StringVal CalcCDEntropySerialize(FunctionContext *context, const StringVal &val);
DoubleVal CalcCDEntropyFinalize(FunctionContext *context, const StringVal &val);
#endif
