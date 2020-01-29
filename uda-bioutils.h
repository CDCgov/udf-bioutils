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

#endif
