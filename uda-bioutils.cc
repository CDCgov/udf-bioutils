// uda-bioutils.cc - Sam Shepard - 2020
// Impala user-defined AGGREGATE functions for CDC biofinformatics.
// Preferable to using Spark when applicable.
#include "uda-bioutils.h"
#include <assert.h>
#include <string>
#include <limits>
#include <memory>
//#include <iostream>

#include "common.h"




using namespace impala_udf;
using namespace std;

inline StringVal to_StringVal(FunctionContext* context, const std::string& s) {
	StringVal result(context, s.size());
	memcpy(result.ptr, s.c_str(), s.size());
	return result;
}


// Structure that takes values from -16 to +16. This makes it suitable for logfold which is generally ±12, but not as a general function.
// One could try to follow a more complicated procedure that would store the data in a hash. This would involve more memory management and determination of the serialization state.
// I use BigInt for counts, but one could be less conservative to save space. Larger data structures (CUTOFF ~7.5K) yield out of memory errors. Size affects performance.
struct BoundedArrayStruct {
	static const int CUTOFF = 16;
 	int upper;
	int lower;
 	uint64_t counts[ CUTOFF * 2 + 1 ];
};

IMPALA_UDF_EXPORT
void BoundedArrayInit(FunctionContext* context, StringVal* val) {
	val->ptr = context->Allocate(sizeof(BoundedArrayStruct));

	// Exit on failed allocation. Impala will fail the query after some time.
	if (val->ptr == NULL) {
		*val = StringVal::null();
		return;
	}

	/// Initialize the string object containing our Struct
	val->is_null = false;
	val->len = sizeof(BoundedArrayStruct);

	// Set our Struct counts to zero
	BoundedArrayStruct* bda = reinterpret_cast<BoundedArrayStruct*>(val->ptr);
	memset(val->ptr, 0, val->len );
	bda->upper = -bda->CUTOFF;
	bda->lower =  bda->CUTOFF;
}

IMPALA_UDF_EXPORT
void BoundedArrayUpdate(FunctionContext* context, const BigIntVal& input, StringVal* val) {
	if (input.is_null || val->is_null) {
		return;
	}

	BoundedArrayStruct* barray = reinterpret_cast<BoundedArrayStruct*>(val->ptr);
	if ( input.val > barray->CUTOFF ) {
		// update count of greatest value
		++barray->counts[ barray->CUTOFF * 2 ];	
		// Upper is greatest value (not index). 
		// If first input, UPPER = LOWER = CUTOFF
		// Otherwise, lower is already set.
		barray->upper = barray->CUTOFF; 	 
	} else if ( input.val < -(barray->CUTOFF) ) {
		// update count of least value
		++barray->counts[0];

		// Lower is least value (not index)
		// If first input, LOWER = UPPER = CUTOFF
		// Otherwise, upper is already set.
		barray->lower = - barray->CUTOFF; 
	} else {
		++barray->counts[ input.val + barray->CUTOFF ];

		if ( input.val > barray->upper ) {
			barray->upper = input.val;
		}

		if ( input.val < barray->lower ) {
			barray->lower = input.val;
		}
	}
}

IMPALA_UDF_EXPORT
void BoundedArrayMerge(FunctionContext* context, const StringVal& src, StringVal* dst) {
	if (src.is_null || dst->is_null) {
		return;
	}

	const BoundedArrayStruct* src_bda = reinterpret_cast<const BoundedArrayStruct*>(src.ptr);
	BoundedArrayStruct* dst_bda = reinterpret_cast<BoundedArrayStruct*>(dst->ptr);

	for ( int i  = src_bda->lower + src_bda->CUTOFF; i <= src_bda->upper + src_bda->CUTOFF; i++ ) {
		dst_bda->counts[i] += src_bda->counts[i];
	}

	if ( src_bda->lower < dst_bda->lower ) {
		dst_bda->lower = src_bda->lower;
	}

	if ( src_bda->upper > dst_bda->upper ) {
		dst_bda->upper = src_bda->upper;
	}
}

IMPALA_UDF_EXPORT
StringVal StringStructSerialize(FunctionContext* context, const StringVal& val) {	
	if (val.is_null) {
		context->Free(val.ptr);
		return StringVal::null();
	}
	// Copy the value into Impala-managed memory with StringVal::CopyFrom().
	// NB: CopyFrom() will return a null StringVal and and fail the query if the allocation
	// fails because of lack of memory.
	StringVal result = StringVal::CopyFrom(context, val.ptr, val.len);
	context->Free(val.ptr);
	return result;
}

// Prints out the bounded array data for debugging purposes.
IMPALA_UDF_EXPORT
StringVal BoundedArrayPrintFinalize(FunctionContext* context, const StringVal& val) {
	BoundedArrayStruct* bda = reinterpret_cast<BoundedArrayStruct*>(val.ptr);
	StringVal result;

	if ( val.is_null || bda->lower > bda->upper ) {
		result = StringVal::null();
	} else {
		std::string buffer = "";
		for(int v = bda->lower; v <= bda->upper; v++ ) {
			if ( v > bda->lower ) { buffer += " "; }
			buffer += "(" + std::to_string(v) + "," + std::to_string(bda->counts[v + bda->CUTOFF] ) + ")"; 
		}
		// Copies the result to memory owned by Impala
		result = to_StringVal(context, buffer);
	}

	context->Free(val.ptr);
	return result;
}

// Simply counts the counts in the array. Intended for debugging. COUNT() is preferred.
IMPALA_UDF_EXPORT
BigIntVal BoundedArrayCountFinalize(FunctionContext* context, const StringVal& val) {
	BoundedArrayStruct* bda = reinterpret_cast<BoundedArrayStruct*>(val.ptr);
	BigIntVal result;

	if ( val.is_null || bda->lower > bda->upper ) {
		result = BigIntVal::null();
	} else {
		int64_t s = 0;
		for(int v = bda->lower; v <= bda->upper; v++ ) {
			s += bda->counts[v + bda->CUTOFF];
		}
		// Copies the result to memory owned by Impala
		result = BigIntVal(s);
	}

	context->Free(val.ptr);
	return result;
}

// Intended only for logfold, but could be modified for other ranges. 
IMPALA_UDF_EXPORT
DoubleVal AgreementFinalize(FunctionContext* context, const StringVal& val) {
	BoundedArrayStruct* bda = reinterpret_cast<BoundedArrayStruct*>(val.ptr);
	DoubleVal result;

	if ( val.is_null || bda->lower > bda->upper ) {
		result = DoubleVal::null();
	} else {
		// convert to index and calculate range
		int start 	= bda->lower + bda->CUTOFF;		// index, starting
		int limit 	= bda->upper + bda->CUTOFF + 1;		// index, limit
		int K		= limit - start;			// number of categories
		int S		= 0;					// non-zero categories
		uint64_t TC	= 0;					// total count
		uint64_t LC	= 0;					// layer count
		double LW	= 0.0;					// layer weight
		double LA	= 0.0;					// layer agreement
		double AA	= 0.0;					// average agreement
		double U	= 0.0;					// unimodality coefficient
		uint64_t minnz	= 0; 					// min non-zero value
	
		// If K < 3, agreement is undefined. Our data structure stretches to bounds. Thus:
		// K = 0 => no data, so return null
		// K = 1 => S = 1, we add the triplet for unimodality
		// K = 2 => S = 2, so we must add the triplet remainder & calculate
		if ( K == 2 ) {
			// S = 2 <= K == 2
			// If at upperbound, add element lower
			if ( bda->upper == bda->CUTOFF ) {
				start--;
			// otherwise add one higher
			} else {
				limit++;
			}
			K++;
		}
		std::unique_ptr<bool[]> P(new bool[K]); // allocate pattern vector

		// Initialize TC, P & S
		for(int i = start; i < limit; i++ ) {
			if ( bda->counts[i] > 0 ) {
				TC += bda->counts[i]; S++;
				P[i - start] = true;
				if ( bda->counts[i] < minnz ) {
					minnz = bda->counts[i];
				}
			} else {
				P[i - start] = false;
			}
		}

		// Singletons are unimodal, assume +/- 1 categories for a complete triplet
		if ( S == 1 ) {
			// S == 1 <= K == 1
			context->Free(val.ptr);
			return DoubleVal(1.0);
		}

		while( S > 0 ) {
			if ( S == K || S == 1) {
				U = 1;
			} else {
				int TU		= 0;			// triplets unimodal
				int TDU		= 0;			// triplets not unimodal
				for(int x = 0; x < (K-2); x++ ) {
					for(int y = (x+1); y < (K-1); y++) {
						for(int z = (y+1); z < K; z++) {
							if ( P[x] && ! P[y] && P[z] )	{ TDU++; }
							if ( P[y] && P[x] != P[z] ) 	{ TU++;  } 
						}
					}
				}
				if ( (TU + TDU) == 0 ) {
					U = 0.0;
				} else {
					U =  static_cast<double>( (K-2)*TU - (K-1)*TDU ) / static_cast<double>( (K-2)*(TU + TDU) );
				}
			}
			LA = U * (1.0 - static_cast<double>(S-1) / static_cast<double>(K-1)); 

			// Reset minnz & LC
			LC 	= 0;
			minnz 	= std::numeric_limits<uint64_t>::max();
			for(int i = start; i < limit; i++) {
				if ( P[i-start] ) {
					if ( bda->counts[i] < minnz ) {
						minnz = bda->counts[i];
					}
				}
			}

			// Adjust pattern vector and counts
			for(int i = 0; i < K; i++) {
				if ( P[i] ) { 
					bda->counts[i+start] -= minnz; LC++;
					if ( bda->counts[i+start] == 0 ) { 
						P[i] = false;
						S--;
					}	
				}
			}

			LW = static_cast<double>(LC * minnz) / static_cast<double>(TC);
			AA += LW * LA;
		} 
		
		result = DoubleVal(AA);
	}

	context->Free(val.ptr);
	return result;
}
