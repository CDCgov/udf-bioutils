// uda-bioutils.cc - Sam Shepard - 2020
// Impala user-defined AGGREGATE functions for CDC biofinformatics.
// Preferable to using Spark when applicable.
#include "uda-bioutils.h"
#include <assert.h>
#include <string>
#include <limits>
#include <cmath>
#include <algorithm>
#include "common.h"

#include <iostream>



using namespace impala_udf;
using namespace std;

inline StringVal to_StringVal(FunctionContext* context, const std::string& s) {
	StringVal result(context, s.size());
	memcpy(result.ptr, s.c_str(), s.size());
	return result;
}


// May be used for Variance (M2), Skewness (M3), and Kurtosis (M4)
struct RunningMomentStruct {
	double mu;
	double m2;
	double m3;
	double m4;
  	int64_t n;
};

IMPALA_UDF_EXPORT
void RunningMomentInit(FunctionContext* context, StringVal* val) {
	val->ptr = context->Allocate(sizeof(RunningMomentStruct));

	// Exit on failed allocation. Impala will fail the query after some time.
	if (val->ptr == NULL) {
		*val = StringVal::null();
		return;
	}

	/// Initialize the string object containing our Struct
	val->is_null = false;
	val->len = sizeof(RunningMomentStruct);

	// Set our Struct counts to zero
	RunningMomentStruct* rms = reinterpret_cast<RunningMomentStruct*>(val->ptr);
	memset(val->ptr, 0, val->len );
}

// Algorithm specified by Timothy B. Terriberry ("pairwise update" single-pass algorithm)
// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics
// https://people.xiph.org/~tterribe/notes/homs.html 
// n	= nA + nB
// ∆	= µB - µA
// µ	= µA + ∆ * nB / n
// m2	= m2_A + m2_B + ∆^2*nA*nB/n
// m3	= m3_A + m3_B + ∆^3*nA*nB*(nA - nB)/n^2 + 3∆(nA*m2_B - nB*m2_A)/n
// m4	= m4_A + m4_B + ∆^4*nA*nB*(nA^2 - nA*nB + nB^2)/n^3 + 6∆^2((nA^2*m2_B + nB^2*m2_A)/n^2 + 4∆( nA*m3_B - nB*m3_A)/n
IMPALA_UDF_EXPORT
void RunningMomentUpdate(FunctionContext* ctx, const DoubleVal& B, StringVal* dst) {
	if ( B.is_null || dst->is_null) { return; }
 	RunningMomentStruct* A = reinterpret_cast<RunningMomentStruct*>(dst->ptr);

	// µB	= B.val
	// nB	= 1
	// ∆	= µB - µA
	double delta 	= B.val - A->mu;
	double nA	= static_cast<double>(A->n);
	
	// Update n
	// n	= nA + nB = nA + 1
	A->n++;
	double n	= static_cast<double>(A->n);
	double delta_n	= delta / n;
	double d2_n2	= std::pow(delta_n,2);
	double d2nA_n 	= delta * delta_n * nA;

	// µ	= µA + ∆ * nB / n
	A->mu += delta_n;

	// m4 + d^4 * nA (nA^2 - nA + 1) / n^3 + 6d^2*m2/n^2 - 4d*m3/n
	A->m4 += d2nA_n * d2_n2 * (std::pow(nA,2) - nA + 1) + 6 * d2_n2 * A->m2 - 4 * delta_n * A->m3;

	// m3 + d^3 * (nA - 1) / n^2 - 3 * delta * m2 / n
	A->m3 += d2nA_n * delta_n * (nA-1) - 3 * delta_n * A->m2;

	// m2 + d^2 * nA / n
	A->m2 += d2nA_n;
}

IMPALA_UDF_EXPORT
void RunningMomentUpdate(FunctionContext* ctx, const BigIntVal& B, StringVal* dst) {
	if ( B.is_null || dst->is_null) { return; }
 	RunningMomentStruct* A = reinterpret_cast<RunningMomentStruct*>(dst->ptr);

	// µB	= B.val
	// nB	= 1
	// ∆	= µB - µA
	double delta 	= static_cast<double>(B.val) - A->mu;
	double nA	= static_cast<double>(A->n);
	
	// Update n
	// n	= nA + nB = nA + 1
	A->n++;
	double n	= static_cast<double>(A->n);
	double delta_n	= delta / n;
	double d2_n2	= std::pow(delta_n,2);
	double d2nA_n 	= delta * delta_n * nA;

	// µ	= µA + ∆ * nB / n
	A->mu += delta_n;

	// m4 + d^4 * nA (nA^2 - nA + 1) / n^3 + 6d^2*m2/n^2 - 4d*m3/n
	A->m4 += d2nA_n*d2_n2*(std::pow(nA,2) - nA + 1) + 6*d2_n2*A->m2 - 4*delta_n*A->m3;

	// m3 + d^3 * (nA - 1) / n^2 - 3 * delta * m2 / n
	A->m3 += d2nA_n*delta_n*(nA-1) - 3*delta_n*A->m2;

	// m2 + d^2 * nA / n
	A->m2 += d2nA_n;
}
	
// Algorithm specified by Timothy B. Terriberry ("pairwise update" single-pass algorithm)
// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics
// https://people.xiph.org/~tterribe/notes/homs.html 
IMPALA_UDF_EXPORT
void RunningMomentMerge(FunctionContext* ctx, const StringVal& src, StringVal* dst) {
	if (src.is_null || dst->is_null) {
		return;
	}

	RunningMomentStruct* B = reinterpret_cast<RunningMomentStruct*>(src.ptr);
	RunningMomentStruct* A = reinterpret_cast<RunningMomentStruct*>(dst->ptr);
	if (B->n == 0) {
		return;
	}

	double nA 	= static_cast<double>(A->n);
	double nB	= static_cast<double>(B->n);
	double nA2	= std::pow(nA,2);
	double nB2	= std::pow(nB,2);
	double nAnB	= nA * nB;

	// Update n
	// n = nA + nB
	A->n		+= B->n;
	double n 	= static_cast<double>(A->n);

	// ∆	= µB - µA
	double delta	= B->mu - A->mu;
	double delta_n	= delta / n;
	double d2_n2	= std::pow(delta_n,2);
	double d2nAnB_n	= delta * delta_n * nAnB;



	// µ	= µA + ∆ * nB / n
	A->mu	+= delta_n * nB;
	// m4	= m4_A + m4_B + ∆^4*nA*nB*(nA^2 - nA*nB + nB^2)/n^3 + 6∆^2((nA^2*m2_B + nB^2*m2_A)/n^2 + 4∆( nA*m3_B - nB*m3_A)/n
	A->m4	+= B->m4 + d2nAnB_n*d2_n2*(nA2 - nAnB + nB2) + 6*d2_n2*(nA2*B->m2 + nB2*A->m2) + 4*delta_n*(nA*B->m3 - nB*A->m3);	
	// m3	= m3_A + m3_B + ∆^3*nA*nB*(nA - nB)/n^2 + 3∆(nA*m2_B - nB*m2_A)/n
	A->m3	+= B->m3 + d2nAnB_n*delta_n*(nA - nB) + 3*delta_n*(nA*B->m2 - nB*A->m2);
	// m2	= m2_A + m2_B + ∆^2*nA*nB/n
	A->m2	+= B->m2 + d2nAnB_n;
}

// Use StringStructSerialize

IMPALA_UDF_EXPORT
DoubleVal RunningMomentSampleVarianceFinalize(FunctionContext* context, const StringVal& rms) {
	RunningMomentStruct* A = reinterpret_cast<RunningMomentStruct*>(rms.ptr);
	DoubleVal result;

	if ( rms.is_null || A->n < 2 ) {
		result = DoubleVal::null();
	} else {
		// Sample variance
		result = DoubleVal(A->m2 / static_cast<double>(A->n - 1));
	}

	context->Free(rms.ptr);
	return result;
}

IMPALA_UDF_EXPORT
DoubleVal RunningMomentPopulationVarianceFinalize(FunctionContext* context, const StringVal& rms) {
	RunningMomentStruct* A = reinterpret_cast<RunningMomentStruct*>(rms.ptr);
	DoubleVal result;

	if ( rms.is_null || A->n == 0 ) {
		result = DoubleVal::null();
	} else {
		// Population variance
		if (A->n == 1) {
			result = DoubleVal(0.0);
		} else {
			result = DoubleVal(A->m2 / static_cast<double>(A->n));
		}
	}

	context->Free(rms.ptr);
	return result;
}

IMPALA_UDF_EXPORT
DoubleVal RunningMomentSkewnessFinalize(FunctionContext* context, const StringVal& rms) {
	RunningMomentStruct* A = reinterpret_cast<RunningMomentStruct*>(rms.ptr);
	DoubleVal result;

	if ( rms.is_null || A->n < 3 ) {
		result = DoubleVal::null();
	} else {
		// Population variance
		if (A->n == 1) {
			result = DoubleVal(0.0);
		} else {
			double N = static_cast<double>(A->n);
			double g1 = std::sqrt(N) * A->m3 / std::pow(A->m2,1.5);
			// type 2, similar to SAS
			result = DoubleVal( g1 * std::sqrt( N * (N - 1.0) ) / (N - 2.0) );
		}
	}

	context->Free(rms.ptr);
	return result;
}

IMPALA_UDF_EXPORT
DoubleVal RunningMomentKurtosisFinalize(FunctionContext* context, const StringVal& rms) {
	RunningMomentStruct* A = reinterpret_cast<RunningMomentStruct*>(rms.ptr);
	DoubleVal result;

	if ( rms.is_null || A->n < 4 ) {
		result = DoubleVal::null();
	} else {
		// Population variance
		if (A->n == 1) {
			result = DoubleVal(0.0);
		} else {
			double N = static_cast<double>(A->n);
			double g2 = (N*A->m4 / std::pow(A->m2,2)) - 3.0;
			// type 2, similar to SAS
			result = DoubleVal( ( (N + 1.0) * g2 + 6.0) * (N-1.0) / ((N-2.0) * (N-3.0))  );
		}
	}

	context->Free(rms.ptr);
	return result;
}

/****************************************************************/
/****************************************************************/
/****************************************************************/

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

inline bool pick_side(int K, int s, int l, uint64_t C[]) {
	uint64_t RHS = 0;
	uint64_t LHS = 0;
	
	for(int i = s; i < s+10; i++ ) {
		LHS += C[i];
	}

	for(int i = l - 10; i < l; i++ ) {
		RHS += C[i];
	}

	// less weight, so fold RHS
	if ( RHS != LHS ) {
		return RHS < LHS;
	} else {
		return K % 2;
	}
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
		bool P[10]	= {false};				// fixed pattern vector, size must be ≤ CUTOFF

		// Singletons are unimodal, assume +/- 1 categories for a complete triplet
		if ( K == 1 ) {
			// K=1 => S=1
			context->Free(val.ptr);
			return DoubleVal(1.0);

		// Fix categories to 10
		} else if ( K > 10 ) {
			// reduction of the range should infringe on bounds
			while( K != 10 ) {
				// true to fold in RHS
				if ( pick_side(K, start, limit, bda->counts) ) {
					// reduce limit, last element WAS limit - 1
					limit--;
					// add last element to its inside neighbor 
					bda->counts[limit-1] += bda->counts[limit];
					
				// false to fold in LHS
				} else {
					// fold in to the inside, update start
					bda->counts[start+1] += bda->counts[start];
					start++;
				}
				K--;
			}

		// Adjust bounds to 10, data should already be zeroed
		} else if ( K < 10 ) {
			unsigned int array_bound = bda->CUTOFF * 2 + 1;
			unsigned int left_over = 10 - K;
			unsigned int inc = std::min( array_bound - limit, left_over );
			
			// add to limit, subtract from start, but not beyond what is needed or available
			limit 		+= inc; 
			left_over 	-= inc;
			start 		-= left_over;
			K 		= 10;

			// this should never happen, but better than crashing the server
			if ( start < 0 ) {
				K 	+= start;
				start 	= 0;
			}
		}
	
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


// ---------------------------------------------------------------------------
// Bitwise Or Aggregate Function
// ---------------------------------------------------------------------------
IMPALA_UDF_EXPORT
void BitwiseOrInit(FunctionContext* context, BigIntVal* val) {
	val->is_null = true;
	val->val = 0;
}

IMPALA_UDF_EXPORT
void BitwiseOrUpdateMerge(FunctionContext* context, const BigIntVal& src, BigIntVal* dst) {
	if ( ! src.is_null ) {
		if ( ! dst->is_null ) {
			dst->val |= src.val;
		} else {
			dst->is_null = false;  
			dst->val = src.val;
		}
	}
}

IMPALA_UDF_EXPORT
BigIntVal BitwiseOrFinalize(FunctionContext* context, const BigIntVal& val) {
  return val;
}
