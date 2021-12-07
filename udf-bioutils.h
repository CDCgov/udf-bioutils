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

#ifndef UDF_BIOUTILS_H
#define UDF_BIOUTILS_H

#include <impala_udf/udf.h>
#include <string>
#include <vector>
#include "boost/date_time/gregorian/gregorian.hpp"

using namespace impala_udf;


// Epi week structure
struct epiweek_t {
	int year;
	int week;
};

// private functions
bool comp_allele				(std::string s1,std::string s2);
struct epiweek_t 				date_to_epiweek( boost::gregorian::date d );

StringVal Sort_List_By_Substring	(FunctionContext* context, const StringVal& listVal, const StringVal& delimVal );
StringVal Sort_List_By_Substring_Unique	(FunctionContext* context, const StringVal& listVal, const StringVal& delimVal );
StringVal Sort_List_By_Set		(FunctionContext* context, const StringVal& listVal, const StringVal& delimVal, const StringVal& outDelimVal );
StringVal Sort_Allele_List		(FunctionContext* context, const StringVal& listVal, const StringVal& delimVal );
StringVal To_AA				(FunctionContext* context, const StringVal& ntsVal );
StringVal To_AA_Mutant			(FunctionContext* context, const StringVal& ntsVal, const StringVal& alleleVal, const IntVal& pos );
StringVal Rev_Complement		(FunctionContext* context, const StringVal& ntsVal );
StringVal Substring_By_Range		(FunctionContext* context, const StringVal& sequence, const StringVal& rangeMap );
StringVal Mutation_List_Strict		(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2 );
StringVal Mutation_List_PDS		(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2, const StringVal& pairwise_delete_set );
StringVal Mutation_List_Strict_GLY	(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2 );
StringVal Mutation_List_Strict		(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2, const StringVal& rangeMap );
StringVal Mutation_List_No_Ambiguous	(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2 );
IntVal Hamming_Distance			(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2 );
IntVal Hamming_Distance_Pairwise_Delete	(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2, const StringVal& pairwise_delete_set );
IntVal Nt_Distance			(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2 );
DoubleVal Physiochemical_Distance	(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2 );
StringVal Physiochemical_Distance_List	(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2 );
BooleanVal Contains_An_Element		(FunctionContext* context, const StringVal& string1, const StringVal& string2, const StringVal& delimVal );
BooleanVal Is_An_Element		(FunctionContext* context, const StringVal& string1, const StringVal& string2, const StringVal& delimVal );
BooleanVal Contains_Symmetric		(FunctionContext* context, const StringVal& string1, const StringVal& string2 );
StringVal Complete_String_Date		(FunctionContext* context, const StringVal& dateStr );
StringVal nt_id				(FunctionContext* context, const StringVal& sequence );
StringVal variant_hash			(FunctionContext* context, const StringVal& sequence );
StringVal Range_From_List		(FunctionContext* context, const StringVal& listVal, const StringVal& delimVal );
StringVal md5				(FunctionContext* context, int num_vars, const StringVal* args );
StringVal nt_std			(FunctionContext* context, const StringVal& sequence );
StringVal aa_std			(FunctionContext* context, const StringVal& sequence );
BooleanVal Find_Set_In_String		(FunctionContext* context, const StringVal& haystackVal, const StringVal& needlesVal );


IntVal Convert_String_To_EPI_Week	(FunctionContext* context, const StringVal& dateStr, const BooleanVal& yearFormat );
IntVal Convert_String_To_EPI_Week	(FunctionContext* context, const StringVal& dateStr );
IntVal Convert_Timestamp_To_EPI_Week	(FunctionContext* context, const TimestampVal& tsVal );
IntVal Convert_Timestamp_To_EPI_Week	(FunctionContext* context, const TimestampVal& tsVal, const BooleanVal& yearFormat );
IntVal Longest_Deletion			(FunctionContext* context, const StringVal& sequence );
IntVal Number_Deletions			(FunctionContext* context, const StringVal& sequence );

#endif
