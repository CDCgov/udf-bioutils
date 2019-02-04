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

using namespace impala_udf;

// private functions
bool comp_allele				(std::string s1,std::string s2);
inline std::vector<std::string> split_by_substr	(const std::string& str, const std::string& delim);
inline StringVal to_StringVal			(FunctionContext* context, const std::string& str);

StringVal Sort_List_By_Substring	(FunctionContext* context, const StringVal& listVal, const StringVal& delimVal );
StringVal Sort_List_By_Substring_Unique	(FunctionContext* context, const StringVal& listVal, const StringVal& delimVal );
StringVal Sort_List_By_Set		(FunctionContext* context, const StringVal& listVal, const StringVal& delimVal, const StringVal& outDelimVal );
StringVal Sort_Allele_List		(FunctionContext* context, const StringVal& listVal, const StringVal& delimVal );
StringVal To_AA				(FunctionContext* context, const StringVal& ntsVal );
StringVal To_AA_Mutant			(FunctionContext* context, const StringVal& ntsVal, const StringVal& alleleVal, const IntVal& pos );
StringVal Rev_Complement		(FunctionContext* context, const StringVal& ntsVal );
StringVal Substring_By_Range		(FunctionContext* context, const StringVal& sequence, const StringVal& rangeMap );
StringVal Mutation_List_Strict		(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2 );
StringVal Mutation_List_No_Ambiguous	(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2 );
IntVal Hamming_Distance			(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2 );
IntVal Hamming_Distance_Pairwise_Delete	(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2, const StringVal& pairwise_delete_set );
IntVal Nt_Distance			(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2 );
BooleanVal Contains_An_Element		(FunctionContext* context, const StringVal& string1, const StringVal& string2, const StringVal& delimVal );
BooleanVal Is_An_Element		(FunctionContext* context, const StringVal& string1, const StringVal& string2, const StringVal& delimVal );
BooleanVal Contains_Symmetric		(FunctionContext* context, const StringVal& string1, const StringVal& string2 );

// available in CentOS 7, CDH 6
StringVal nt_id				(FunctionContext* context, const StringVal& sequence );
StringVal variant_hash			(FunctionContext* context, const StringVal& sequence );

#endif
