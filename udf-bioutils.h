// Samuel S. Shepard

#ifndef UDF_BIOUTILS_H
#define UDF_BIOUTILS_H

#include "boost/date_time/gregorian/gregorian.hpp"
#include <impala_udf/udf.h>
#include <string>
#include <vector>

using namespace impala_udf;

// Epi week structure
struct epiweek_t {
    int year;
    int week;
};

int get_epoch_offset() {
    boost::gregorian::date EPOCH(1970, 1, 1);
    return EPOCH.day_number();
}
static int EPOCH_OFFSET = get_epoch_offset();

int get_final_saturday() {
    boost::gregorian::date EPOCH(9999, 12, 31);
    return EPOCH.day_number() + 1;
}
static int FINAL_SATURDAY = get_final_saturday();

// private functions
bool comp_allele(std::string s1, std::string s2);
struct epiweek_t date_to_epiweek(boost::gregorian::date d);

StringVal Sort_List_By_Substring(
    FunctionContext *context, const StringVal &listVal, const StringVal &delimVal
);
StringVal Sort_List_By_Substring_Unique(
    FunctionContext *context, const StringVal &listVal, const StringVal &delimVal
);
StringVal Sort_List_By_Set(
    FunctionContext *context, const StringVal &listVal, const StringVal &delimVal,
    const StringVal &outDelimVal
);
StringVal Sort_Allele_List(
    FunctionContext *context, const StringVal &listVal, const StringVal &delimVal
);
StringVal To_AA(FunctionContext *context, const StringVal &ntsVal);
StringVal To_AA_Mutant(
    FunctionContext *context, const StringVal &ntsVal, const StringVal &alleleVal, const IntVal &pos
);
StringVal Rev_Complement(FunctionContext *context, const StringVal &ntsVal);
StringVal Substring_By_Range(
    FunctionContext *context, const StringVal &sequence, const StringVal &rangeMap
);

StringVal Mutation_List_PDS(
    FunctionContext *context, const StringVal &sequence1, const StringVal &sequence2,
    const StringVal &pairwise_delete_set
);
StringVal Mutation_List_Strict_GLY(
    FunctionContext *context, const StringVal &sequence1, const StringVal &sequence2
);
StringVal Mutation_List_Strict(
    FunctionContext *context, const StringVal &sequence1, const StringVal &sequence2
);
StringVal Mutation_List_Strict_Range(
    FunctionContext *context, const StringVal &sequence1, const StringVal &sequence2,
    const StringVal &rangeMap
);
StringVal Mutation_List_No_Ambiguous(
    FunctionContext *context, const StringVal &sequence1, const StringVal &sequence2
);
IntVal Hamming_Distance(
    FunctionContext *context, const StringVal &sequence1, const StringVal &sequence2
);
IntVal Hamming_Distance_Pairwise_Delete(
    FunctionContext *context, const StringVal &sequence1, const StringVal &sequence2,
    const StringVal &pairwise_delete_set
);
IntVal Nt_Distance(
    FunctionContext *context, const StringVal &sequence1, const StringVal &sequence2
);
StringVal Sequence_Diff(FunctionContext *context, const StringVal &seq1, const StringVal &seq2);
StringVal Sequence_Diff_NT(FunctionContext *context, const StringVal &seq1, const StringVal &seq2);
DoubleVal Physiochemical_Distance(
    FunctionContext *context, const StringVal &sequence1, const StringVal &sequence2
);
StringVal Physiochemical_Distance_List(
    FunctionContext *context, const StringVal &sequence1, const StringVal &sequence2
);
BooleanVal Contains_An_Element(
    FunctionContext *context, const StringVal &string1, const StringVal &string2,
    const StringVal &delimVal
);
BooleanVal Is_An_Element(
    FunctionContext *context, const StringVal &string1, const StringVal &string2,
    const StringVal &delimVal
);
BooleanVal Contains_Symmetric(
    FunctionContext *context, const StringVal &string1, const StringVal &string2
);
StringVal Complete_String_Date(FunctionContext *context, const StringVal &dateStr);
StringVal nt_id(FunctionContext *context, const StringVal &sequence);
StringVal variant_hash(FunctionContext *context, const StringVal &sequence);
StringVal Range_From_List(
    FunctionContext *context, const StringVal &listVal, const StringVal &delimVal
);
StringVal md5(FunctionContext *context, int num_vars, const StringVal *args);
StringVal nt_std(FunctionContext *context, const StringVal &sequence);
StringVal aa_std(FunctionContext *context, const StringVal &sequence);
BooleanVal Find_Set_In_String(
    FunctionContext *context, const StringVal &haystackVal, const StringVal &needlesVal
);


IntVal Convert_String_To_EPI_Week(
    FunctionContext *context, const StringVal &dateStr, const BooleanVal &yearFormat
);
IntVal Convert_String_To_EPI_Week(FunctionContext *context, const StringVal &dateStr);
IntVal Convert_Timestamp_To_EPI_Week(FunctionContext *context, const TimestampVal &tsVal);
IntVal Convert_Timestamp_To_EPI_Week(
    FunctionContext *context, const TimestampVal &tsVal, const BooleanVal &yearFormat
);
IntVal Longest_Deletion(FunctionContext *context, const StringVal &sequence);
IntVal Number_Deletions(FunctionContext *context, const StringVal &sequence);

StringVal Cut_Paste(
    FunctionContext *context, const StringVal &my_string, const StringVal &delim,
    const StringVal &range_map
);

StringVal Cut_Paste_Output(
    FunctionContext *context, const StringVal &my_string, const StringVal &delim,
    const StringVal &range_map, const StringVal &out_delim
);

IntVal NT_To_CDS_Position(
    FunctionContext *context, const StringVal &oriMap, const StringVal &cdsMap,
    const BigIntVal &oriPos
);

IntVal NT_To_AA_Position(
    FunctionContext *context, const StringVal &oriMap, const StringVal &cdsMap,
    const BigIntVal &oriPos
);

StringVal NT_Position_To_CDS_Codon(
    FunctionContext *context, const StringVal &oriMap, const StringVal &cdsMap,
    const StringVal &cdsAlignment, const BigIntVal &oriPos
);

StringVal NT_Position_To_CDS_Codon_Mutant(
    FunctionContext *context, const StringVal &oriMap, const StringVal &cdsMap,
    const StringVal &cdsAlignment, const BigIntVal &oriPos, const StringVal &allele
);

StringVal NT_Position_To_Mutation_AA3(
    FunctionContext *context, const StringVal &oriMap, const StringVal &cdsMap,
    const StringVal &cdsAlignment, const BigIntVal &oriPos, const StringVal &major_allele,
    const StringVal &minor_allele
);

StringVal To_AA3(FunctionContext *context, const StringVal &ntsVal);

DateVal Date_Ending_In_Saturday_STR(FunctionContext *context, const StringVal &dateStr);
DateVal Date_Ending_In_Saturday_TS(FunctionContext *context, const TimestampVal &tsVal);
DateVal Date_Ending_In_Saturday_DATE(FunctionContext *context, const DateVal &dateVal);


DateVal Fortnight_Date_Either_STR(
    FunctionContext *context, const StringVal &dateStr, const BooleanVal &legacy_default_week
);
DateVal Fortnight_Date_Either_TS(
    FunctionContext *context, const TimestampVal &tsVal, const BooleanVal &legacy_default_week
);
DateVal Fortnight_Date_Either(
    FunctionContext *context, const DateVal &dateVal, const BooleanVal &legacy_default_week
);

DateVal Fortnight_Date_STR(FunctionContext *context, const StringVal &dateStr);
DateVal Fortnight_Date_TS(FunctionContext *context, const TimestampVal &tsVal);
DateVal Fortnight_Date(FunctionContext *context, const DateVal &dateVal);

#endif
