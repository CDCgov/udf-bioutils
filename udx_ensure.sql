create function if not exists udx.sort_list(string, string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Sort_List_By_Substring";
create function if not exists udx.sort_list_unique(string, string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Sort_List_By_Substring_Unique";
create function if not exists udx.sort_list_set(string, string, string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Sort_List_By_Set";
create function if not exists udx.sort_alleles(string, string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Sort_Allele_List";
create function if not exists udx.to_aa(string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="To_AA";
create function if not exists udx.to_aa(string,string,int) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="To_AA_Mutant";
create function if not exists udx.reverse_complement(string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Rev_Complement";
create function if not exists udx.substr_range(string,string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Substring_By_Range";
create function if not exists udx.range_from_list(string,string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Range_From_List";
create function if not exists udx.mutation_list(string,string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Mutation_List_Strict";
create function if not exists udx.mutation_list_pds(string,string,string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Mutation_List_PDS";
create function if not exists udx.mutation_list_gly(string,string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Mutation_List_Strict_GLY";
create function if not exists udx.mutation_list(string,string,string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Mutation_List_Strict";
create function if not exists udx.mutation_list_nt(string,string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Mutation_List_No_Ambiguous";
create function if not exists udx.hamming_distance(string,string) returns int location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Hamming_Distance";
create function if not exists udx.hamming_distance(string,string,string) returns int location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Hamming_Distance_Pairwise_Delete";
create function if not exists udx.nt_distance(string,string) returns int location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Nt_Distance";
create function if not exists udx.contains_element(string,string,string) returns boolean location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Contains_An_Element";
create function if not exists udx.is_element(string,string,string) returns boolean location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Is_An_Element";
create function if not exists udx.contains_sym(string,string) returns boolean location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Contains_Symmetric";
create function if not exists udx.nt_id(string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="nt_id";
create function if not exists udx.variant_hash(string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="variant_hash";
create function if not exists udx.complete_date(string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Complete_String_Date";
create function if not exists udx.md5(string ...) returns string location "/udx/ncird_id/prod/libudfbioutils.so" symbol="md5";
create function if not exists udx.longest_deletion(string) returns int location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Longest_Deletion";
create function if not exists udx.deletion_events(string) returns int location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Number_Deletions";
create function if not exists udx.nt_std(string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="nt_std";
create function if not exists udx.aa_std(string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="aa_std";
create function if not exists udx.pcd_list(string,string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Physiochemical_Distance_List";
create function if not exists udx.pcd(string,string) returns double location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Physiochemical_Distance";
create function if not exists udx.any_instr(string,string) returns boolean location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Find_Set_In_String";

create function if not exists default.sort_list(string, string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Sort_List_By_Substring";
create function if not exists default.sort_list_unique(string, string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Sort_List_By_Substring_Unique";
create function if not exists default.sort_list_set(string, string, string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Sort_List_By_Set";
create function if not exists default.sort_alleles(string, string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Sort_Allele_List";
create function if not exists default.to_aa(string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="To_AA";
create function if not exists default.to_aa(string,string,int) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="To_AA_Mutant";
create function if not exists default.reverse_complement(string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Rev_Complement";
create function if not exists default.substr_range(string,string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Substring_By_Range";
create function if not exists default.range_from_list(string,string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Range_From_List";
create function if not exists default.mutation_list(string,string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Mutation_List_Strict";
create function if not exists default.mutation_list_gly(string,string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Mutation_List_Strict_GLY";
create function if not exists default.mutation_list(string,string,string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Mutation_List_Strict";
create function if not exists default.mutation_list_nt(string,string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Mutation_List_No_Ambiguous";
create function if not exists default.hamming_distance(string,string) returns int location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Hamming_Distance";
create function if not exists default.hamming_distance(string,string,string) returns int location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Hamming_Distance_Pairwise_Delete";
create function if not exists default.nt_distance(string,string) returns int location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Nt_Distance";
create function if not exists default.contains_element(string,string,string) returns boolean location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Contains_An_Element";
create function if not exists default.is_element(string,string,string) returns boolean location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Is_An_Element";
create function if not exists default.contains_sym(string,string) returns boolean location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Contains_Symmetric";
create function if not exists default.nt_id(string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="nt_id";
create function if not exists default.variant_hash(string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="variant_hash";
create function if not exists default.complete_date(string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Complete_String_Date";
create function if not exists default.md5(string ...) returns string location "/udx/ncird_id/prod/libudfbioutils.so" symbol="md5";
create function if not exists default.longest_deletion(string) returns int location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Longest_Deletion";
create function if not exists default.deletion_events(string) returns int location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Number_Deletions";
create function if not exists default.nt_std(string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="nt_std";
create function if not exists default.aa_std(string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="aa_std";
create function if not exists default.pcd_list(string,string) returns string location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Physiochemical_Distance_List";
create function if not exists default.pcd(string,string) returns double location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Physiochemical_Distance";

create function if not exists default.to_epiweek(string,boolean) returns int location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Convert_String_To_EPI_Week";
create function if not exists default.to_epiweek(string) returns int location "/udx/ncird_id/prod/libudfbioutils.so" SYMBOL="Convert_String_To_EPI_Week";


CREATE AGGREGATE FUNCTION IF NOT EXISTS udx.logfold_agreement(bigint) 
RETURNS double INTERMEDIATE string
LOCATION "/udx/ncird_id/prod/libudabioutils.so"
    INIT_FN = "BoundedArrayInit"
    UPDATE_FN = "BoundedArrayUpdate"
    MERGE_FN = "BoundedArrayMerge"
    SERIALIZE_FN = "StringStructSerialize"
    FINALIZE_FN = "AgreementFinalize";


CREATE AGGREGATE FUNCTION IF NOT EXISTS udx.bitwise_sum(bigint) 
RETURNS bigint
LOCATION "/udx/ncird_id/prod/libudabioutils.so"
    INIT_FN = "BitwiseOrInit"
    UPDATE_FN = "BitwiseOrUpdateMerge"
    MERGE_FN = "BitwiseOrUpdateMerge"
    FINALIZE_FN = "BitwiseOrFinalize";
