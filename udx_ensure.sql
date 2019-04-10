create function if not exists udx.sort_list(string, string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Sort_List_By_Substring";
create function if not exists udx.sort_list_unique(string, string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Sort_List_By_Substring_Unique";
create function if not exists udx.sort_list_set(string, string, string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Sort_List_By_Set";
create function if not exists udx.sort_alleles(string, string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Sort_Allele_List";
create function if not exists udx.to_aa(string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="To_AA";
create function if not exists udx.to_aa(string,string,int) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="To_AA_Mutant";
create function if not exists udx.reverse_complement(string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Rev_Complement";
create function if not exists udx.substr_range(string,string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Substring_By_Range";
create function if not exists udx.range_from_list(string,string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Range_From_List";
create function if not exists udx.mutation_list(string,string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Mutation_List_Strict";
create function if not exists udx.mutation_list(string,string,string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Mutation_List_Strict";
create function if not exists udx.mutation_list_nt(string,string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Mutation_List_No_Ambiguous";
create function if not exists udx.hamming_distance(string,string) returns int location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Hamming_Distance";
create function if not exists udx.hamming_distance(string,string,string) returns int location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Hamming_Distance_Pairwise_Delete";
create function if not exists udx.contains_element(string,string,string) returns boolean location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Contains_An_Element";
create function if not exists udx.is_element(string,string,string) returns boolean location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Is_An_Element";
create function if not exists udx.contains_sym(string,string) returns boolean location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Contains_Symmetric";
create function if not exists udx.nt_id(string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="nt_id";
create function if not exists udx.variant_hash(string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="variant_hash";
create function if not exists udx.complete_date(string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Complete_String_Date";

create function if not exists default.sort_list(string, string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Sort_List_By_Substring";
create function if not exists default.sort_list_unique(string, string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Sort_List_By_Substring_Unique";
create function if not exists default.sort_list_set(string, string, string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Sort_List_By_Set";
create function if not exists default.sort_alleles(string, string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Sort_Allele_List";
create function if not exists default.to_aa(string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="To_AA";
create function if not exists default.to_aa(string,string,int) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="To_AA_Mutant";
create function if not exists default.reverse_complement(string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Rev_Complement";
create function if not exists default.substr_range(string,string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Substring_By_Range";
create function if not exists default.range_from_list(string,string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Range_From_List";
create function if not exists default.mutation_list(string,string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Mutation_List_Strict";
create function if not exists default.mutation_list(string,string,string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Mutation_List_Strict";
create function if not exists default.mutation_list_nt(string,string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Mutation_List_No_Ambiguous";
create function if not exists default.hamming_distance(string,string) returns int location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Hamming_Distance";
create function if not exists default.hamming_distance(string,string,string) returns int location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Hamming_Distance_Pairwise_Delete";
create function if not exists default.contains_element(string,string,string) returns boolean location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Contains_An_Element";
create function if not exists default.is_element(string,string,string) returns boolean location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Is_An_Element";
create function if not exists default.contains_sym(string,string) returns boolean location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Contains_Symmetric";
create function if not exists default.nt_id(string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="nt_id";
create function if not exists default.variant_hash(string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="variant_hash";
create function if not exists default.complete_date(string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Complete_String_Date";
