create function if not exists udx.sort_list(string, string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Sort_List_By_Substring";
create function if not exists udx.sort_list_set(string, string, string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Sort_List_By_Set";
create function if not exists udx.sort_alleles(string, string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Sort_Allele_List";
create function if not exists udx.to_aa(string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="To_AA";
create function if not exists udx.to_aa(string,string,int) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="To_AA_Mutant";
create function if not exists udx.reverse_complement(string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Rev_Complement";
create function if not exists udx.substr_range(string,string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Substring_By_Range";
create function if not exists udx.mutation_list_strict(string,string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Mutation_List_Strict";
create function if not exists udx.mutation_list(string,string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Mutation_List_No_Ambiguous";
create function if not exists udx.hamming_distance(string,string) returns int location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Hamming_Distance";
create function if not exists udx.hamming_distance(string,string,string) returns int location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Hamming_Distance_Pairwise_Delete";

create function if not exists default.sort_list(string, string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Sort_List_By_Substring";
create function if not exists default.sort_list_set(string, string, string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Sort_List_By_Set";
create function if not exists default.sort_alleles(string, string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Sort_Allele_List";
create function if not exists default.to_aa(string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="To_AA";
create function if not exists default.to_aa(string,string,int) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="To_AA_Mutant";
create function if not exists default.reverse_complement(string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Rev_Complement";
create function if not exists default.substr_range(string,string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Substring_By_Range";
create function if not exists default.mutation_list_strict(string,string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Mutation_List_Strict";
create function if not exists default.mutation_list(string,string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Mutation_List_No_Ambiguous";
create function if not exists default.hamming_distance(string,string) returns int location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Hamming_Distance";
create function if not exists default.hamming_distance(string,string,string) returns int location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Hamming_Distance_Pairwise_Delete";
