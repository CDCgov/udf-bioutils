drop function if exists udx.sort_list(string, string);
create function udx.sort_list(string, string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Sort_List_By_Substring";

drop function if exists udx.sort_list_set(string, string, string);
create function udx.sort_list_set(string, string, string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Sort_List_By_Set";

drop function if exists udx.sort_alleles(string,string);
create function udx.sort_alleles(string, string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Sort_Allele_List";

drop function if exists udx.to_aa(string);
create function udx.to_aa(string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="To_AA";

drop function if exists udx.to_aa(string,string,int);
create function udx.to_aa(string,string,int) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="To_AA_Mutant";

drop function if exists udx.reverse_complement(string);
create function udx.reverse_complement(string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Rev_Complement";
