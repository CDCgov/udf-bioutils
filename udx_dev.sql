
--drop function if exists udx.dev_mutation_list(string,string);
--create function if not exists udx.dev_mutation_list(string,string) returns string location "/udx/ncird_id/dev/libudfbioutils.so" SYMBOL="Mutation_List_Strict";


--drop function if exists udx.dev_mutation_list_pds(string,string,string);
--create function if not exists udx.dev_mutation_list_pds(string,string,string) returns string location "/udx/ncird_id/dev/libudfbioutils.so" SYMBOL="Mutation_List_PDS";


drop function if exists udx.dev_complete_date;
create function if not exists udx.dev_complete_date(string) returns string location "/udx/ncird_id/dev/libudfbioutils.so" SYMBOL="Complete_String_Date";


--drop function if exists udx.dev_nt_distance(string,string);
--reate function if not exists udx.dev_nt_distance(string,string) returns int location "/udx/ncird_id/dev/libudfbioutils.so" SYMBOL="Nt_Distance";
--drop function if exists udx.dev_mutation_list_nt(string,string);
--create function if not exists udx.dev_mutation_list_nt(string,string) returns string location "/udx/ncird_id/dev/libudfbioutils.so" SYMBOL="Mutation_List_No_Ambiguous";

--drop function if exists udx.dev_substr(string,string);
--create function if not exists udx.dev_substr_range(string,string) returns string location "/udx/ncird_id/dev/libudfbioutils.so" SYMBOL="Substring_By_Range";

--drop function if exists udx.dev_reverse_complement(string);
--create function if not exists udx.dev_reverse_complement(string) returns string location "/udx/ncird_id/dev/libudfbioutils.so" SYMBOL="Rev_Complement";


--drop function if exists udx.dev_md5(string ...);
--create function if not exists udx.dev_md5(string ...) returns string location "/udx/ncird_id/dev/libudfbioutils.so" symbol="md5";

--drop function if exists udx.dev_pcd(string,string);
--create function if not exists udx.dev_pcd(string,string) returns double location "/udx/ncird_id/dev/libudfbioutils.so" SYMBOL="Physiochemical_Distance";


--drop function if exists udx.dev_sort_list(string, string);
--create function if not exists udx.dev_sort_list(string, string) returns string location "/udx/ncird_id/dev/libudfbioutils.so" SYMBOL="Sort_List_By_Substring";
