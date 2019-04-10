drop function if exists udx.sort_list_unique(string, string);
create function udx.sort_list_unique(string, string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Sort_List_By_Substring_Unique";

