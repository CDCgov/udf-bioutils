drop function if exists udx.sort_list(string, string);
create function udx.sort_list(string, string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Sort_List_By_Substring";
