drop function if exists udx.range_from_list(string,string);
create function if not exists udx.range_from_list(string,string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Range_From_List";
