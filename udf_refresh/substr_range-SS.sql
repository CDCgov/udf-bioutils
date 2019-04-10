drop function if exists udx.substr_range(string,string);
create function udx.substr_range(string,string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Substring_By_Range";

