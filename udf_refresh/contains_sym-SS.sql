drop function if exists udx.contains_sym(string,string);
create function udx.contains_sym(string,string) returns boolean location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Contains_Symmetric";

