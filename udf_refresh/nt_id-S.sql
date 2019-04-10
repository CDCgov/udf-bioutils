drop function if exists udx.nt_id(string);
create function udx.nt_id(string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="nt_id";

