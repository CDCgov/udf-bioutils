drop function if exists udx.to_aa(string);
create function udx.to_aa(string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="To_AA";

