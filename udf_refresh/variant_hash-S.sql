drop function if exists udx.variant_hash(string);
create function udx.variant_hash(string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="variant_hash";
