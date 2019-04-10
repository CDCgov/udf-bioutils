drop function if exists udx.reverse_complement(string);
create function udx.reverse_complement(string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Rev_Complement";

