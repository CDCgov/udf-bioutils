drop function if exists udx.nt_distance(string,string);
create function udx.nt_distance(string,string) returns int location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Nt_Distance";

