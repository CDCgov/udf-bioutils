drop function if exists udx.longest_deletion(string);
create function udx.longest_deletion(string) returns int location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Longest_Deletion";

