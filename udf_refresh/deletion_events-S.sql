drop function if exists udx.deletion_events(string);
create function udx.deletion_events(string) returns int location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Number_Deletions";

