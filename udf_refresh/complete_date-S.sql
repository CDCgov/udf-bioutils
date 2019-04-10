drop function if exists udx.complete_date(string);
create function if not exists udx.complete_date(string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Complete_String_Date";
