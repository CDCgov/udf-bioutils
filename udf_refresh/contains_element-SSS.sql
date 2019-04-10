drop function if exists udx.contains_element(string,string,string);
create function udx.contains_element(string,string,string) returns boolean location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Contains_An_Element";

