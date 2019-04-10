drop function if exists udx.is_element(string,string,string);
create function udx.is_element(string,string,string) returns boolean location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Is_An_Element";

