drop function if exists udx.hamming_distance(string,string);
create function udx.hamming_distance(string,string) returns int location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Hamming_Distance";

