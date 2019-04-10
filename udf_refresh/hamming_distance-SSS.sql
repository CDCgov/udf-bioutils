drop function if exists udx.hamming_distance(string,string,string);
create function udx.hamming_distance(string,string,string) returns int location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Hamming_Distance_Pairwise_Delete";

