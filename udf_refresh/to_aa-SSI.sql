drop function if exists udx.to_aa(string,string,int);
create function udx.to_aa(string,string,int) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="To_AA_Mutant";

