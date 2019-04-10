drop function if exists udx.sort_alleles(string,string);
create function udx.sort_alleles(string, string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Sort_Allele_List";

