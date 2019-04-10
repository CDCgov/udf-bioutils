drop function if exists udx.mutation_list(string,string,string);
create function udx.mutation_list(string,string,string) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="Mutation_List_Strict";

