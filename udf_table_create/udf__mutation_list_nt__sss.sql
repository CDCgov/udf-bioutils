create table if not exists udx.udf__mutation_list_nt__sss (arg1_seq string, arg2_seq string, outcome string);
insert overwrite udx.udf__mutation_list_nt__sss (arg1_seq,arg2_seq) values ("ATGAGGCAG","ATcAGGCrG"),(NULL,"ATcAGGCrG"),("ATGAGGCAG",NULL),("","ATcAGGCrG"),("ATGAGGCAG",""),("ATGAGGCAG","ATcAGGCrGnnn");
insert overwrite udx.udf__mutation_list_nt__sss select arg1_seq,arg2_seq,udx.mutation_list_nt(arg1_seq,arg2_seq) from udx.udf__mutation_list_nt__sss;
