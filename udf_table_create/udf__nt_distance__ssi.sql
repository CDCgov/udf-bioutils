
create table if not exists udx.udf__nt_distance__ssi (arg1_seq string, arg2_seq string, outcome int);
insert overwrite udx.udf__nt_distance__ssi (arg1_seq,arg2_seq) values ("ATGAGGCAG","ATcAGGCrG"),(NULL,"ATcAGGCrG"),("ATGAGGCAG",NULL),("","ATcAGGCrG"),("ATGAGGCAG",""),("ATGAGGCAG","ATcAGGCrGnnn"),("AGCT.","AGCTN"),("AGCT-","AGCTN"),("NNNNNNNNNNNNNNNBBBBBBBDDDDDDDHHHHHHHVVVVVVRRYYYSSWWWKKKMM","acgturyswkmbdhvcgtuyskagturwkactuywmacgrsmagctucgatugtuac");
insert overwrite udx.udf__nt_distance__ssi select arg1_seq,arg2_seq,udx.nt_distance(arg1_seq,arg2_seq) from udx.udf__nt_distance__ssi;
