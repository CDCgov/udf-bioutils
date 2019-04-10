create table if not exists udx.udf__sort_alleles__sss (arg1_alleles string, arg2_delim string, outcome string);
insert overwrite udx.udf__sort_alleles__sss values ( "89G, 56Y, 2S, 160T", ", ", NULL),(NULL,", ",""),( "89G, 56Y, 2S, 160T", NULL,""),("A160T;S53G;M140R","",NULL),("",";",NULL),
("A160T;S53G;M140R",";",NULL),("A1starkC3starkA2","stark",NULL),("89G, 56Y, 2S, 160T","#",NULL),("A,C,B",",",NULL);
insert overwrite udx.udf__sort_alleles__sss select arg1_alleles,arg2_delim, udx.sort_alleles(arg1_alleles,arg2_delim) from udx.udf__sort_alleles__sss;
