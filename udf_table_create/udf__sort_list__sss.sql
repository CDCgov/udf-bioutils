create table if not exists udx.udf__sort_list__sss (arg1_list string, arg2_delim string, outcome string);
insert overwrite udx.udf__sort_list__sss values ( "B;C;A", ";", NULL),( "B;C;A", NULL,"OK"),(NULL,";","OK"),
("B;C;A","",NULL),("BstarkCstarkA","stark",NULL),("Ok Bye;Hello, yes!",";,",NULL),("Ok Bye;Hello, yes!",";",NULL),("A,B,A,C",",",NULL);
insert overwrite udx.udf__sort_list__sss select arg1_list,arg2_delim, udx.sort_list(arg1_list,arg2_delim) from udx.udf__sort_list__sss;
