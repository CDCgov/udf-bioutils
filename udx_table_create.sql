create table if not exists udx.udf_sort_list (arg1_list string, arg2_delim string, outcome string);
insert overwrite udx.udf_sort_list values ( "B;C;A", ";", NULL),( "B;C;A", NULL,"OK"),(NULL,";","OK"),
("B;C;A","",NULL),("BstarkCstarkA","stark",NULL),("Ok Bye;Hello, yes!",";,",NULL),("Ok Bye;Hello, yes!",";",NULL);
insert overwrite udx.udf_sort_list select arg1_list,arg2_delim, udx.sort_list(arg1_list,arg2_delim) from udx.udf_sort_list;

create table if not exists udx.udf_sort_list_set (arg1_list string, arg2_delim_set string, arg3_output_delim string, outcome string);
insert overwrite udx.udf_sort_list_set values ( "B;C;A",";",":",""),(NULL,";",":",""),( "B;C;A",NULL,":",""),("B;C;A",";",NULL,""),("B;C;A",";","",""),
("B;C;A","",":",NULL),("BstarkCstarkA","stark",":",NULL),("Ok Bye;Hello, yes!",";,",":",NULL),("Ok Bye;Hello, yes!",";",":",NULL);
insert overwrite udx.udf_sort_list_set select arg1_list,arg2_delim_set,arg3_output_delim, udx.sort_list_set(arg1_list,arg2_delim_set,arg3_output_delim) from udx.udf_sort_list_set;
