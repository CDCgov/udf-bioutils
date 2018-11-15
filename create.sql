create table if not exists udx.udf_sort_list (arg1_list string, arg2_delim string, outcome string);
insert overwrite udx.udf_sort_list values ( "B;C;A", ";", NULL)
insert overwrite udx.udf_sort_list select arg1_list,arg2_delim, sort_list(arg1_list,arg2_delim) from udf_sort_list;
