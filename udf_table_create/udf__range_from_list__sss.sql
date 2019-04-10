create table if not exists udx.udf__range_from_list__sss (arg1_list string, arg2_delim string, outcome string);
insert overwrite udx.udf__range_from_list__sss (arg1_list, arg2_delim) values ("1;2;3;5;9;10;11;12",";"),("1;2;3;5;9;10;11;12", NULL),(NULL,";"),
("1;2;3;5;9;10;11;12",""),("",","),("1,1,1,1,2,3,4,5,6",","),("1,2,Stark!",","),("1,2;3;4",",;"),("1stark2stark3stark5","stark");
insert overwrite udx.udf__range_from_list__sss select arg1_list,arg2_delim, udx.range_from_list(arg1_list,arg2_delim) from udx.udf__range_from_list__sss;
