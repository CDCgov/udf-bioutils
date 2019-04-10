create table if not exists udx.udf__contains_sym__ssb (arg1_string string, arg2_string string, outcome boolean);
insert overwrite udx.udf__contains_sym__ssb (arg1_string,arg2_string) values ("sam","samuel"),("samuel","sam"),(NULL,"sam"),("sam",NULL),("","sam"),("sam",""),("",""),("sam","sam"),("SAM","samuel");
insert overwrite udx.udf__contains_sym__ssb select arg1_string,arg2_string,udx.contains_sym(arg1_string,arg2_string) from udx.udf__contains_sym__ssb;
