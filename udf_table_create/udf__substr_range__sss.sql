create table if not exists udx.udf__substr_range__sss (arg1_string string, arg2_range_coords string, outcome string);
insert overwrite udx.udf__substr_range__sss (arg1_string,arg2_range_coords) values ("SammySheep","1..3"),(NULL,"1..3"),("SammySheep",NULL),("SammySheep",""),("","1..3"),("SammySheep","3..1;8..9;5"),("123456789","0..3"),("123456789","7..12"),("ABC456","1..2,5");
insert overwrite udx.udf__substr_range__sss select arg1_string,arg2_range_coords,udx.substr_range(arg1_string,arg2_range_coords) from udx.udf__substr_range__sss;
