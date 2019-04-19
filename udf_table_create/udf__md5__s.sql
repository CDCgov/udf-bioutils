create table if not exists udx.udf__md5__s (arg1 string, arg2 string, arg3 string, args string, outcome string);
insert overwrite udx.udf__md5__s (arg1,arg2,arg3) values ("Sam","Stark","Sujatha"),("Sam","Ironman","Sujatha"),("SSS","Stark","Sujatha"),("Sam","Stark","Seenu"),("","Stark","Sujatha"),("Sam","","Sujatha"),("Sam","Stark",""),("","",""),(NULL,"Stark","Sujatha"),("Sam",NULL,"Sujatha"),("Sam","Stark",NULL),(NULL,NULL,NULL);
insert overwrite udx.udf__md5__s select arg1,arg2,arg3,"1,2,3",udx.md5(arg1,arg2,arg3) from udx.udf__md5__s;
insert INTO udx.udf__md5__s select arg1,arg2,arg3,"1,2",udx.md5(arg1,arg2) from udx.udf__md5__s;
insert INTO udx.udf__md5__s select arg1,arg2,arg3,"1",udx.md5(arg1) from udx.udf__md5__s;
