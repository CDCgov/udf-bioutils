create table if not exists udx.udf__mutation_list_gly__sss (arg1_seq string, arg2_seq string, outcome string);
insert overwrite udx.udf__mutation_list_gly__sss (arg1_seq,arg2_seq) values 
	("NRMANHSSELL","NRMANHSSELL"),
	(NULL,"NRMANHSSELL"),("NRMANHSSELL",NULL),
	("","NRMANHSSELL"),("NRMANHSSELL",""),
	("NRMANHSSELL","NRMAN"),
	("NRMANHSSELL","NRSANPSSELL"),
	("NRMANHSSELL","NXTANHSSNAT"),
	("NANHSSELL","NATHSSELL"),
	("AAA","NIT"),("APA","NIT"),
	("NAA","NIT"),("NPA","NIT"),
	("NAT","NIS"),("NPT","NIT"),
	("NAT","NAP"),("NSS","NSP"),
	("NPS","NNS"),("NNS","NPS");
insert overwrite udx.udf__mutation_list_gly__sss select arg1_seq,arg2_seq,udx.mutation_list_gly(arg1_seq,arg2_seq) from udx.udf__mutation_list_gly__sss;
