create table if not exists udx.udf__sort_list_set__ssss (
    arg1_list string,
    arg2_delim_set string,
    arg3_output_delim string,
    outcome string
);
insert overwrite udx.udf__sort_list_set__ssss
values ("B;C;A", ";", ":", ""),
    (NULL, ";", ":", ""),
    ("B;C;A", NULL, ":", ""),
    ("B;C;A", ";", NULL, ""),
    ("B;C;A", ";", "", ""),
    ("B;C;A", "", ":", NULL),
    ("BstarkCstarkA", "stark", ":", NULL),
    ("Ok Bye;Hello, yes!", ";,", ":", NULL),
    ("Ok Bye;Hello, yes!", ";", ":", NULL);
insert overwrite udx.udf__sort_list_set__ssss
select arg1_list,
    arg2_delim_set,
    arg3_output_delim,
    udx.sort_list_set(arg1_list, arg2_delim_set, arg3_output_delim)
from udx.udf__sort_list_set__ssss;