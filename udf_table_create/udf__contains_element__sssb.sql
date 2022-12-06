create table if not exists udx.udf__contains_element__sssb (
    arg1_string string,
    arg2_list string,
    arg3_delim string,
    outcome boolean
);
insert overwrite udx.udf__contains_element__sssb (arg1_string, arg2_list, arg3_delim)
values ("baby whales", "BABY;WHALES;FISH", ";"),
    ("baby whales", "baby;whales;fish", ";"),
    (NULL, "whales;baby", ";"),
    ("baby whales", NULL, ";"),
    ("baby whales", "whales;baby", NULL),
    ("", "whales;baby", ";"),
    ("baby whales", "", ";"),
    ("baby whales", "xz", ""),
    ("baby whales", "xyz", ""),
    ("baby whales", "xYz", ""),
    ("300028908", "28907::28906::28905", "::"),
    ("300028908", "28908::28907::28906::28905", "::");
insert overwrite udx.udf__contains_element__sssb
select arg1_string,
    arg2_list,
    arg3_delim,
    udx.contains_element(arg1_string, arg2_list, arg3_delim)
from udx.udf__contains_element__sssb;