create table if not exists udx.udf__is_element__sssb (
    arg1_string string,
    arg2_list string,
    arg3_delim string,
    outcome boolean
);
insert overwrite udx.udf__is_element__sssb (arg1_string, arg2_list, arg3_delim)
values ("whales", "BABY;WHALES;FISH", ";"),
    ("whales", "baby;whales;fish", ";"),
    ("baby whales", "baby;whales;fish", ";"),
    (NULL, "whales;baby", ";"),
    ("whales", NULL, ";"),
    ("whales", "whales;baby", NULL),
    ("", "whales;baby", ";"),
    ("baby whales", "", ";"),
    ("y", "xz", ""),
    ("y", "xyz", ""),
    ("y", "xYz", ""),
    ("300028908", "28907::28906::28905", "::"),
    ("300028908", "28908::28907::28906::28905", "::"),
    (
        "300028908",
        "300028908::300028907::300028906::300028905",
        "::"
    );
insert overwrite udx.udf__is_element__sssb
select arg1_string,
    arg2_list,
    arg3_delim,
    udx.is_element(arg1_string, arg2_list, arg3_delim)
from udx.udf__is_element__sssb;