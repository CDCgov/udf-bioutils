create table if not exists udx.udf__mutation_list__ssss (
    arg1_seq string,
    arg2_seq string,
    arg3_range string,
    outcome string
);
insert overwrite udx.udf__mutation_list__ssss (arg1_seq, arg2_seq, arg3_range)
values ("ATGAGGCAG", "ATcAGGCrG", "1..4"),
    (NULL, "ATcAGGCrG", "1..4"),
    ("ATGAGGCAG", NULL, "1..4"),
    ("ATGAGGCAG", "ATcAGGCrG", NULL),
    ("", "ATcAGGCrG", "1..4"),
    ("ATGAGGCAG", "", "1..4"),
    ("ATGAGGCAG", "ATcAGGCrG", ""),
    ("ATGAGGCAG", "ATcAGGCrGnnn", "1..4"),
    ("ATGAGGCAG", "ATcAGGCrG", "1..9"),
    ("ATGAGGCAG", "ATcAGGCrG", "1..9,3,8"),
    ("ATGAGGCAG", "ATcAGGCrG", "8,1..4"),
    ("ATGAGGCAG", "ATcAGGCrG", "1..4,Stark");
insert overwrite udx.udf__mutation_list__ssss
select arg1_seq,
    arg2_seq,
    arg3_range,
    udx.mutation_list(arg1_seq, arg2_seq, arg3_range)
from udx.udf__mutation_list__ssss;