create table if not exists udx.udf__to_aa__ssis (
    arg1_nucleotides string,
    arg2_replacement string,
    arg3_start_position int,
    outcome string
);
insert overwrite udx.udf__to_aa__ssis (
        arg1_nucleotides,
        arg2_replacement,
        arg3_start_position
    )
values ("ATGAGG---GGGTGGTAG", "G", 1),
    ("", "G", 1),
    ("ATG", "", 1),
    (NULL, "G", 1),
    ("ATG", NULL, 1),
    ("ATG", "G", NULL),
    ("ATGaggCC", "ATG", 4),
    ("...ATG.-~GGG", "atgggg", 1),
    ("ATGcagAGG", "GGG", 4),
    ("ATGcagAGG", "GGG", 0),
    ("ATGcagAGG", "GGG", 10),
    ("ATG", "ggATG", 2);
insert overwrite udx.udf__to_aa__ssis
select arg1_nucleotides,
    arg2_replacement,
    arg3_start_position,
    udx.to_aa(
        arg1_nucleotides,
        arg2_replacement,
        arg3_start_position
    )
from udx.udf__to_aa__ssis;