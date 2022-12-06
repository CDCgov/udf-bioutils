create table if not exists udx.udf__hamming_distance__sssi (
    arg1_seq string,
    arg2_seq string,
    arg3_pairwise_deletion_set string,
    outcome int
);
insert overwrite udx.udf__hamming_distance__sssi (arg1_seq, arg2_seq, arg3_pairwise_deletion_set)
values ("ATGAGGCAG", "ATcAGGCrG", "-"),
    (NULL, "ATcAGGCrG", "-"),
    ("ATGAGGCAG", NULL, "-"),
    ("", "ATcAGGCrG", "-"),
    ("ATGAGGCAG", "", "-"),
    ("ATGAGGCAG", "ATcAGGCrGnnn", "-"),
    ("AGCTN", "AGCT.", "-"),
    ("AGCTN", "AGCT-", "-"),
    ("AGCTNNN", "AGCT.-~", "-"),
    ("AGCTNNN", "AGCT.-~", "-.~"),
    ("AGC", "AGX", "");
insert overwrite udx.udf__hamming_distance__sssi
select arg1_seq,
    arg2_seq,
    arg3_pairwise_deletion_set,
    udx.hamming_distance(arg1_seq, arg2_seq, arg3_pairwise_deletion_set)
from udx.udf__hamming_distance__sssi;