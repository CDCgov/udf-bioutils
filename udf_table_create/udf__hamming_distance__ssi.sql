create table if not exists udx.udf__hamming_distance__ssi (arg1_seq string, arg2_seq string, outcome int);
insert overwrite udx.udf__hamming_distance__ssi (arg1_seq, arg2_seq)
values ("ATGAGGCAG", "ATcAGGCrG"),
    (NULL, "ATcAGGCrG"),
    ("ATGAGGCAG", NULL),
    ("", "ATcAGGCrG"),
    ("ATGAGGCAG", ""),
    ("ATGAGGCAG", "ATcAGGCrGnnn"),
    ("AGCT.", "AGCTN"),
    ("AGCT-", "AGCTN");
insert overwrite udx.udf__hamming_distance__ssi
select arg1_seq,
    arg2_seq,
    udx.hamming_distance(arg1_seq, arg2_seq)
from udx.udf__hamming_distance__ssi;