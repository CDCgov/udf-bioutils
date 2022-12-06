create table if not exists udx.udf__reverse_complement__ss (arg1_nucleotides string, outcome string);
insert overwrite udx.udf__reverse_complement__ss (arg1_nucleotides)
values ("ATGAGG---GGGTGGTAG"),
    ("ctaccaccc---cctcat"),
    (""),
    (NULL),
    ("gcatrykmbvdhuGCATRYKMBVDHU");
insert overwrite udx.udf__reverse_complement__ss
select arg1_nucleotides,
    udx.reverse_complement(arg1_nucleotides)
from udx.udf__reverse_complement__ss;