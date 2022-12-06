create table if not exists udx.udf__to_aa__ss (arg1_nucleotides string, outcome string);
insert overwrite udx.udf__to_aa__ss (arg1_nucleotides)
values ("ATGAGG---GGGTGGTAG"),
    (""),
    (NULL),
    ("ATGaggCC"),
    ("...ATG.-~GGG"),
    ("AGGaagARG---GCGgcwGCRgcnzzz"),
    ("..ATG..");
insert overwrite udx.udf__to_aa__ss
select arg1_nucleotides,
    udx.to_aa(arg1_nucleotides)
from udx.udf__to_aa__ss;