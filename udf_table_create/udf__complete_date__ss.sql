create table if not exists udx.udf__complete_date__ss (arg1_date string, outcome string);
insert overwrite udx.udf__complete_date__ss (arg1_date)
values ("2019"),
    ("2019-03"),
    ("2019-03-15"),
    ("STARK"),
    (""),
    (NULL),
    ("2010.02"),
    ("1981.09.12"),
    ("2000/01"),
    ("1");
insert overwrite udx.udf__complete_date__ss
select arg1_date,
    udx.complete_date(arg1_date)
from udx.udf__complete_date__ss;