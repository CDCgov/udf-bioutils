create table if not exists udx.udf__longest_deletion__si (arg1_sequence string, outcome int);
insert overwrite udx.udf__longest_deletion__si (arg1_sequence) values ("ATG---AGG---GGG--TAG"),(""),(NULL),("...ATG----TAG..."),("ATG---..."),("ATG------AGG---gac"),("Stark!");
insert overwrite udx.udf__longest_deletion__si select arg1_sequence,udx.longest_deletion(arg1_sequence) from udx.udf__longest_deletion__si;
