create table if not exists udx.udf__variant_hash__ss (arg1_residues string, outcome string);
insert overwrite udx.udf__variant_hash__ss (arg1_residues) values (""),(NULL),("MNTQILVFALVASIPTNA"),("MNTQILVFALVASIPTNA :.-"),("..MNTQIL---VFA  LVASIPTNA:"),("MNTQILVFALVASIPTNA~"),("mntqilvfalvasiptna"),("1"),("STARK");
insert overwrite udx.udf__variant_hash__ss select arg1_residues,udx.variant_hash(arg1_residues) from udx.udf__variant_hash__ss;
