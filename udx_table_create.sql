create table if not exists udx.udf_sort_list (arg1_list string, arg2_delim string, outcome string);
insert overwrite udx.udf_sort_list values ( "B;C;A", ";", NULL),( "B;C;A", NULL,"OK"),(NULL,";","OK"),
("B;C;A","",NULL),("BstarkCstarkA","stark",NULL),("Ok Bye;Hello, yes!",";,",NULL),("Ok Bye;Hello, yes!",";",NULL);
insert overwrite udx.udf_sort_list select arg1_list,arg2_delim, udx.sort_list(arg1_list,arg2_delim) from udx.udf_sort_list;

create table if not exists udx.udf_sort_list_set (arg1_list string, arg2_delim_set string, arg3_output_delim string, outcome string);
insert overwrite udx.udf_sort_list_set values ( "B;C;A",";",":",""),(NULL,";",":",""),( "B;C;A",NULL,":",""),("B;C;A",";",NULL,""),("B;C;A",";","",""),
("B;C;A","",":",NULL),("BstarkCstarkA","stark",":",NULL),("Ok Bye;Hello, yes!",";,",":",NULL),("Ok Bye;Hello, yes!",";",":",NULL);
insert overwrite udx.udf_sort_list_set select arg1_list,arg2_delim_set,arg3_output_delim, udx.sort_list_set(arg1_list,arg2_delim_set,arg3_output_delim) from udx.udf_sort_list_set;

create table if not exists udx.udf_sort_alleles (arg1_alleles string, arg2_delim string, outcome string);
insert overwrite udx.udf_sort_alleles values ( "89G, 56Y, 2S, 160T", ", ", NULL),(NULL,", ",""),( "89G, 56Y, 2S, 160T", NULL,""),("A160T;S53G;M140R","",NULL),("",";",NULL),
("A160T;S53G;M140R",";",NULL),("A1starkC3starkA2","stark",NULL),("89G, 56Y, 2S, 160T","#",NULL),("A,C,B",",",NULL);
insert overwrite udx.udf_sort_alleles select arg1_alleles,arg2_delim, udx.sort_alleles(arg1_alleles,arg2_delim) from udx.udf_sort_alleles;

create table if not exists udx.udf_to_aa_1 (arg1_nucleotides string, outcome string);
insert overwrite udx.udf_to_aa_1 (arg1_nucleotides) values ("ATGAGG---GGGTGGTAG"),(""),(NULL),("ATGaggCC"),("...ATG.-~GGG"),("AGGaagARG---GCGgcwGCRgcnzzz"),("..ATG..");
insert overwrite udx.udf_to_aa_1 select arg1_nucleotides,udx.to_aa(arg1_nucleotides) from udx.udf_to_aa_1;

create table if not exists udx.udf_to_aa_2 (arg1_nucleotides string, arg2_replacement string, arg3_start_position int, outcome string);
insert overwrite udx.udf_to_aa_2 (arg1_nucleotides,arg2_replacement,arg3_start_position) values ("ATGAGG---GGGTGGTAG","G",1),("","",1),(NULL,NULL,1),("ATGaggCC","ATG",4),("...ATG.-~GGG","atgggg",1),("ATGcagAGG","GGG",4),("ATGcagAGG","GGG",0),("ATGcagAGG","GGG",10),("ATG","ggATG",2);
insert overwrite udx.udf_to_aa_2 select arg1_nucleotides,arg2_replacement,arg3_start_position,udx.to_aa(arg1_nucleotides,arg2_replacement,arg3_start_position) from udx.udf_to_aa_2;

create table if not exists udx.udf_reverse_complement (arg1_nucleotides string, outcome string);
insert overwrite udx.udf_reverse_complement (arg1_nucleotides) values ("ATGAGG---GGGTGGTAG"),("ctaccaccc---cctcat"),(""),(NULL),("gcatrykmbvdhuGCATRYKMBVDHU");
insert overwrite udx.udf_reverse_complement select arg1_nucleotides,udx.reverse_complement(arg1_nucleotides) from udx.udf_reverse_complement;

create table if not exists udx.udf_substr_range (arg1_string string, arg2_range_coords string, outcome string);
insert overwrite udx.udf_substr_range (arg1_string,arg2_range_coords) values ("SammySheep","1..3"),(NULL,"1..3"),("SammySheep",NULL),("SammySheep",""),("","1..3"),("SammySheep","3..1;8..9;5"),("123456789","0..3"),("123456789","7..12"),("ABC456","1..2,5");
insert overwrite udx.udf_substr_range select arg1_string,arg2_range_coords,udx.substr_range(arg1_string,arg2_range_coords) from udx.udf_substr_range;

create table if not exists udx.udf_mutation_list_strict (arg1_seq string, arg2_seq string, outcome string);
insert overwrite udx.udf_mutation_list_strict (arg1_seq,arg2_seq) values ("ATGAGGCAG","ATcAGGCrG"),(NULL,"ATcAGGCrG"),("ATGAGGCAG",NULL),("","ATcAGGCrG"),("ATGAGGCAG",""),("ATGAGGCAG","ATcAGGCrGnnn");
insert overwrite udx.udf_mutation_list_strict select arg1_seq,arg2_seq,udx.mutation_list_strict(arg1_seq,arg2_seq) from udx.udf_mutation_list_strict;

create table if not exists udx.udf_mutation_list (arg1_seq string, arg2_seq string, outcome string);
insert overwrite udx.udf_mutation_list (arg1_seq,arg2_seq) values ("ATGAGGCAG","ATcAGGCrG"),(NULL,"ATcAGGCrG"),("ATGAGGCAG",NULL),("","ATcAGGCrG"),("ATGAGGCAG",""),("ATGAGGCAG","ATcAGGCrGnnn");
insert overwrite udx.udf_mutation_list select arg1_seq,arg2_seq,udx.mutation_list(arg1_seq,arg2_seq) from udx.udf_mutation_list;
