select "sort_list" as udf, sum(coalesce(outcome,"NULL") = coalesce(udx.sort_list(arg1_list,arg2_delim),"NULL")) as correct, count(*) as total_tests, sum(coalesce(outcome,"NULL") = coalesce(udx.sort_list(arg1_list,arg2_delim),"NULL")) / count(*) * 100 as percent_correct from udx.udf_sort_list;
select "sort_list_set" as udf, sum(coalesce(outcome,"NULL") = coalesce(udx.sort_list_set(arg1_list,arg2_delim_set,arg3_output_delim),"NULL")) as correct, count(*) as total_tests, sum(coalesce(outcome,"NULL") = coalesce(udx.sort_list_set(arg1_list,arg2_delim_set,arg3_output_delim),"NULL")) / count(*) * 100 as percent_correct from udx.udf_sort_list_set;
select "sort_alleles" as udf, sum(coalesce(outcome,"NULL") = coalesce(udx.sort_alleles(arg1_alleles,arg2_delim),"NULL")) as correct, count(*) as total_tests, sum(coalesce(outcome,"NULL") = coalesce(udx.sort_alleles(arg1_alleles,arg2_delim),"NULL")) / count(*) * 100 as percent_correct from udx.udf_sort_alleles;
select "to_aa (s)" as udf, sum(coalesce(outcome,"NULL") = coalesce(udx.to_aa(arg1_nucleotides),"NULL")) as correct, count(*) as total_tests, sum(coalesce(outcome,"NULL") = coalesce(udx.to_aa(arg1_nucleotides),"NULL")) / count(*) * 100 as percent_correct from udx.udf_to_aa_1;
select "to_aa (s,s,i)" as udf, sum(coalesce(outcome,"NULL") = coalesce(udx.to_aa(arg1_nucleotides,arg2_replacement,arg3_start_position),"NULL")) as correct, count(*) as total_tests, sum(coalesce(outcome,"NULL") = coalesce(udx.to_aa(arg1_nucleotides,arg2_replacement,arg3_start_position),"NULL")) / count(*) * 100 as percent_correct from udx.udf_to_aa_2;
select "reverse_complement" as udf, sum(coalesce(outcome,"NULL") = coalesce(udx.reverse_complement(arg1_nucleotides),"NULL")) as correct, count(*) as total_tests, sum(coalesce(outcome,"NULL") = coalesce(udx.reverse_complement(arg1_nucleotides),"NULL")) / count(*) * 100 as percent_correct from udx.udf_reverse_complement;
select "substr_range" as udf, sum(coalesce(outcome,"NULL") = coalesce(udx.substr_range(arg1_string,arg2_range_coords),"NULL")) as correct, count(*) as total_tests, sum(coalesce(outcome,"NULL") = coalesce(udx.substr_range(arg1_string,arg2_range_coords),"NULL")) / count(*) * 100 as percent_correct from udx.udf_substr_range;
select "mutation_list_strict" as udf, sum(coalesce(outcome,"NULL") = coalesce(udx.mutation_list_strict(arg1_seq,arg2_seq),"NULL")) as correct, count(*) as total_tests, sum(coalesce(outcome,"NULL") = coalesce(udx.mutation_list_strict(arg1_seq,arg2_seq),"NULL")) / count(*) * 100 as percent_correct from udx.udf_mutation_list_strict;
select "mutation_list" as udf, sum(coalesce(outcome,"NULL") = coalesce(udx.mutation_list(arg1_seq,arg2_seq),"NULL")) as correct, count(*) as total_tests, sum(coalesce(outcome,"NULL") = coalesce(udx.mutation_list(arg1_seq,arg2_seq),"NULL")) / count(*) * 100 as percent_correct from udx.udf_mutation_list;
