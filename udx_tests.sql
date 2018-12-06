select "sort_list" as udf, sum( outcome IS NOT DISTINCT FROM udx.sort_list(arg1_list,arg2_delim)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.sort_list(arg1_list,arg2_delim)) / count(*) * 100 as percent_correct from udx.udf_sort_list;
select "sort_list_unique" as udf, sum( outcome IS NOT DISTINCT FROM udx.sort_list_unique(arg1_list,arg2_delim)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.sort_list_unique(arg1_list,arg2_delim)) / count(*) * 100 as percent_correct from udx.udf_sort_list_unique;
select "sort_list_set" as udf, sum( outcome IS NOT DISTINCT FROM udx.sort_list_set(arg1_list,arg2_delim_set,arg3_output_delim)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.sort_list_set(arg1_list,arg2_delim_set,arg3_output_delim)) / count(*) * 100 as percent_correct from udx.udf_sort_list_set;
select "sort_alleles" as udf, sum( outcome IS NOT DISTINCT FROM udx.sort_alleles(arg1_alleles,arg2_delim)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.sort_alleles(arg1_alleles,arg2_delim)) / count(*) * 100 as percent_correct from udx.udf_sort_alleles;
select "to_aa (s)" as udf, sum(outcome IS NOT DISTINCT FROM udx.to_aa(arg1_nucleotides)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.to_aa(arg1_nucleotides)) / count(*) * 100 as percent_correct from udx.udf_to_aa_1;
select "to_aa (s,s,i)" as udf, sum( outcome IS NOT DISTINCT FROM udx.to_aa(arg1_nucleotides,arg2_replacement,arg3_start_position)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.to_aa(arg1_nucleotides,arg2_replacement,arg3_start_position)) / count(*) * 100 as percent_correct from udx.udf_to_aa_2;
select "reverse_complement" as udf, sum( outcome IS NOT DISTINCT FROM udx.reverse_complement(arg1_nucleotides)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.reverse_complement(arg1_nucleotides)) / count(*) * 100 as percent_correct from udx.udf_reverse_complement;
select "substr_range" as udf, sum( outcome IS NOT DISTINCT FROM udx.substr_range(arg1_string,arg2_range_coords)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.substr_range(arg1_string,arg2_range_coords)) / count(*) * 100 as percent_correct from udx.udf_substr_range;
select "mutation_list" as udf, sum( outcome IS NOT DISTINCT FROM udx.mutation_list(arg1_seq,arg2_seq)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.mutation_list(arg1_seq,arg2_seq)) / count(*) * 100 as percent_correct from udx.udf_mutation_list;
select "mutation_list_nt" as udf, sum( outcome IS NOT DISTINCT FROM udx.mutation_list_nt(arg1_seq,arg2_seq)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.mutation_list_nt(arg1_seq,arg2_seq)) / count(*) * 100 as percent_correct from udx.udf_mutation_list_nt;
select "hamming_distance (s,s)" as udf, sum( outcome IS NOT DISTINCT FROM udx.hamming_distance(arg1_seq,arg2_seq)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.hamming_distance(arg1_seq,arg2_seq)) / count(*) * 100 as percent_correct from udx.udf_hamming_distance_1;
select "hamming_distance (s,s,s)" as udf, sum( outcome IS NOT DISTINCT FROM udx.hamming_distance(arg1_seq,arg2_seq,arg3_pairwise_deletion_set)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.hamming_distance(arg1_seq,arg2_seq,arg3_pairwise_deletion_set)) / count(*) * 100 as percent_correct from udx.udf_hamming_distance_2;
select "contains_element" as udf, sum( outcome IS NOT DISTINCT FROM udx.contains_element(arg1_string,arg2_list,arg3_delim)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.contains_element(arg1_string,arg2_list,arg3_delim)) / count(*) * 100 as percent_correct from udx.udf_contains_element;
select "is_element" as udf, sum( outcome IS NOT DISTINCT FROM udx.is_element(arg1_string,arg2_list,arg3_delim)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.is_element(arg1_string,arg2_list,arg3_delim)) / count(*) * 100 as percent_correct from udx.udf_is_element;
select "contains_sym" as udf, sum( outcome IS NOT DISTINCT FROM udx.contains_sym(arg1_string,arg2_string)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.contains_sym(arg1_string,arg2_string)) / count(*) * 100 as percent_correct from udx.udf_contains_sym;
