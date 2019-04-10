select "sort_list (s,s):s" as udf, sum( outcome IS NOT DISTINCT FROM udx.sort_list(arg1_list,arg2_delim)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.sort_list(arg1_list,arg2_delim)) / count(*) * 100 as percent_correct from udx.udf__sort_list__sss;

select "sort_list_unique (s,s):s" as udf, sum( outcome IS NOT DISTINCT FROM udx.sort_list_unique(arg1_list,arg2_delim)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.sort_list_unique(arg1_list,arg2_delim)) / count(*) * 100 as percent_correct from udx.udf__sort_list_unique__sss;

select "sort_list_set (s,s,s):s" as udf, sum( outcome IS NOT DISTINCT FROM udx.sort_list_set(arg1_list,arg2_delim_set,arg3_output_delim)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.sort_list_set(arg1_list,arg2_delim_set,arg3_output_delim)) / count(*) * 100 as percent_correct from udx.udf__sort_list_set__ssss;

select "sort_alleles (s,s):s" as udf, sum( outcome IS NOT DISTINCT FROM udx.sort_alleles(arg1_alleles,arg2_delim)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.sort_alleles(arg1_alleles,arg2_delim)) / count(*) * 100 as percent_correct from udx.udf__sort_alleles__sss;

select "to_aa (s):s" as udf, sum(outcome IS NOT DISTINCT FROM udx.to_aa(arg1_nucleotides)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.to_aa(arg1_nucleotides)) / count(*) * 100 as percent_correct from udx.udf__to_aa__ss;

select "to_aa (s,s,i):s" as udf, sum( outcome IS NOT DISTINCT FROM udx.to_aa(arg1_nucleotides,arg2_replacement,arg3_start_position)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.to_aa(arg1_nucleotides,arg2_replacement,arg3_start_position)) / count(*) * 100 as percent_correct from udx.udf__to_aa__ssis;

select "reverse_complement (s):s" as udf, sum( outcome IS NOT DISTINCT FROM udx.reverse_complement(arg1_nucleotides)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.reverse_complement(arg1_nucleotides)) / count(*) * 100 as percent_correct from udx.udf__reverse_complement__ss;

select "substr_range (s,s):s" as udf, sum( outcome IS NOT DISTINCT FROM udx.substr_range(arg1_string,arg2_range_coords)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.substr_range(arg1_string,arg2_range_coords)) / count(*) * 100 as percent_correct from udx.udf__substr_range__sss;

select "range_from_list (s,s):s" as udf, sum( outcome IS NOT DISTINCT FROM udx.range_from_list(arg1_list,arg2_delim)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.range_from_list(arg1_list,arg2_delim)) / count(*) * 100 as percent_correct from udx.udf__range_from_list__sss;

select "mutation_list (s,s):s" as udf, sum( outcome IS NOT DISTINCT FROM udx.mutation_list(arg1_seq,arg2_seq)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.mutation_list(arg1_seq,arg2_seq)) / count(*) * 100 as percent_correct from udx.udf__mutation_list__sss;

select "mutation_list (s,s,s):s" as udf, sum( outcome IS NOT DISTINCT FROM udx.mutation_list(arg1_seq,arg2_seq,arg3_range)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.mutation_list(arg1_seq,arg2_seq,arg3_range)) / count(*) * 100 as percent_correct from udx.udf__mutation_list__ssss;

select "mutation_list_nt (s,s):s" as udf, sum( outcome IS NOT DISTINCT FROM udx.mutation_list_nt(arg1_seq,arg2_seq)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.mutation_list_nt(arg1_seq,arg2_seq)) / count(*) * 100 as percent_correct from udx.udf__mutation_list_nt__sss;

select "hamming_distance (s,s):i" as udf, sum( outcome IS NOT DISTINCT FROM udx.hamming_distance(arg1_seq,arg2_seq)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.hamming_distance(arg1_seq,arg2_seq)) / count(*) * 100 as percent_correct from udx.udf__hamming_distance__ssi;

select "hamming_distance (s,s,s):i" as udf, sum( outcome IS NOT DISTINCT FROM udx.hamming_distance(arg1_seq,arg2_seq,arg3_pairwise_deletion_set)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.hamming_distance(arg1_seq,arg2_seq,arg3_pairwise_deletion_set)) / count(*) * 100 as percent_correct from udx.udf__hamming_distance__sssi;

select "nt_distance (s,s):i" as udf, sum( outcome IS NOT DISTINCT FROM udx.nt_distance(arg1_seq,arg2_seq)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.nt_distance(arg1_seq,arg2_seq)) / count(*) * 100 as percent_correct from udx.udf__nt_distance__ssi;

select "contains_element (s,s,s):b" as udf, sum( outcome IS NOT DISTINCT FROM udx.contains_element(arg1_string,arg2_list,arg3_delim)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.contains_element(arg1_string,arg2_list,arg3_delim)) / count(*) * 100 as percent_correct from udx.udf__contains_element__sssb;

select "is_element (s,s,s):b" as udf, sum( outcome IS NOT DISTINCT FROM udx.is_element(arg1_string,arg2_list,arg3_delim)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.is_element(arg1_string,arg2_list,arg3_delim)) / count(*) * 100 as percent_correct from udx.udf__is_element__sssb;

select "contains_sym (s,s):b" as udf, sum( outcome IS NOT DISTINCT FROM udx.contains_sym(arg1_string,arg2_string)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.contains_sym(arg1_string,arg2_string)) / count(*) * 100 as percent_correct from udx.udf__contains_sym__ssb;

select "complete_date (s):s" as udf, sum(outcome IS NOT DISTINCT FROM udx.complete_date(arg1_date)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.complete_date(arg1_date)) / count(*) * 100 as percent_correct from udx.udf__complete_date__ss;

select "variant_hash (s):s" as udf, sum( outcome IS NOT DISTINCT FROM udx.variant_hash(arg1_residues)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.variant_hash(arg1_residues)) / count(*) * 100 as percent_correct from udx.udf__variant_hash__ss;

select "nt_id (s):s" as udf, sum(outcome IS NOT DISTINCT FROM udx.nt_id(arg1_nucleotides)) as correct, count(*) as total_tests, sum( outcome IS NOT DISTINCT FROM udx.nt_id(arg1_nucleotides)) / count(*) * 100 as percent_correct from udx.udf__nt_id__ss;
