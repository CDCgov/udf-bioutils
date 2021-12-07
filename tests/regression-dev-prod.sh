#!/bin/bash

if [ "$#" -ne "1" ];then
    echo -e "\nUsage:\n\t$0 <function>\n"
    exit 0
elif [ "${1,,}" == "mutation_list" ];then
    Q='select SUM(udx.mutation_list(aa_aln1, aa_aln2) IS NOT DISTINCT FROM udx.dev_mutation_list(aa_aln1, aa_aln2)) as PASSED, count(*) as TOTAL
    FROM (select nt_id as id1, aa_aln as aa_aln1 from sc2_src.alignments where protein = "S" limit 1000) as s1,
         (select nt_id as id2, aa_aln as aa_aln2 from sc2_src.alignments where protein = "S" limit 1000) as s2'
elif [ "${1,,}" == "mutation_list_pds" ];then
    Q='select SUM(udx.mutation_list_pds(aa_aln1, aa_aln2, ".~X") IS NOT DISTINCT FROM udx.dev_mutation_list_pds(aa_aln1, aa_aln2, ".~X")) as PASSED, count(*) as TOTAL
        FROM (select nt_id as id1, aa_aln as aa_aln1 from sc2_src.alignments where protein = "S" limit 1000) as s1,
        (select nt_id as id2, aa_aln as aa_aln2 from sc2_src.alignments where protein = "S" limit 1000) as s2'
elif [ "${1,,}" == "nt_distance" ];then
    Q='select SUM(udx.nt_distance(cds_aln1, cds_aln2) IS NOT DISTINCT FROM udx.dev_nt_distance(cds_aln1, cds_aln2)) as PASSED, count(*) as TOTAL
        FROM (select nt_id as id1, cds_aln as cds_aln1 from sc2_src.alignments where protein = "S" limit 1000) as s1,
         (select nt_id as id2, cds_aln as cds_aln2 from sc2_src.alignments where protein = "S" limit 1000) as s2'
elif [ "${1,,}" == "mutation_list_nt" ];then
    Q='select SUM(udx.mutation_list_nt(cds_aln1, cds_aln2) IS NOT DISTINCT FROM udx.dev_mutation_list_nt(cds_aln1, cds_aln2)) as PASSED, count(*) as TOTAL
        FROM (select nt_id as id1, cds_aln as cds_aln1 from sc2_src.alignments where protein = "S" limit 1000) as s1,
         (select nt_id as id2, cds_aln as cds_aln2 from sc2_src.alignments where protein = "S" limit 1000) as s2'
elif [ "${1,,}" == "substr_range" ];then
    Q='select SUM( udx.substr_range(cds_aln1, "1..10,50..25,100..1000;12;14..18;250..100") 
        is not distinct from  udx.dev_substr_range(cds_aln1, "1..10,50..25,100..1000;12;14..18;250..100") ), count(*) as TOTAL
        FROM (select nt_id as id2, cds_aln as cds_aln1 from sc2_src.alignments where protein = "S" and left(nt_id,1) = "a") as s2'
elif [ "${1,,}" == "reverse_complement" ];then
    Q='select SUM(udx.reverse_complement(cds_aln1) IS NOT DISTINCT FROM udx.dev_reverse_complement(cds_aln1)) as PASSED, count(*) as TOTAL
        FROM (select nt_id as id1, cds_aln as cds_aln1 from sc2_src.alignments where protein = "S" limit 1000000) as s1'
elif [ "${1,,}" == "md5" ];then
    Q='select SUM(udx.md5(cds_aln1) IS NOT DISTINCT FROM udx.dev_md5(cds_aln1)) as PASSED, count(*) as TOTAL
        FROM (select nt_id as id1, cds_aln as cds_aln1 from sc2_src.alignments where protein = "S" limit 1000000) as s1'
elif [ "${1,,}" == "complete_date" ];then
    Q='select SUM(udx.complete_date(covv_collection_date) IS NOT DISTINCT FROM udx.dev_complete_date(covv_collection_date)), count(*) as T from sc2_src.gisaid_ingest'
elif [ "${1,,}" == "sort_list" ];then
    Q='select SUM(udx.sort_list(left(cds_aln2,1000),"AG") IS NOT DISTINCT FROM udx.dev_sort_list(left(cds_aln2,1000),"AG")), count(*)
       FROM (select nt_id as id2, cds_aln as cds_aln2 from sc2_src.alignments where protein = "S" and left(nt_id,1) = "a") as s2'
else
    echo "$1 not found"
fi

echo "REGRESSION TEST for '$1' between DEV and PROD"
time himpala "$Q"
