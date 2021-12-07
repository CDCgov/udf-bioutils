#!/bin/bash

if [ "$#" -ne "1" ];then
    echo -e "\nUsage:\n\t$0 <function>\n"
    exit 0
elif [ "${1,,}" == "mutation_list" ];then
    Q=' select id1, id2, udx.mutation_list(aa_aln1, aa_aln2) as V
        FROM (select nt_id as id1, aa_aln as aa_aln1 from sc2_src.alignments where aa_id = "5beece4b7a01ce068efc2ea91f2c668c") as s1,
       (select nt_id as id2, aa_aln as aa_aln2 from sc2_src.alignments where protein = "S" and left(nt_id,2) = "ab" ) as s2'
elif [ "${1,,}" == "mutation_list_pds" ];then
    Q='select id1, id2, udx.mutation_list_pds(aa_aln1, aa_aln2, ".~X") as V
        FROM (select nt_id as id1, aa_aln as aa_aln1 from sc2_src.alignments where aa_id = "5beece4b7a01ce068efc2ea91f2c668c") as s1,
       (select nt_id as id2, aa_aln as aa_aln2 from sc2_src.alignments where protein = "S" and left(nt_id,2) = "ab") as s2'
elif [ "${1,,}" == "pcd" ];then
    Q='select id1, id2, udx.pcd(aa_aln1, aa_aln2) as V
        FROM (select nt_id as id1, aa_aln as aa_aln1 from sc2_src.alignments where aa_id = "5beece4b7a01ce068efc2ea91f2c668c") as s1,
       (select nt_id as id2, aa_aln as aa_aln2 from sc2_src.alignments where protein = "S" and left(nt_id,2) = "ab") as s2'
elif [ "${1,,}" == "nt_distance" ];then
    Q='select id1, id2, udx.nt_distance(cds_aln1, cds_aln2) as V
        FROM (select nt_id as id1, cds_aln as cds_aln1 from sc2_src.alignments where cds_id = "273b3e5d14b23261453c5883f52b9f7340fd7516") as s1,
        (select nt_id as id2, cds_aln as cds_aln2 from sc2_src.alignments where protein = "S" and left(nt_id,2) = "ab") as s2'
elif [ "${1,,}" == "mutation_list_nt" ];then
    Q='select id1, id2, udx.mutation_list_nt(cds_aln1, cds_aln2) as V
       FROM (select nt_id as id1, cds_aln as cds_aln1 from sc2_src.alignments where cds_id = "273b3e5d14b23261453c5883f52b9f7340fd7516") as s1,
       (select nt_id as id2, cds_aln as cds_aln2 from sc2_src.alignments where protein = "S" and left(nt_id,2) = "ab") as s2'
elif [ "${1,,}" == "substr_range" ];then
    Q='select id2, udx.substr_range(cds_aln2, "1..10,50..25,100..1000;12;14..18;250..100")
       FROM (select nt_id as id2, cds_aln as cds_aln2 from sc2_src.alignments where protein = "S" and left(nt_id,1) = "a") as s2'
elif [ "${1,,}" == "reverse_complement" ];then
    Q='select id2, length(udx.reverse_complement(cds_aln2))
       FROM (select nt_id as id2, cds_aln as cds_aln2 from sc2_src.alignments where protein = "S" and left(nt_id,1) = "a") as s2'
elif [ "${1,,}" == "md5" ];then
    Q='select id2, udx.md5(cds_aln2,"SAM","ROCKS!")
       FROM (select nt_id as id2, cds_aln as cds_aln2 from sc2_src.alignments where protein = "S" and left(nt_id,1) = "a") as s2'
elif [ "${1,,}" == "sort_list" ];then
    Q='select id2, udx.sort_list(left(cds_aln2,1000),"AG")
       FROM (select nt_id as id2, cds_aln as cds_aln2 from sc2_src.alignments where protein = "S" and left(nt_id,1) = "a") as s2'
elif [ "${1,,}" == "complete_date" ];then
    Q='select udx.complete_date(collection_date), udx.complete_date(collection_date), udx.complete_date(collection_date) from sc2_src.ncbi_ingest union all select udx.complete_date(covv_collection_date), udx.complete_date(covv_collection_date), udx.complete_date(covv_collection_date) from sc2_src.gisaid_ingest'
else
    echo "$1 not found"
fi

echo "Doing $1 for PROD"
time himpala "$Q" | pv --line-mode --rate |wc -l
