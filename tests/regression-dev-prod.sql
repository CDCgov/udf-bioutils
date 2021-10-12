select SUM(udx.dev_pcd(aa_aln1, aa_aln2) IS NOT DISTINCT FROM udx.pcd(aa_aln1, aa_aln2)) as PASSED, count(*) as TOTAL
FROM (select nt_id as id1, aa_aln as aa_aln1 from sc2_src.alignments where protein = "S" limit 1000) as s1,
     (select nt_id as id2, aa_aln as aa_aln2 from sc2_src.alignments where protein = "S" limit 1000) as s2
