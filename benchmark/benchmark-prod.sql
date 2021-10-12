
select id1, id2, udx.pcd(aa_aln1, aa_aln2) as PCD
FROM (select nt_id as id1, aa_aln as aa_aln1 from sc2_src.alignments where protein = "S" limit 1000) as s1,
     (select nt_id as id2, aa_aln as aa_aln2 from sc2_src.alignments where protein = "S" limit 1000) as s2
