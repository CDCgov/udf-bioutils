# UDF-BioUtils Changelog #

## v1.3.1 (2024-10-16) ##

- Optimized functions `sort_alleles` and `sort_list_unique` for efficiency.
- Updated documentation

## v1.3 (2024-10-04) ##

- Added distance function `tn_93` for calculating Tamura-Nei (TN-93) Distance  between two aligned nucleotide sequences, with optional parameter for gamma correction for variability of mutations among sites.

## v1.2 (2024-08-22) ##

- Added aggregate functions `alphanumeric_entropy`, `nt_entropy`, `aa_entropy`, and `codon_entropy` which will calculate the Shannon information entropy for alphanumeric strings, nucleotides, amino acids and codons respectively (the latter three expect only one per record).
- Added scalar function `alnum_entropy` to calculate the Shannon entropy of an alphanumeric string.

## v1.1 (2024-07-27) ##

- Changed the annotation for glycosylation in `mutation_list_gly` and `mutation_list_indel_gly` from reporting `-ADD-GLY` and `-LOSS-GLY` to the more conventionally accepted `(CHO+)`, `(CHO+/-)`, and `(CHO-)`.
- `mutation_list_gly` and `mutation_list_indel_gly` will ignore `.` (which represents an unresolved base at the 3\` or 5\` end) in a sequence but will correctly treat `-` as a gap.
- Unit tests are changed to test for whether `.` is correctly ignored in `mutation_list_gly` and `mutation_list_indel_gly`.
