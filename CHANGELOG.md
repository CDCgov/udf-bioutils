# CHANGELOG #

## Changes in udf-bioutils v1.2 (as of 8/22/2024) ##

- Added aggregate functions `alphanumeric_entropy`, `nt_entropy`, `aa_entropy`, and `codon_entropy` which will calculate the Shannon information entropy for alphanumeric strings, nucleotides, amino acids and codons respectively (the latter three expect only one per record).
- Added scalar function `alnum_entropy` to calculate the Shannon entropy of an alphanumeric string.

## Changes in udf-bioutils v1.1 (as of 7/29/2024) ##

- Changed the annotation for glycosylation in `mutation_list_gly` and `mutation_list_indel_gly` from reporting `-ADD-GLY` and `-LOSS-GLY` to the more conventionally accepted `(CHO+)`, `(CHO+/-)`, and `(CHO-)`.
- `mutation_list_gly` and `mutation_list_indel_gly` will ignore `.` (which represents an unresolved base at the 3\` or 5\` end) in a sequence but will correctly treat `-` as a gap.
- Unit tests are changed to test for whether `.` is correctly ignored in `mutation_list_gly` and `mutation_list_indel_gly`.
