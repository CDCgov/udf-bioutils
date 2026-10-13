# UDF-BioUtils Changelog #

## Unreleased ##

- Add missing `skewness`/`kurtosis` serializers; existing registrations need manual reinitialization.
- Handle failed string-result allocations and fix invalid memory access for malformed ranges, CDS maps, delimiter-only lists, and non-ASCII sequence bytes.
- Reject overflowing integer parses, prevent coordinate arithmetic overflow, and release benchmark test contexts.
- Correct codon entropy for `ACA`; report entropy counter overflow without changing the intermediate-state format.
- Return `NULL` for undefined or unrepresentable t-distribution results instead of allowing exceptions to escape.
- Add regression tests and CTest registration, check SQL serializer bindings, and return a failing exit status when UDA tests fail.

## v1.6.0 (2026-05-20) ##

- Added UDA `pearson_corr` for one-pass correlations
- Added a helper UDF `og_pos_to_cds_allele`, where users can provide the `cds_aln`, and the function will take the cds position and return the 1-base substring at that position in the sequence.

## v1.5.1 (2026-04-08) ##

- Fixes underflow bug in `Mutation_List_Strict_GLY`.

## v1.5.0 (2025-07-31) ##

- Added functions `date_to_decimal` and `decimal_to_date` for interconversion between standard dates and a double format used by programs such as BEAST.

## v1.4.0 (2024-10-28) ##

- Added function `sort_site_list` for numerically sorting a string of comma-separated integers, as returned by `group_concat()`'s.
- _INTERNAL_: Removed redundant inline function `split_by_substr_view` and instead used the already-existing `split_by_substr` function.

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
