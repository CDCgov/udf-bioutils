# CHANGELOG #

## Changes in udf-bioutils v1.1 (as of 7/29/2024) ##
- Changed the annotation for glycosylation in mutation\_list\_gly and mutation\_list\_indel\_gly from reporting `-ADD-GLY` and `-LOSS-GLY` to the more conventionally accepted `(CHO+)`, `(CHO+/-)`, and `(CHO-)`.
- `mutation_list_gly` and `mutation_list_indel_gly` will ignore `.` (which represents an unresolved base at the 3\` or 5\` end) in a sequence but will correctly treat `-` as a gap.
- Unit tests are changed to test for whether `.` is correctly ignored in `mutation_list_gly` and `mutation_list_indel_gly`.

