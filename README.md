# Overview of UDF BIOUTILS

The `udf-bioutils` project contains bioinformatics (and public health surveillance adjacent) functions used internally by CDC's respiratory virus surveillance programs. Functions must be [uploaded](install_SO.sh) and [registered](sql/) with [Apache Impala](https://impala.apache.org). UDFs and UDAs extend Impala's native capabilities with domain-specific analysis. This library, in concert with external programs and specialized schema, helps form a basis for database-centric sequence surveillance analytics. See [INSTALL.md](INSTALL.md) for building and installation instructions.

For direct correspondence, feel free to contact: Samuel S. Shepard ([vfn4@cdc.gov](mailto:%22Samuel%20Shepard%22%3cvfn4@cdc.gov>?subject=UDF%20BIOUTILS)), Centers for Disease Control and Prevention.

---

- [Overview of UDF BIOUTILS](#overview-of-udf-bioutils)
- [User-defined bioinformatics utilities for Impala SQL](#user-defined-bioinformatics-utilities-for-impala-sql)
  - [Scalar Function Descriptions](#scalar-function-descriptions)
    - [Date Functions](#date-functions)
      - [Complete Date](#complete-date)
      - [Fortnight Date](#fortnight-date)
      - [Saturday Date](#saturday-date)
      - [To EpiWeek](#to-epiweek)
      - [Date to Decimal and Decimal to Date](#date-to-decimal-and-decimal-to-date)
    - [ID Functions](#id-functions)
      - [Variant Hash and Nucleotide ID](#variant-hash-and-nucleotide-id)
      - [md5](#md5)
    - [Math Functions](#math-functions)
      - [Confidence Interval for T-distributions](#confidence-interval-for-t-distributions)
      - [Quantile for T-distribution](#quantile-for-t-distribution)
    - [General Bioinformatic Functions](#general-bioinformatic-functions)
      - [Reverse Complement](#reverse-complement)
      - [To Amino Acids](#to-amino-acids)
      - [To Amino Acids with Degeneracy Up to 3](#to-amino-acids-with-degeneracy-up-to-3)
    - [Sequence Comparison](#sequence-comparison)
      - [Hamming and Nucleotide Distance](#hamming-and-nucleotide-distance)
      - [Tamura-Nei Distance (TN-93)](#tamura-nei-distance-tn-93)
      - [Sequence Difference Functions](#sequence-difference-functions)
      - [Mutation List Family of Functions](#mutation-list-family-of-functions)
      - [Physiochemical Distance](#physiochemical-distance)
      - [Physiochemical Difference List](#physiochemical-difference-list)
    - [Sequence Quality Control](#sequence-quality-control)
      - [Amino Acid and Nucleotide standardization](#amino-acid-and-nucleotide-standardization)
      - [Longest Deletion](#longest-deletion)
    - [String Manipulation and Matching](#string-manipulation-and-matching)
      - [Any Character In String](#any-character-in-string)
      - [Contains Substring Symmetric Check](#contains-substring-symmetric-check)
      - [Contains Any Element In List](#contains-any-element-in-list)
      - [Cut and Paste](#cut-and-paste)
      - [Is Element In List](#is-element-in-list)
      - [Range from Integer List](#range-from-integer-list)
      - [Substring by Range](#substring-by-range)
    - [Sorting Functions](#sorting-functions)
      - [Sort Alleles](#sort-alleles)
      - [Sort List Family of Functions](#sort-list-family-of-functions)
    - [DAIS-ribosome Related Functions](#dais-ribosome-related-functions)
      - [Codon at Original Position](#codon-at-original-position)
      - [Original Position to AA or CDS Position](#original-position-to-aa-or-cds-position)
      - [Original Position to Degenerate Amino Acid Mutation](#original-position-to-degenerate-amino-acid-mutation)
  - [Aggregate Function Descriptions](#aggregate-function-descriptions)
    - [Bitwise Sum](#bitwise-sum)
    - [Kurtosis](#kurtosis)
    - [LogFold Titer Distribution Agreement](#logfold-titer-distribution-agreement)
    - [Skewness](#skewness)
    - [Entropy](#entropy)
- [Acknowledgments](#acknowledgments)
- [Notices](#notices)
  - [Public Domain Standard Notice](#public-domain-standard-notice)
  - [License Standard Notice](#license-standard-notice)
  - [Privacy Standard Notice](#privacy-standard-notice)
  - [Contributing Standard Notice](#contributing-standard-notice)
  - [Records Management Standard Notice](#records-management-standard-notice)
  - [Additional Standard Notices](#additional-standard-notices)

# User-defined bioinformatics utilities for Impala SQL

Functions may be created in a schema such as `udx`. This is our default, but the user is free to change the function location to whatever they wish. To show functions already registered, one may write:

```sql
use udx; 
show functions;
show aggregate functions;
```

For further readings related to function development, see:

- [Impala User-Defined Functions](https://docs.cloudera.com/cdp-private-cloud-base/latest/impala-sql-reference/topics/impala-udf.html)
- [Impala UDF Samples](https://github.com/cloudera/impala-udf-samples)
- [Impala GitHub mirror](https://github.com/apache/impala)

## Scalar Function Descriptions

Scalar functions take any number of values from a **single** row and return a single value.

---

### Date Functions

#### Complete Date

```sql
complete_date(STRING date) -> STRING
```

**Purpose:** Parses STRING dates with delimiters `.`,`/`, and `-`; adds missing month or day component when applicable (as the first of either). A `NULL` in the argument will return a null value.

#### Fortnight Date

```sql
fortnight_date(<STRING or TIMESTAMP or DATE> date [, BOOLEAN default_cadence]) -> DATE
```

**Purpose:** Given a valid date, the function returns the date of the LAST Saturday within a complete, two-week period. Fortnights begin on a Sunday. In other words, the date is rounded to the last date of the fortnight (ending in a Saturday). The optional `default_cadence` parameter changes which two-week period is being considered, where a `false` value results in the fortnight dates being adjusted by one week.

Null inputs, invalid dates, or the empty STRING will return `null`.  If the `date` is a STRING it must be in *YYYY-MM-DD* format but may also be delimited using `.` or `/`. Partial STRING dates are considered invalid but can be corrected (see [complete_date](#complete-date)). Finally, dates before `1400-01-01` return `NULL`, even if the date is considered valid otherwise.

#### Saturday Date

```sql
saturday_date(<STRING or TIMESTAMP or DATE> date) -> DATE
```

**Purpose:** Given a valid date, the function returns the date of the Saturday within the same week. In other words, the date is rounded to the last date for that week (Sunday to Saturday).

Null inputs, invalid dates, or the empty STRING will return `null`.  If the `date` is a STRING it must be in *YYYY-MM-DD* format but may also be delimited using `.` or `/`. Partial STRING dates are considered invalid but can be corrected (see [complete_date](#complete-date)). Finally, dates before `1400-01-01` return `NULL`, even if the date is considered valid otherwise.

#### To EpiWeek

```sql
to_epiweek(<STRING or TIMESTAMP> date [, BOOLEAN year_format]) -> INT
```

**Purpose:** Returns the [MMWR or EPI](https://wwwn.cdc.gov/nndss/document/MMWR_Week_overview.pdf) week of a formatted date STRING or timestamp. Normally the output is an integer ranging from 1 up to 52 or 53. The *optional* `year_format` (defaults to `false`) adds the padded week to the year so that the output can be graphed on a contiguous timeline. Some examples using the year format would be: *202102* and *199853*.

Null inputs, invalid dates, or the empty STRING will return `null`.  If the `date` is a STRING it must be in *YYYY-MM-DD* format but may also be delimited using `.` or `/`. Partial STRING dates are considered invalid but can be corrected (see [complete_date](#complete-date)). Finally, dates before `1400-01-01` return `NULL`, even if the date is considered valid otherwise.

#### Date to Decimal and Decimal to Date

```sql
date_to_decimal(<STRING or TIMESTAMP or DATE> date) -> DOUBLE
decimal_to_date(DOUBLE) -> DATE
```

**Purpose:** Converts a provided date into a double format, as used by [BEAST](https://beast.community/tip_dates), and vice versa. The double output will be a decimal with the integer part equal to the year of the provided date, and the fractional part representing the proportion of the year that *elapsed* at the *completion* of the given month and day. `decimal_to_date()` will convert the double back into the standard date format.

**Example:**

```sql
select udx.date_to_decimal("2025-06-16") --> 2025.458
select udx.decimal_to_date(2025.047)     --> "2025-01-17"
```

Null inputs, invalid dates, or the empty STRING will return `null`.  If the `date` is a STRING it must be in *YYYY-MM-DD* format but may also be delimited using `.` or `/`. Partial STRING dates are considered invalid but can be corrected (see [complete_date](#complete-date)). Finally, dates before `1400-01-01` return `NULL`, even if the date is considered valid otherwise.

### ID Functions

#### Variant Hash and Nucleotide ID

```sql
variant_hash(STRING residues) -> STRING
nt_id(STRING nucleotides) -> STRING
```

**Purpose:** Returns hashed identifiers for aligned or unaligned sequences. Case is ignored in the hash, as is whitespace, `:`, `-`, and `.`. The `nt_id` function also ignores `~`, which represents a translated partial codon in the `variant_hash`. The `variant_hash` is a 32 character [hexadecimal](https://en.wikipedia.org/wiki/Hexadecimal#Binary_conversion) from the [md5](https://en.wikipedia.org/wiki/MD5) hash while the `nt_id` is a 40 character hexadecimal from the [sha1](https://en.wikipedia.org/wiki/SHA-1) hash. Null values or empty STRING return `NULL`.
<br /><br />

#### md5

```sql
md5(STRING field1 [,STRING field2, ...]) -> STRING
```

**Purpose:** This function takes a variable number of STRING fields (i.e., "[variadic](https://en.wikipedia.org/wiki/Variadic_function)") and returns a 32 character [hexadecimal](https://en.wikipedia.org/wiki/Hexadecimal#Binary_conversion) from the [md5](https://en.wikipedia.org/wiki/MD5) hash.
Any null value in an argument or all empty STRINGs returns `NULL`. Note: fields are concatenated with the [bell character](https://en.wikipedia.org/wiki/Bell_character) as the delimiter before hashing in order to preserve field boundaries.  

&rarr; *See also the Impala native functions [MURMUR_HASH](https://docs.cloudera.com/cdp-private-cloud-base/7.1.8/impala-sql-reference/topics/impala-math-functions.html#math_functions__murmur_hash), [FNV_HASH](https://docs.cloudera.com/cdp-private-cloud-base/7.1.8/impala-sql-reference/topics/impala-math-functions.html#math_functions__fnv_hash), [SHA1/SHA2](https://docs.cloudera.com/cdp-private-cloud-base/7.1.8/impala-sql-reference/topics/impala-hash-functions.html), and [HEX](https://docs.cloudera.com/cdp-private-cloud-base/7.1.8/impala-sql-reference/topics/impala-math-functions.html#math_functions__hex).*
<br /><br />

### Math Functions

#### Confidence Interval for T-distributions

```sql
ci_t(DOUBLE confidence, BIGINT sample_size, DOUBLE standard_deviation [, BOOLEAN is_two_sided]) -> DOUBLE
```

**Purpose:** Takes the confidence level (e.g., 0.99), `sample_size`,sample `standard_deviation` and an optional boolean (default is two-sided) for sided-ness in order to generate the [confidence interval](https://en.wikipedia.org/wiki/Confidence_interval#Example) for the [Student's T-distribution](https://en.wikipedia.org/wiki/Student%27s_t-distribution). In other words, the value returned is +/- around the point estimate.

#### Quantile for T-distribution

```sql
quantile_t(DOUBLE confidence, BIGINT samples, BOOLEAN is_two_sided) -> DOUBLE
```

**Purpose:** Given some `confidence` level (e.g., 0.99), number of `samples` and a boolean for whether the [Student's T-distribution](https://en.wikipedia.org/wiki/Student%27s_t-distribution) is two-sided, estimate the [quantile function](https://en.wikipedia.org/wiki/Quantile_function).

### General Bioinformatic Functions

#### Reverse Complement

```sql
reverse_complement(STRING nucleotides) -> STRING
```

**Purpose:** Returns the reverse complement nucleotide sequence. Case is preserved and ambiguous nucleotides are mapped. Non-nucleotide characters are left as-is but in reverse order. Null values return `NULL` and an empty STRING remains empty.

#### To Amino Acids

```sql
to_aa(STRING nucleotides[, STRING replacement_nucleotides, int starting_position])
```

**Purpose:** Translates a nucleotide sequence to an amino acid sequence starting at position 1 of argument `nucleotides` (including resolvable ambiguous codons). Unknown or partial codons are translated as `?`, mixed or partially gapped codons are translated as `~`, and deletions (`-`) or missing data (`.`) are compacted from 3 to 1 character. Residues are always written out in uppercase. *Optionally*, one may overwrite a portion of the nucleotide sequence prior to translation by providing `replacement_nucleotides` and a `starting_position`. Specifying out-of-range indices will append to the 5' or 3' end while specifying a replacement sequence larger than the original will result in the extra nucleotides being appended after in-range bases are overwritten. If any argument is `NULL` a null value is returned. If the `replacement_nucleotides` argument is an empty STRING, the `nucleotides` argument is translated as-is. On the other hand, if the `nucleotides` argument is an empty STRING but `replacement_nucleotides` is not, then `replacement_nucleotides` is translated and returned.

#### To Amino Acids with Degeneracy Up to 3

```sql
to_aa3(STRING nucleotides) -> STRING
```

**Purpose:** Translates DNA coding sequences into an amino acid sequence. However, if a codon could result in more than one translated AA residue, then up to 3 possible translations will be prouduced (separated by `/`) before reverting to an `X`. If the degenerate translation is in a sequence it is enclosed in brackets to avoid misreading.

**Example:**

```sql
select udx.to_aa3("ATGsCCTCCTGA") --> "M[A/P]S*"
select udx.to_aa3("GrN")          --> "D/E/G"
select udx.to_aa3("GNy")          --> "X" (more than 3 residues possible)
```

### Sequence Comparison

#### Hamming and Nucleotide Distance

```sql
hamming_distance(STRING seq1, STRING seq2 [, STRING pairwise_deletion_set])
nt_distance(STRING seq1, STRING seq2)
```

**Return type:** `INT`  
**Purpose:** Counts the [number of differences](https://en.wikipedia.org/wiki/Hamming_distance) between two sequences (though any STRINGs may be used).
If one sequence is longer than the other, the extra characters are discarded from the calculation. By default, any pair of characters with a `.` as an element is ignored by the calculation. In *DAIS*, the `.` character is used for missing data. Optionally, one may explicitly add a pairwise deletion character set. If any pair of characters contain any of the characters in the argument, that position is ignored from the calculation. If any argument is `NULL` or either sequence argument is an empty STRING, a null value is returned. If the optional `pairwise_deletion_set` argument is an empty STRING, no pairwise deletion is performed. The `nt_distance` function is the same as the default version of `hamming_distance` but does not count ambiguated differences. For example, A ≠ T but A = R.  

&rarr; *See also the Impala native function [JARO_DISTANCE](https://docs.cloudera.com/cdp-private-cloud-base/7.1.8/impala-sql-reference/topics/impala-string-functions.html?#string_functions__jaro_distance).*

#### Tamura-Nei Distance (TN-93)

```sql
tn_93(STRING sequence1, STRING sequence2 [, DOUBLE alpha]) -> DOUBLE
```

**Purpose:** Calculates the [Tamura-Nei (TN-93)](https://pubmed.ncbi.nlm.nih.gov/8336541/) evolutionary distance between two **aligned** nucleotide sequences. The model accounts for different base frequencies of each nucleotide, as well as different rates for different substitution types: transitions, (A ↔ G or C ↔ T) type 1 transversions, (A ↔ T and C ↔ G) and type 2 transversions. (A ↔ C and G ↔ T). If either argument is `NULL` or `""` then a null is returned. For very short and dissimilar sequences, a null value may also be returned due to needing to calculate the logarithm of a non-positive number. This model can optionally include a correction for rate variability among sites using a [gamma](http://abacus.gene.ucl.ac.uk/ziheng/pdf/1996YangTREEv11p367.pdf) distribution with a single shape parameter (alpha). Smaller values of alpha represent greater rate variation, while larger values suggest more uniform rates. If no alpha is specified, the distance is calculated under the assumption of equal rates across all sites.

#### Sequence Difference Functions

```sql
sequence_diff(STRING seq1, STRING seq2)
sequence_diff_nt(STRING seq1, STRING seq2)
```

**Return type:** `STRING`  
**Purpose:** Expects **aligned** biological sequences and returns a sequence that denotes the differences between `seq1` and `seq2` by returning the character from `seq2` in the differing positions. Otherwise, if the characters are the same at a given position, a `.` will be returned. For example, if `seq1` sequence is `AAAA-A` and `seq2` sequence is `AGA-AA`, the function will return `.G.-A.`.

- The function `sequence_diff_nt` is similar to the vanilla `sequence_diff` function but will correctly account for degenerate nucleotides (e.g., Y for pyrimidine, R for purine).

#### Mutation List Family of Functions

```sql
mutation_list(STRING seq1, STRING seq2 [, STRING range])
mutation_list_gly(STRING seq1, STRING seq2)
mutation_list_indel_gly(STRING seq1, STRING seq2)
mutation_list_nt(STRING seq1, STRING seq2)
mutation_list_pds(STRING seq1, STRING seq2, STRING pairwise_deletion_set)
```

**Return type:** `STRING`  
**Purpose:** Expects **aligned** biological sequences and returns a list of mutations from `seq1` to `seq2`, delimited by a comma + space.  If the `range` argument is included, only those sites will be compared (see the description of `range_coords` in `substr_range`). For example: `A2G, T160K, G340R`. If any argument is `NULL` or empty, a null value is returned. The function `mutation_list` returns differences and may be used for nucleotide, amino acid, or any other sequence. There are some alternative variants to the function:

- The function `mutation_list_gly` is similar to the vanilla `mutation_list` function but annotates the addition or loss of an [N-linked glycosylation site](https://en.wikipedia.org/wiki/N-linked_glycosylation#Transfer_of_glycan_to_protein) due to substitution or single deletion (from `seq1` to `seq2`). Function `mutation_list_indel_gly` is an enhancement of `mutation_list_gly` which robustly accounts for the effects of indels on glycosylation for deletions no larger than 5 (a parameter that is adjustable at compile-time). Warning: `mutation_list_indel_gly` is substantially slower than `mutation_list_gly`.
- The function `mutation_list_nt` is *suitable only for nucleotide sequences* and ignores resolvable differences involving ambiguous nucleotides (e.g., "R2G" would not be listed).
- The function `mutation_list_pds` also contains an explicit argument for a pairwise deletion character set. If any pair of characters contain any of the characters in the argument, that position is ignored from the calculation.

#### Physiochemical Distance

```sql
pcd(STRING sequence1, STRING sequence2) -> DOUBLE
```

**Purpose:**  Calculates the PCD ("physiochemical", or perhaps more precisely, the "physicochemical" distance) between two **aligned** amino acid sequences. First the function takes the [Euclidean distance](https://en.wikipedia.org/wiki/Euclidean_distance) of physiochemical factors (see [*Atchley et al.*'s protein sequence analysis](https://www.pnas.org/doi/full/10.1073/pnas.0408677102)) corresponding to the compared residues at each site. After that the per-site PCD values are averaged over all *valid* sites, or sites where both alleles contain normal amino residues. The function ignores sequence case and does not count `X` as a valid comparison. However, comparisons to deletion states (`-`) are calculated versus the 0-vector. If either argument is `NULL` or `""` then a null is returned. Anecdotally, S. Shepard has observed that the distances correlate strongly with [the JTT model](https://pubmed.ncbi.nlm.nih.gov/1633570/).

#### Physiochemical Difference List

```sql
pcd_list(STRING sequence1, STRING sequence2) -> STRING
```

**Purpose:**  Calculates the per-site "physiochemical distance" (PCD) between two **aligned** amino acid sequences (see description for `pcd()`). String-encoded PCDs *for each site* are space-delimited, and the number of sites will be the shorter of the two sequences. If either argument is `NULL` or `""` then a null is returned.

<br /><br />

### Sequence Quality Control

#### Amino Acid and Nucleotide standardization

```sql
aa_std(STRING sequence) -> STRING
nt_std(STRING sequence) -> STRING
```

**Purpose:** Standardizes the assumed amino acid or nucleotide `sequence` respectively by removing extraneous characters (`\n\r\t :.-`) and switching to uppercase. The nucleotide version also removes `~`, which is interpreted by [DAIS-ribosome](https://hub.docker.com/r/cdcgov/dais-ribosome/tags) as a partial codon for amino acid sequences. The same sequence cleaning is done by the ID generating functions [variant_hash and nt_id](#variant-hash-and-nucleotide-id). Returns `NULL` if the input is null or an empty string is provided.

#### Longest Deletion

```sql
longest_deletion(STRING sequence), deletion_events(STRING sequence) -> INT
```

**Return type:** `INT`  
**Purpose:** Returns the length of the longest deletion in the `sequence` or the number of deletion events respectively. In either function, a *deletion event* must have upstream and downstream alphabetic character to the deletion span (using `-` for deletion characters only). The functions return `null` if the sequence is null and they return 0 if the empty STRING is used.

### String Manipulation and Matching

#### Any Character In String

```sql
any_instr(STRING haystack, STRING needles) -> BOOLEAN
```

**Purpose:** Checks if any of the *characters* in the `needles` STRING is in the `haystack` STRING. Will return `NULL` if either argument is null. Note that if both `needles` and `haystack` has an empty string, the function always returns `TRUE`.  

&rarr; *See also the Impala native function [INSTR](https://docs.cloudera.com/cdp-private-cloud-base/7.1.8/impala-sql-reference/topics/impala-string-functions.html#string_functions__instr).*

#### Contains Substring Symmetric Check

```sql
contains_sym(STRING str1, STRING str2) -> BOOLEAN
```

**Purpose:** Returns true if `str1` is a substring of `str2` or *vice-versa*. If *just one* argument is an empty STRING, the function returns false. A `NULL` in any argument will return a null value.

#### Contains Any Element In List

```sql
contains_element(STRING str, STRING list, STRING delim) -> BOOLEAN
```

**Purpose:** Return TRUE if `str` contains *any* element in `list` delimited by `delim` as a **substring**. The delimiter may be a sequence of characters, but if it is empty the list is split by character. A `NULL` in any argument will return a null value.

#### Cut and Paste

```sql
cut_paste(STRING str, STRING delim, STRING fields [, STRING output_delim]) -> STRING
```

**Purpose:** For `str`, split the string using the `delim` and paste/concatenate back together based on the specified `fields` (similar to using Unix `cut` and/or `paste`). One can optionally specify the `output_delim` otherwise `delim` (or if `NULL`). The `fields` uses 1-based indexing which can be separated with `;` or `,` characters. Ranges are also allowed using `-` or `..` strings. The function will return `NULL` if either `str`, `delim`, or `fields` is `NULL`.

**Example:**

```sql
select udx.cut_paste("The::fields::are::cut::pastable::!", "::", "1..3;6,6;4-3", " ") 
-- Returns: "The fields are ! ! cut are"
```

#### Is Element In List

```sql
is_element(STRING str, STRING list, STRING delim) -> BOOLEAN
```

**Purpose:** Returns true if `str` is equal to any element of `list` delimited by `delim`. The delimiter may be a sequence of characters, but if it is empty the list is split by character.  

&rarr; *See also the Impala native function [FIND_IN_SET](https://docs.cloudera.com/cdp-private-cloud-base/latest/impala-sql-reference/topics/impala-string-functions.html#string_functions__find_in_set).*

#### Range from Integer List

```sql
range_from_list(STRING integer_list, STRING delim) -> STRING
```

**Purpose:** Takes STRING `list` of integers separated by STRING `delim` (split by substring) and returns a STRING *range* in the format described in `substr_range` for the field `range_coords`. List elements will be added uniquely as a set. If a non-integer element is found or any argument is `NULL`, a null value is returned. If an empty `list` is given it is returned as-is; if an empty `delim` is given, the `list` is returned.

#### Substring by Range

```sql
substr_range(STRING str, STRING range_coords) -> STRING
```

**Purpose:** Returns the characters in `str` specified by `range_coords`.
All characters are concatenated as specified by `range_coords`. Ranges may be listed in forward and reverse, which affects output order, and are denoted by `#..#`.
Multiple ranges or single characters may be separated using a semi-colon or comma. For example: `10..1;12;15;20..25`. If any argument is `NULL` or an empty STRING, a null value is returned.  

&rarr; *See also the Impala native function [SUBSTR](https://docs.cloudera.com/cdp-private-cloud-base/7.1.8/impala-sql-reference/topics/impala-string-functions.html#string_functions__substr).*
<br /><br />

### Sorting Functions

#### Sort Alleles

```sql
sort_alleles(STRING allele_or_mutation_list, STRING delim) -> STRING
```

**Return type:** `STRING`  
**Purpose:** Returns a list of sorted mutations (e.g., `A2G, T160K`) or alleles (e.g., `2G 160K`) where the list delimiter `delim` is used both for splitting elements and for the output of the sorted list. Alleles and mutations are sorted first by their integer position, unlike other list sorting which looks at the element as a STRING. If any argument is `NULL` a null value is returned.

#### Sort List Family of Functions

```sql
sort_list(STRING list, STRING delim)
sort_list_unique(STRING list, STRING delim)
sort_list_set(STRING list, STRING delim_set, STRING output_delim)
sort_site_list(SRING list)
```

**Return type:** `STRING`  
**Purpose:** Returns an alphabetically sorted list of elements in the STRING `list` delimited by `delim` or `delim_set`. The function `sort_list` interprets multi-character delimiters as a whole STRING while the function `sort_list_set` treats each character in the argument `delim_set` as a single-character delimiter (all are applied). The input and output delimiter for `sort_list` are taken to be the same while the output delimiter for `sort_list_set` is specified by `output_delim`. If any argument is `NULL` a null value is returned. The function variant `sort_list_unique` behaves exactly like `sort_list` but removes redundant elements. `sort_site_list` is for numerically sorting a string of comma-separated integers returned by the output of `group_concat()`, and will return `NULL` if non-numeric values are found in the list.

### DAIS-ribosome Related Functions

#### Codon at Original Position

```sql
codon_at_og_position(STRING query_nt_coords, STRING cds_nt_coords, STRING cds_aln, BIGINT original_position [, STRING allele]) -> STRING
```

**Purpose:** Takes coordinate mapping information provided by [DAIS-ribosome](https://hub.docker.com/r/cdcgov/dais-ribosome/tags), such as the  `query_nt_coords` and `cds_nt_coords`, the `cds_aln` (aligned coding sequence), and a position in the original coordinate space and then yields the whole codon at that position. The original position might come from an *untrimmed* [IRMA](https://wonder.cdc.gov/amd/flu/irma/) variant position. An *optional* allele argument may be provided in order to mutate the retrieved codon at the mapped CDS position. This can be convenient for analyzing variants.

Empty strings and null values on any input returns `NULL`. In particular, if the mapped position is *before*, *after*, or *in-between* (insertions) alignment-space coordinates, they are considered out-of-bounds and the function will return `NULL`.

**Example:**

```sql
-- Out of bounds
select udx.codon_at_og_position("4..6;8..10", "1..3;4..6", "ATGGAC", 3)                           --> NULL

-- Retrieves the in-frame codon regardless of codon position
select udx.codon_at_og_position("4..6;8..10", "1..3;4..6", "ATGGAC", 4)                           --> "ATG"
select udx.codon_at_og_position("4..6;8..10", "1..3;4..6", "ATGGAC", 6)                           --> "ATG"
select udx.codon_at_og_position("4..6;8..10", "1..3;4..6", "ATGGAC", 9)                           --> "GAC"

-- The retrieved codon can be mutated at the corresponding alignment position
select udx.codon_at_og_position("4..6;8..10;11..16", "1..3;4..6;7..12", "ATGTAGCATTAR", 16)       --> "TAR"
select udx.codon_at_og_position("4..6;8..10;11..16", "1..3;4..6;7..12", "ATGTAGCATTAR", 16, "a")  --> "TAa"
```

#### Original Position to AA or CDS Position

```sql
og_to_aa_position(STRING query_nt_coords, STRING cds_nt_coords, BIGINT original_position)   -> INT
og_to_cds_position(STRING query_nt_coords, STRING cds_nt_coords, BIGINT original_position)  -> INT
```

**Purpose:** Takes coordinate mapping information provided by [DAIS-ribosome](https://hub.docker.com/r/cdcgov/dais-ribosome/tags), such as the  `query_nt_coords` and `cds_nt_coords` plus a position in the original coordinate space and maps to the **aligned** AA or CDS position for the `og_to_aa_position` or `og_to_cds_position` functions respectively. The original position might come from an *untrimmed* [IRMA](https://wonder.cdc.gov/amd/flu/irma/) variant position.

Empty strings and null values on any input returns `NULL`. In particular, if the mapped position is *before*, *after*, or *in-between* (insertions) alignment-space coordinates, they are considered out-of-bounds and the function will return `NULL`.

**Example:**

```sql
select udx.og_to_aa_position("4..6;8..10;11..16",  "1..3;4..6;7..12", 11)   --> 3
select udx.og_to_cds_position("4..6;8..10;11..16", "1..3;4..6;7..12", 11)   --> 7

-- Insertions are not mappable
select udx.og_to_cds_position("1..456;457..459;460..983", "31..486;486;487..1010", 458) --> NULL
-- This is fine
select udx.og_to_cds_position("1..456;457..459;460..983", "31..486;486;487..1010", 460) --> 487
```

#### Original Position to Degenerate Amino Acid Mutation

```sql
og_pos_to_aa3_mutation(STRING query_nt_coords, STRING cds_nt_coords, STRING cds_aln, BIGINT original_position, STRING consensus_allele, STRING minority_allele) -> STRING
```

**Purpose:** Takes coordinate mapping information provided by [DAIS-ribosome](https://hub.docker.com/r/cdcgov/dais-ribosome/tags), such as the  `query_nt_coords` and `cds_nt_coords`, the `cds_aln` (aligned coding sequence), a position in the original coordinate space, and a pair of alleles. Under the hood, the function obtains the codon, mutates it at the provided CDS position using each allele, and then translates each mutated codon using the [degenerate amino acid](#to-amino-acids-with-degeneracy-up-to-3) translation function. The output is of the form `AA1#AA2` where the numeric position is the **aligned** amino acid position.

The original position might come from an *untrimmed* [IRMA](https://wonder.cdc.gov/amd/flu/irma/) variant position. Likewise, the alleles might come from IRMA's consensus and variant allele provided by its [variant table](https://wonder.cdc.gov/amd/flu/irma/output/A_MP-variants.html).

Empty strings and null values on any input returns `NULL`. In particular, if the mapped position is *before*, *after*, or *in-between* (insertions) alignment-space coordinates, they are considered out-of-bounds and the function will return `NULL`.

**Example:**

```sql
-- Simple amino acid mutations
select udx.og_pos_to_aa3_mutation("4..6", "1..3", "ATR", 6, "G", "A")                                   --> "M1I"
select udx.og_pos_to_aa3_mutation("4..6;8..10", "1..3;4..6", "ATGTYG", 9, "C", "T")                     --> "S2L"
select udx.og_pos_to_aa3_mutation("4..6;8..10;11..16", "1..3;4..6;7..12", "ATGSCGCATTYN", 8, "C", "G")  --> "P2A"

-- Multiple products are allowed on either side
select udx.og_pos_to_aa3_mutation("4..6;8..10;11..16", "1..3;4..6;7..12", "ATGTAGCMWTYN", 12, "C", "A") --> "P3H/Q"
select udx.og_pos_to_aa3_mutation("4..6;8..10;11..16", "1..3;4..6;7..12", "ATGTAGCATTYK", 16, "g", "t") --> "L/S4F/S"
```

## Aggregate Function Descriptions

Aggregate functions take many values within a group and return a single value per group.

---

### Bitwise Sum

```sql
bitwise_sum(BIGINT values) -> BIGINT
```

**Purpose:** Returns the [bitwise OR](https://en.wikipedia.org/wiki/Bitwise_operation#OR) of all values. Available on the CDP cluster

### Kurtosis

```sql
kurtosis(<INT or DOUBLE> values) -> DOUBLE
```

**Purpose:** Compute the [4th moment or kurtosis](https://en.wikipedia.org/wiki/Kurtosis) using [a one-pass formula](https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics).

### LogFold Titer Distribution Agreement

```sql
logfold_agreement(INT values) -> DOUBLE
```

**Purpose:** Performs a test for modality called [agreement](https://en.wikipedia.org/wiki/Multimodal_distribution#van_der_Eijk's_A) (uniform: 0, unimodal: +1, bimodal: -1). The logfold titer values are allowed to take -16 to 16 but the categories for the calculation will be bounded to 10 for the distribution.

### Skewness

```sql
skewness(<INT or DOUBLE> values) -> DOUBLE
```

**Purpose:** Compute the [3rd moment or skewness](https://en.wikipedia.org/wiki/Skewness) using [a one-pass formula](https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics).

### Entropy

```sql
-- Aggregate Function
alphanumeric_entropy(STRING s) -> DOUBLE
nt_entropy(STRING s) -> DOUBLE
aa_entropy(STRING s) -> DOUBLE
codon_entropy(STRING s) -> DOUBLE

-- Scalar Function
alnum_entropy(STRING s) -> DOUBLE

```

**Purpose:** The aggregate function `alphanumeric_entropy` slurps all strings within the `GROUP BY` column and calculates the alphanumeric character entropy over the total group-wise data. Likewise, the aggregate functions `nt_entropy`, `aa_entropy`, and `codon_entropy` calculate the entropy based on group data where the entropy is being calculated over canonical nucleotides, amino acids (1-letter code, no `X` or `*`), or by codon (3 canonical nucleotides) respectively. Please note that these function operate over the first base, residue or codon and do not slurp every value in each sequence. As such they are useful in combination with extraction and slicing functions.

The scalar function `alnum_entropy` is equivalent to `alphanumeric_entropy` but only performs its operation on a single value.

# Acknowledgments

We'd like to thank contributors (in alphabetical order) who have suggested features, identified bugs, or submitted merge requests:

- Norman Hassell (CDC)
- Kristine Lacek (CDC)
- Clint Paden (CDC)

# Notices

**General disclaimer** This repository was created for use by CDC programs to collaborate on public health related projects in support of the [CDC mission](https://www.cdc.gov/about/cdc/index.html).  GitHub is not hosted by the CDC, but is a third party website used by CDC and its partners to share information and collaborate on software. CDC use of GitHub does not imply an endorsement of any one particular service, product, or enterprise.  

## Public Domain Standard Notice

This repository constitutes a work of the United States Government and is not subject to domestic copyright protection under 17 USC § 105. This repository is in the public domain within the United States, and copyright and related rights in the work worldwide are waived through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/).  All contributions to this repository will be released under the CC0 dedication.  By submitting a pull request you are agreeing to comply with this waiver of copyright interest.

## License Standard Notice

The repository utilizes code licensed under the terms of the Apache Software License and therefore is licensed under ASL v2 or later. This source code in this repository is free: you can redistribute it and/or modify it under the terms of the Apache Software License version 2, or (at your option) any later version. This source code in this repository is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the Apache Software License for more details. You should have received a copy of the Apache Software License along with this program. If not, see: <http://www.apache.org/licenses/LICENSE-2.0.html>. The source code forked from other open source projects will inherit its license.

## Privacy Standard Notice

This repository contains only non-sensitive, publicly available data and information. All material and community participation is covered by the [Disclaimer](DISCLAIMER.md). For more information about CDC's privacy policy, please visit <http://www.cdc.gov/other/privacy.html>.

## Contributing Standard Notice

Anyone is encouraged to contribute to the repository by [forking](https://help.github.com/articles/fork-a-repo) and submitting a pull request. (If you are new to GitHub, you might start with a [basic tutorial](https://help.github.com/articles/set-up-git).) By contributing to this project, you grant a world-wide, royalty-free, perpetual, irrevocable, non-exclusive, transferable license to all users under the terms of the [Apache Software License v2](http://www.apache.org/licenses/LICENSE-2.0.html) or later.

All comments, messages, pull requests, and other submissions received through CDC including this GitHub page may be subject to applicable federal law, including but not limited to the Federal Records Act, and may be archived. Learn more at [http://www.cdc.gov/other/privacy.html](http://www.cdc.gov/other/privacy.html).

## Records Management Standard Notice

This repository is not a source of government records, but is a copy to increase collaboration and collaborative potential. All government records will be published through the [CDC web site](http://www.cdc.gov).

## Additional Standard Notices

Please refer to [CDC's Template Repository](https://github.com/CDCgov/template) for more information about [contributing to this repository](https://github.com/CDCgov/template/blob/main/CONTRIBUTING.md), [public domain notices and disclaimers](https://github.com/CDCgov/template/blob/main/DISCLAIMER.md), and [code of conduct](https://github.com/CDCgov/template/blob/main/code-of-conduct.md).
