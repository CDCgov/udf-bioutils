# User-defined bioinformatics utilities for Impala SQL

- [User-defined bioinformatics utilities for Impala SQL](#user-defined-bioinformatics-utilities-for-impala-sql)
  - [Summary](#summary)
  - [License and Usage](#license-and-usage)
  - [Scalar Function Descriptions](#scalar-function-descriptions)
    - [aa\_std(STRING sequence), nt\_std(STRING sequence)](#aa_stdstring-sequence-nt_stdstring-sequence)
    - [any\_instr(STRING haystack, STRING needles)](#any_instrstring-haystack-string-needles)
    - [ci\_t(DOUBLE confidence, BIGINT sample\_size, DOUBLE standard\_deviation \[, BOOLEAN is\_two\_sided\])](#ci_tdouble-confidence-bigint-sample_size-double-standard_deviation--boolean-is_two_sided)
    - [complete\_date(STRING date)](#complete_datestring-date)
    - [contains\_sym(STRING str1, STRING str2)](#contains_symstring-str1-string-str2)
    - [contains\_element(STRING str, STRING list, STRING delim)](#contains_elementstring-str-string-list-string-delim)
    - [is\_element(STRING str, STRING list, STRING delim)](#is_elementstring-str-string-list-string-delim)
    - [hamming\_distance(STRING sequence1, STRING sequence2\[, STRING pairwise\_deletion\_set\])nt\_distance(STRING sequence1, STRING sequence2)](#hamming_distancestring-sequence1-string-sequence2-string-pairwise_deletion_setnt_distancestring-sequence1-string-sequence2)
    - [longest\_deletion(STRING sequence), deletion\_events(STRING sequence)](#longest_deletionstring-sequence-deletion_eventsstring-sequence)
    - [md5(STRING field1 \[,STRING field2, ...\])](#md5string-field1-string-field2-)
    - [mutation\_list(STRING sequence1, STRING sequence2 \[, STRING range\])mutation\_list\_gly(STRING sequence1, STRING sequence2)mutation\_list\_nt(STRING sequence1, STRING sequence2)mutation\_list\_pds(STRING sequence1, STRING sequence2, STRING pairwise\_deletion\_set)](#mutation_liststring-sequence1-string-sequence2--string-rangemutation_list_glystring-sequence1-string-sequence2mutation_list_ntstring-sequence1-string-sequence2mutation_list_pdsstring-sequence1-string-sequence2-string-pairwise_deletion_set)
    - [pcd(STRING sequence1, STRING sequence2)](#pcdstring-sequence1-string-sequence2)
    - [pcd\_list(STRING sequence1, STRING sequence2)](#pcd_liststring-sequence1-string-sequence2)
    - [quantile\_t(DOUBLE confidence, BIGINT samples, BOOLEAN is\_two\_sided)](#quantile_tdouble-confidence-bigint-samples-boolean-is_two_sided)
    - [range\_from\_list(STRING list, STRING delim)](#range_from_liststring-list-string-delim)
    - [reverse\_complement(STRING nucleotides)](#reverse_complementstring-nucleotides)
    - [sort\_alleles(STRING allele\_or\_mutation\_list, STRING delim)](#sort_allelesstring-allele_or_mutation_list-string-delim)
    - [sort\_list(STRING list, STRING delim)sort\_list\_unique(STRING list, STRING delim)sort\_list\_set(STRING list, STRING delim\_set, STRING output\_delim)](#sort_liststring-list-string-delimsort_list_uniquestring-list-string-delimsort_list_setstring-list-string-delim_set-string-output_delim)
    - [substr\_range(STRING str, STRING range\_coords)](#substr_rangestring-str-string-range_coords)
    - [to\_aa(STRING nucleotides\[, STRING replacement\_nucleotides, int starting\_position\])](#to_aastring-nucleotides-string-replacement_nucleotides-int-starting_position)
    - [to\_epiweek(\<STRING|timestamp\> date \[, BOOLEAN year\_format\])](#to_epiweekstringtimestamp-date--boolean-year_format)
    - [variant\_hash(STRING residues), nt\_id(STRING nucleotides)](#variant_hashstring-residues-nt_idstring-nucleotides)
  - [Aggregate Function Descriptions](#aggregate-function-descriptions)
    - [BITWISE\_SUM(BIGINT values)](#bitwise_sumbigint-values)
    - [KURTOSIS(\<INT|DOUBLE\> values)](#kurtosisintdouble-values)
    - [LOGFOLD\_AGREEMENT(INT values)](#logfold_agreementint-values)
    - [SKEWNESS(\<INT|DOUBLE\> values)](#skewnessintdouble-values)

## Summary

Functions may be created in a schema such as **udx**. To show persistent function one would then write: `use udx; show functions;`.
Tables in **udx** show function input arguments and expected return values (*outcome*).
The provided table SQL code are named after the function, but with a prefix for user-defined functions (*udf*) or user-defined aggregate functions (*uda*) and with a suffix indicating argument/return types to help distinguish when the function has been [overloaded](https://en.wikipedia.org/wiki/Function_overloading). Some function creation code for SQL, like ([udx_ensure.sql](https://git.biotech.cdc.gov/vfn4/udf-bioutils/blob/master/udx_ensure.sql), has been provided.

For further reading related to function development:

- [Impala User-Defined Functions](https://docs.cloudera.com/cdp-private-cloud-base/latest/impala-sql-reference/topics/impala-udf.html)
- [Impala UDF Samples](https://github.com/cloudera/impala-udf-samples)
- [Impala GitHub mirror](https://github.com/apache/impala)

## License and Usage

This repository follows Cloudera's licensing for Apache Impala and is licensed under Apache License, Version 2.0 (see: <http://www.apache.org/licenses/LICENSE-2.0>). All code is dedicated to the Public Domain with attributions appreciated (Samuel S. Shepard, CDC) except where Cloudera copyright is provided or indicated in the file (e.g., `common.h`).

## Scalar Function Descriptions

Scalar functions take any number of values from a **single** row and return a single value.

---

### <pre>aa_std(STRING sequence), nt_std(STRING sequence)</pre>

**Return type:** STRING  
**Purpose:** Standardizes the assumed amino acid or nucleotide `sequence` respectively by removing extraneous characters (`\n\r\t :.-`) and switching to uppercase. The nucleotide version also removes `~`, which is interpreted by [DAIS-ribosome](https://git.biotech.cdc.gov/flu-informatics/dais-ribosome) as a partial codon for amino acid sequences. The same sequence cleaning is done by the ID generating functions [variant_hash and nt_id](#variant_hashstring-residues-nt_idstring-nucleotides). Returns `NULL` if the input is null or an empty string is provided.
<br /><br />

### <pre>any_instr(STRING haystack, STRING needles)</pre>

**Return type:** `BOOLEAN`  
**Purpose:** Checks if any of the *characters* in the `needles` STRING is in the `haystack` STRING. Will return `NULL` if either argument is null. Note that if both `needles` and `haystack` has an empty string, the function always returns `TRUE`.  

&rarr; *See also the Impala native function [INSTR](https://docs.cloudera.com/cdp-private-cloud-base/7.1.8/impala-sql-reference/topics/impala-string-functions.html#string_functions__instr).*
<br /><br />

### <pre>ci_t(DOUBLE confidence, BIGINT sample_size, DOUBLE standard_deviation [, BOOLEAN is_two_sided])</pre>

**Return type:** `DOUBLE`  
**Purpose:** Takes the confidence level (e.g., 0.99), `sample_size`,sample `standard_deviation` and an optional boolean (default is two-sided) for sided-ness in order to generate the [confidence interval](https://en.wikipedia.org/wiki/Confidence_interval#Example) for the [Student's T-distribution](https://en.wikipedia.org/wiki/Student%27s_t-distribution). In other words, the value returned is +/- around the point estimate.
<br /><br />

### <pre>complete_date(STRING date)</pre>

**Return type:** `STRING`  
**Purpose:** Parses STRING dates with delimiters `.`,`/`, and `-`; adds missing month or day component when applicable (as the first of either). A `NULL` in the argument will return a null value.
<br /><br />

### <pre>contains_sym(STRING str1, STRING str2)</pre>

**Return type:** `BOOLEAN`  
**Purpose:** Returns true if `str1` is a substring of `str2` or *vice-versa*. If *just one* argument is an empty STRING, the function returns false. A `NULL` in any argument will return a null value.
<br /><br />

### <pre>contains_element(STRING str, STRING list, STRING delim)</pre>

**Return type:** `BOOLEAN`  
**Purpose:** Return TRUE if `str` contains *any* element in `list` delimited by `delim` as a **substring**. The delimiter may be a sequence of characters, but if it is empty the list is split by character. A `NULL` in any argument will return a null value.
<br /><br />

### <pre>is_element(STRING str, STRING list, STRING delim)</pre>

**Return type:** `BOOLEAN`  
**Purpose:** Returns true if `str` is equal to any element of `list` delimited by `delim`. The delimiter may be a sequence of characters, but if it is empty the list is split by character.  

&rarr; *See also the Impala native function [FIND_IN_SET](https://docs.cloudera.com/cdp-private-cloud-base/7.1.8/impala-sql-reference/topics/impala-string-functions.html#string_functions__find_in_set).*
<br /><br />

### <pre>hamming_distance(STRING sequence1, STRING sequence2[, STRING pairwise_deletion_set])<br />nt_distance(STRING sequence1, STRING sequence2)</pre>

**Return type:** `INT`  
**Purpose:** Counts the [number of differences](https://en.wikipedia.org/wiki/Hamming_distance) between two sequences (though any STRINGs may be used).
If one sequence is longer than the other, the extra characters are discarded from the calculation. By default, any pair of characters with a `.` as an element is ignored by the calculation. In *DAIS*, the `.` character is used for missing data. Optionally, one may explicitly add a pairwise deletion character set. If any pair of characters contain any of the characters in the argument, that position is ignored from the calculation. If any argument is `NULL` or either sequence argument is an empty STRING, a null value is returned. If the optional `pairwise_deletion_set` argument is an empty STRING, no pairwise deletion is performed. The `nt_distance` function is the same as the default version of `hamming_distance` but does not count ambiguated differences. For example, A ≠ T but A = R.  

&rarr; *See also the Impala native function [JARO_DISTANCE](https://docs.cloudera.com/cdp-private-cloud-base/7.1.8/impala-sql-reference/topics/impala-string-functions.html?#string_functions__jaro_distance).*
<br /><br />

### <pre>longest_deletion(STRING sequence), deletion_events(STRING sequence)</pre>

**Return type:** `INT`  
**Purpose:** Returns the length of the longest deletion in the `sequence` or the number of deletion events respectively. In either function, a *deletion event* must have upstream and downstream alphabetic character to the deletion span (using `-` for deletion characters only). The functions return `null` if the sequence is null and they return 0 if the empty STRING is used.
<br /><br />

### <pre>md5(STRING field1 [,STRING field2, ...])</pre>

**Return type:** `STRING`  
**Purpose:** This function takes a variable number of STRING fields (i.e., "[variadic](https://en.wikipedia.org/wiki/Variadic_function)") and returns a 32 character [hexadecimal](https://en.wikipedia.org/wiki/Hexadecimal#Binary_conversion) from the [md5](https://en.wikipedia.org/wiki/MD5) hash.
Any null value in an argument or all empty STRINGs returns `NULL`. Note: fields are concatenated with the [bell character](https://en.wikipedia.org/wiki/Bell_character) as the delimiter before hashing in order to preserve field boundaries.  

&rarr; *See also the Impala native functions [MURMUR_HASH](https://docs.cloudera.com/cdp-private-cloud-base/7.1.8/impala-sql-reference/topics/impala-math-functions.html#math_functions__murmur_hash), [FNV_HASH](https://docs.cloudera.com/cdp-private-cloud-base/7.1.8/impala-sql-reference/topics/impala-math-functions.html#math_functions__fnv_hash), [SHA1/SHA2](https://docs.cloudera.com/cdp-private-cloud-base/7.1.8/impala-sql-reference/topics/impala-hash-functions.html), and [HEX](https://docs.cloudera.com/cdp-private-cloud-base/7.1.8/impala-sql-reference/topics/impala-math-functions.html#math_functions__hex).*
<br /><br />

### <pre>mutation_list(STRING sequence1, STRING sequence2 [, STRING range])<br />mutation_list_gly(STRING sequence1, STRING sequence2)<br />mutation_list_nt(STRING sequence1, STRING sequence2)<br />mutation_list_pds(STRING sequence1, STRING sequence2, STRING pairwise_deletion_set)</pre>

**Return type:** `STRING`  
**Purpose:** Expects **aligned** biological sequences and returns a list of mutations from `sequence1` to `sequence2`, delimited by a comma + space.  If the `range` argument is included, only those sites will be compared (see the description of `range_coords` in `substr_range`). For example: `A2G, T160K, G340R`. If any argument is `NULL` or empty, a null value is returned. The function `mutation_list` returns differences and may be used for nucleotide, amino acid, or any other sequence. There are some alternative variants to the function:

- The function `mutation_list_gly` is similar to the vanilla `mutation_list` function but annotates the addition or loss of an [N-linked glycosylation site](https://en.wikipedia.org/wiki/N-linked_glycosylation#Transfer_of_glycan_to_protein) due to substitution (from `sequence1` to `sequence2`).
- The function `mutation_list_nt` is *suitable only for nucleotide sequences* and ignores resolvable differences involving ambiguous nucleotides (e.g., "R2G" would not be listed).
- The function `mutation_list_pds` also contains an explicit argument for a pairwise deletion character set. If any pair of characters contain any of the characters in the argument, that position is ignored from the calculation.
<br /><br />

### <pre>pcd(STRING sequence1, STRING sequence2)</pre>

**Return type:** `DOUBLE`  
**Purpose:**  Calculates the "physiochemical distance" (PCD) between two **aligned** amino acid sequences. First the function takes the [Euclidean distance](https://en.wikipedia.org/wiki/Euclidean_distance) of physiochemical factors (see [*Atchley et al.*'s protein sequence analysis](https://www.pnas.org/doi/full/10.1073/pnas.0408677102)) corresponding to the compared residues at each site. After that the per-site PCD values are averaged over all *valid* sites, or sites where both alleles contain normal amino residues. The function ignores sequence case and does not count `X` as a valid comparison. However, comparisons to deletion states (`-`) are calculated versus the 0-vector. If either argument is `NULL` or `""` then a null is returned. Anecdotally, S. Shepard has observed that the distances correlate strongly with [the JTT model](https://pubmed.ncbi.nlm.nih.gov/1633570/).
<br /><br />

### <pre>pcd_list(STRING sequence1, STRING sequence2)</pre>

**Return type:** `STRING`  
**Purpose:**  Calculates the per-site "physiochemical distance" (PCD) between two **aligned** amino acid sequences (see description for `pcd()`). String-encoded PCDs *for each site* are space-delimited, and the number of sites will be the shorter of the two sequences. If either argument is `NULL` or `""` then a null is returned.
<br /><br />

### <pre>quantile_t(DOUBLE confidence, BIGINT samples, BOOLEAN is_two_sided)</pre>

**Return type:** `DOUBLE`  
**Purpose:** Given some `confidence` level (e.g., 0.99), number of `samples` and a boolean for whether the [Student's T-distribution](https://en.wikipedia.org/wiki/Student%27s_t-distribution) is two-sided, estimate the [quantile function](https://en.wikipedia.org/wiki/Quantile_function).
<br /><br />

### <pre>range_from_list(STRING list, STRING delim)</pre>

**Return type:** `STRING`  
**Purpose:** Takes STRING `list` of integers separated by STRING `delim` (split by substring) and returns a STRING *range* in the format described in `substr_range` for the field `range_coords`. List elements will be added uniquely as a set. If a non-integer element is found or any argument is `NULL`, a null value is returned. If an empty `list` is given it is returned as-is; if an empty `delim` is given, the `list` is returned.
<br /><br />

### <pre>reverse_complement(STRING nucleotides)</pre>

**Return type:** `STRING`  
**Purpose:** Returns the reverse complement nucleotide sequence. Case is preserved and ambiguous nucleotides are mapped. Non-nucleotide characters are left as-is but in reverse order. Null values return `NULL` and an empty STRING remains empty.
<br /><br />

### <pre>sort_alleles(STRING allele_or_mutation_list, STRING delim)</pre>

**Return type:** `STRING`  
**Purpose:** Returns a list of sorted mutations (e.g., `A2G, T160K`) or alleles (e.g., `2G 160K`) where the list delimiter `delim` is used both for splitting elements and for the output of the sorted list. Alleles and mutations are sorted first by their integer position, unlike other list sorting which looks at the element as a STRING. If any argument is `NULL` a null value is returned.
<br /><br />

### <pre>sort_list(STRING list, STRING delim)<br />sort_list_unique(STRING list, STRING delim)<br />sort_list_set(STRING list, STRING delim_set, STRING output_delim)</pre>

**Return type:** `STRING`  
**Purpose:** Returns an alphabetically sorted list of elements in the STRING `list` delimited by `delim` or `delim_set`. The function `sort_list` interprets multi-character delimiters as a whole STRING while the function `sort_list_set` treats each character in the argument `delim_set` as a single-character delimiter (all are applied). The input and output delimiter for `sort_list` are taken to be the same while the output delimiter for `sort_list_set` is specified by `output_delim`. If any argument is `NULL` a null value is returned. The function variant `sort_list_unique` behaves exactly like `sort_list` but removes redundant elements.
<br /><br />

### <pre>substr_range(STRING str, STRING range_coords)</pre>

**Return type:** `STRING`  
**Purpose:** Returns the characters in `str` specified by `range_coords`.
All characters are concatenated as specified by `range_coords`. Ranges may be listed in forward and reverse, which affects output order, and are denoted by `#..#`.
Multiple ranges or single characters may be separated using a semi-colon or comma. For example: `10..1;12;15;20..25`. If any argument is `NULL` or an empty STRING, a null value is returned.  

&rarr; *See also the Impala native function [SUBSTR](https://docs.cloudera.com/cdp-private-cloud-base/7.1.8/impala-sql-reference/topics/impala-string-functions.html#string_functions__substr).*
<br /><br />

### <pre>to_aa(STRING nucleotides[, STRING replacement_nucleotides, int starting_position])</pre>

**Return type:** `STRING`  
**Purpose:** Translates a nucleotide sequence to an amino acid sequence starting at position 1 of argument `nucleotides` (including resolvable ambiguous codons). Unknown or partial codons are translated as `?`, mixed or partially gapped codons are translated as `~`, and deletions (`-`) or missing data (`.`) are compacted from 3 to 1 character. Residues are always written out in uppercase. *Optionally*, one may overwrite a portion of the nucleotide sequence prior to translation by providing `replacement_nucleotides` and a `starting_position`. Specifying out-of-range indices will append to the 5' or 3' end while specifying a replacement sequence larger than the original will result in the extra nucleotides being appended after in-range bases are overwritten. If any argument is `NULL` a null value is returned. If the `replacement_nucleotides` argument is an empty STRING, the `nucleotides` argument is translated as-is. On the other hand, if the `nucleotides` argument is an empty STRING but `replacement_nucleotides` is not, then `replacement_nucleotides` is translated and returned.
<br /><br />

### <pre>to_epiweek(&lt;STRING|timestamp&gt; date [, BOOLEAN year_format])</pre>

**Return type:** `INT`  
**Purpose:** Returns the [MMWR or EPI](https://wwwn.cdc.gov/nndss/document/MMWR_Week_overview.pdf) week of a formatted date STRING or timestamp. If any argument is null then `null` is returned. Invalid dates, including the empty STRING, will also return `null`.  If the `date` is a STRING it must be in *YYYY-MM-DD* format but may also be delimited using `.` or `/`. If the month or day are missing they are set to January or the 1st day of the month, respectively. Normally the output is an integer ranging from 1 up to 52 or 53. The *optional* `year_format` (defaults to `false`) adds the padded week to the year so that the output can be graphed on a contiguous timeline. Some examples using the year format would be: *202102* and *199853*.

### <pre>variant_hash(STRING residues), nt_id(STRING nucleotides)</pre>

**Return type:** `STRING`  
**Purpose:** Returns hashed identifiers for aligned or unaligned sequences. Case is ignored in the hash, as is whitespace, `:`, `-`, and `.`. The `nt_id` function also ignores `~`, which represents a translated partial codon in the `variant_hash`. The `variant_hash` is a 32 character [hexadecimal](https://en.wikipedia.org/wiki/Hexadecimal#Binary_conversion) from the [md5](https://en.wikipedia.org/wiki/MD5) hash while the `nt_id` is a 40 character hexadecimal from the [sha1](https://en.wikipedia.org/wiki/SHA-1) hash. Null values or empty STRING return `NULL`.
<br /><br />

## Aggregate Function Descriptions

Aggregate functions take many values within a group and return a single value per group.

---

### <pre>BITWISE_SUM(BIGINT values)</pre>

**Return type:** `BIGINT`  
**Purpose:** Returns the [bitwise OR](https://en.wikipedia.org/wiki/Bitwise_operation#OR) of all values. Available on the CDP cluster
<br /><br />

### <pre>KURTOSIS(&lt;INT|DOUBLE&gt; values)</pre>

**Return type:** `DOUBLE`  
**Purpose:** Compute the [4th moment or kurtosis](https://en.wikipedia.org/wiki/Kurtosis) using [a one-pass formula](https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics).
<br /><br />

### <pre>LOGFOLD_AGREEMENT(INT values)</pre>

**Return type:** `DOUBLE`  
**Purpose:** Performs a test for modality called [agreement](https://en.wikipedia.org/wiki/Multimodal_distribution#van_der_Eijk's_A) (uniform: 0, unimodal: +1, bimodal: -1). The logfold titer values are allowed to take -16 to 16 but the categories for the calculation will be bounded to 10 for the distribution.
<br /><br />

### <pre>SKEWNESS(&lt;INT|DOUBLE&gt; values)</pre>

**Return type:** `DOUBLE`  
**Purpose:** Compute the [3rd moment or skewness](https://en.wikipedia.org/wiki/Skewness) using [a one-pass formula](https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics).
<br /><br />
