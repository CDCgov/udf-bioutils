# User-defined bioinformatics utilities for Impala SQL

Functions are created in the **udx** schema and can be shown via `use udx; show functions;` commands. 
Tables in **udx** show function input arguments and expected return values (*outcome*). 
The tables are named after the function, but with a prefix for user-defined functions (_udf_) or user-defined aggregate functions (_uda_) and with a suffix indicating argument/return types to help distinguish when the function has been [overloaded](https://en.wikipedia.org/wiki/Function_overloading). For convenience, I have written SQL files to re-create the function bindings in the event the library has been updated ([udx_refresh.sql](https://git.biotech.cdc.gov/vfn4/udf-bioutils/blob/master/udx_refresh.sql)), to perform regression testing ([udx_tests.sql](https://git.biotech.cdc.gov/vfn4/udf-bioutils/blob/master/udx_tests.sql)), or to ensure functions exist after a server restart ([udx_ensure.sql](https://git.biotech.cdc.gov/vfn4/udf-bioutils/blob/master/udx_ensure.sql), not necessary after CDH6+). 
If the behavior of the function changes over time, one can re-create the example tables using the [udx_table_create.sql](https://git.biotech.cdc.gov/vfn4/udf-bioutils/blob/master/udx_table_create.sql) file. *Please feel free to submit bugs and feature requests as issues within GitLab.*

For further reading related to function development:
* [Impala User-Defined Functions](https://www.cloudera.com/documentation/enterprise/6/6.0/topics/impala_udf.html)
* [Impala UDF Samples](https://github.com/cloudera/impala-udf-samples)
* [Impala GitHub mirror](https://github.com/apache/impala)


## Function Descriptions
<pre><b>complete_date(<i>string date</i>)</b></pre>
**Return type:** `string`<br />
**Purpose:** Parses string dates with delimiters `.`,`/`, and `-`; adds missing month or day component when applicable (as the first of either). A `NULL` in the argument will return a null value.<br />

<br />

<pre><b>contains_sym(<i>string str1, string str2</i>)</b></pre>
**Return type:** `boolean`<br />
**Purpose:** Returns true if `str1` is a substring of `str2` or vice-versa. If *just one* argument is an empty string, the function returns false. A `NULL` in any argument will return a null value.<br />

<br />

<pre><b>contains_element(<i>string str, string list, string delim</i>)</b></pre>
**Return type:** `boolean`<br />
**Purpose:** Return true if `str` contains *any* element in `list` delimited by `delim` as a substring. The delimiter may be a sequence of characters, but if it is empty the list is split by character. A `NULL` in any argument will return a null value.<br />

<br />

<pre><b>is_element(<i>string str, string list, string delim</i>)</b></pre>
**Return type:** `boolean`<br />
**Purpose:** Returns true if `str` is equal to any element of `list` delimited by `delim`. The delimiter may be a sequence of characters, but if it is empty the list is split by character.<br />

<br />

<pre><b>hamming_distance(<i>string sequence1, string sequence2[, string pairwise_deletion_set]</i>)</b>, <b>nt_distance(<i>string sequence1, string sequence2</i>)</b></pre>
**Return type:** `int`<br />
**Purpose:** Counts the [number of differences](https://en.wikipedia.org/wiki/Hamming_distance) between two sequences (though any strings may be used). 
If one sequence is longer than the other, the extra characters are discarded from the calculation. 
By default, any pair of characters with a `.` as an element is ignored by the calculation. 
In *DAIS*, the `.` character is used for missing data. Optionally, one may explicitly add a pairwise deletion character set. 
If any pair of characters contain any of the characters in the argument, that position is ignored from the calculation. 
If any argument is `NULL` or either sequence argument is an empty string, a null value is returned. 
If the optional `pairwise_deletion_set` argument is an empty string, no pairwise deletion is performed.
The `nt_distance` function is the same as the default version of `hamming_distance` but does not count ambiguated differences. For example, A ≠ T but A = R.
<br />

<br />

<pre><b>mutation_list(<i>string sequence1, string sequence2 [, string range]</i>)</b>, <b>mutation_list_nt(<i>string sequence1, string sequence2</i>)</b></pre>
**Return type:** `string`<br />
**Purpose:** Returns a list of mutations from `sequence1` to `sequence2`, delimited by a comma and space. 
If the `range` argument is included, only those sites will be compared (see the description of `range_coords` in `substr_range`).
For example: `A2G, T160K, G340R`. If any argument is `NULL` or empty, a null value is returned. 
The function `mutation_list` returns differences and may be used for nucleotide, amino acid, or any other sequence. 
Alternatively, the function `mutation_list_nt` is *suitable only for nucleotide sequences* and ignores resolvable differences involving ambiguous nucleotides (e.g., "R2G" would not be listed).<br />

<br />

<pre><b>reverse_complement(<i>string nucleotides</i>)</b></pre>
**Return type:** `string`<br />
**Purpose:** Returns the reverse complement nucleotide sequence. Case is preserved and ambiguous nucleotides are mapped. Non-nucleotide characters are left as-is but in reverse order. Null values return `NULL` and an empty string remains empty.<br />

<br />

<pre><b>sort_alleles(<i>string allele_or_mutation_list, string delim</i>)</b></b></pre>
**Return type:** `string`<br />
**Purpose:** Returns a list of sorted mutations (e.g., `A2G, T160K`) or alleles (e.g., `2G 160K`) where the list delimiter `delim` is used both for splitting elements and for the output of the sorted list. Alleles and mutations are sorted first by their integer position, unlike other list sorting which looks at the element as a string. If any argument is `NULL` a null value is returned.<br />

<br />

<pre><b>sort_list(<i>string list, string delim</i>)</b>, <b>sort_list_unique(<i>string L, string D</i>)</b>, <b>sort_list_set(<i>string list, string delim_set, string output_delim</i>)</b></pre>
**Return type:** `string`<br />
**Purpose:** Returns an alphabetically sorted list of elements in the string `list` delimited by `delim` or `delim_set`. The function `sort_list` interprets multi-character delimiters as a whole string while the function `sort_list_set` treats each character in the argument `delim_set` as a single-character delimiter (all are applied). The input and output delimiter for `sort_list` are taken to be the same while the output delimiter for `sort_list_set` is specified by `output_delim`. If any argument is `NULL` a null value is returned. The function variant `sort_list_unique` behaves exactly like `sort_list` but removes redundant elements.<br />


<br />

<pre><b>substr_range(<i>string str, string range_coords</i>)</b></b></pre>
**Return type:** `string`<br />
**Purpose:** Returns the characters in `str` specified by `range_coords`. 
All characters are concatenated as specified by `range_coords`. 
Ranges may be listed in forward and reverse, which affects output order, and are denoted by `#..#`. 
Multiple ranges or single characters may be separated using a semi-colon or comma. 
For example: `10..1;12;15;20..25`. 
If any argument is `NULL` or an empty string, a null value is returned.<br />

<br />

<pre><b>range_from_list(<i>string list, string delim</i>)</b></b></pre>
**Return type:** `string`<br />
**Purpose:** Takes string `list` of integers separated by string `delim` (split by substring) and returns a string *range* in the format described in `substr_range` for the field `range_coords`.
List elements will be added uniquely as a set. 
If a non-integer element is found or any argument is `NULL`, a null value is returned.
If an empty `list` is given it is returned as-is; if an empty `delim` is given, the `list` is returned.
<br />

<br />

<pre><b>to_aa(<i>string nucleotides[, string replacement_nucleotides, int starting_position]</i>)</b></pre>
**Return type:** `string`<br />
**Purpose:** Translates a nucleotide sequence to an amino acid sequence starting at position 1 of argument `nucleotides` (including resolvable ambiguous codons). Unknown or partial codons are translated as `?`, mixed or partially gapped codons are translated as `~`, and deletions (`-`) or missing data (`.`) are compacted from 3 to 1 character. Residues are always written out in uppercase. *Optionally*, one may overwrite a portion of the nucleotide sequence prior to translation by providing `replacement_nucleotides` and a `starting_position`. Specifying out-of-range indices will append to the 5' or 3' end while specifying a replacement sequence larger than the original will result in the extra nucleotides being appended after in-range bases are overwritten. If any argument is `NULL` a null value is returned. If the `replacement_nucleotides` argument is an empty string, the `nucleotides` argument is translated as-is. On the other hand, if the `nucleotides` argument is an empty string but `replacement_nucleotides` is not, then `replacement_nucleotides` is translated and returned.<br />

<br />

<pre><b>variant_hash(<i>string residues</i>)</b>, <b>nt_id(<i>string nucleotides</i>)</b></pre>
**Return type:** `string`<br />
**Purpose:** Returns hashed identifiers for aligned or unaligned sequences. 
Case is ignored in the hash, as are spaces, `:`, `-`, and `.`; `nt_id` also ignores `~`, which represents a translated partial codon in the `variant_hash`. 
The `variant_hash` is a 32 character [hexadecimal](https://en.wikipedia.org/wiki/Hexadecimal#Binary_conversion) from the [md5](https://en.wikipedia.org/wiki/MD5) hash while the `nt_id` is a 40 character hexadecimal from the [sha1](https://en.wikipedia.org/wiki/SHA-1) hash.
Null values or empty string return `NULL`.<br />

<br />

<pre><b>md5(<i>string field1</i> [,string field2, ...]</i>)</b></b></pre>
**Return type:** `string`<br />
**Purpose:** This function takes a variable number of string fields (i.e., "[variadic](https://en.wikipedia.org/wiki/Variadic_function)") and returns a 32 character [hexadecimal](https://en.wikipedia.org/wiki/Hexadecimal#Binary_conversion) from the [md5](https://en.wikipedia.org/wiki/MD5) hash.
Any null value in an argument or all empty strings returns `NULL`. Note: fields are concatenated with the [bell character](https://en.wikipedia.org/wiki/Bell_character) as the delimiter before hashing in order to preserve field boundaries.<br />

<br />

<pre><b>longest_deletion(<i>string sequence</i>), deletion_events(<i>string sequence</i>)</b></pre>
**Return type:** `int`<br />
**Purpose:** Returns the length of the longest deletion in the `sequence` or the number of deletion events respectively. 
In either function, a *deletion event* must have upstream and downstream alphabetic character to the deletion span (using `-` for deletion characters only). 
The functions return `null` if the sequence is null and they return 0 if the empty string is used.<br />

<br />

<pre><b>to_epiweek(<i>string date</i> [, <i>boolean year_format</i>])</b></pre>
**Return type:** `int`<br />
**Purpose:** Returns the [MMWR or EPI](https://wwwn.cdc.gov/nndss/document/MMWR_Week_overview.pdf) week of a formatted date string. If any argument is null then `null` is returned. Invalid dates, including the empty string, will also return `null`.  The `date` string is in *YYYY-MM-DD* format but may also be delimited using `.` or `/`. If the month or day are missing they are set to January or the 1st day of the month, respectively. Normally the output is an integer ranging from 1 up to 52 or 53. The *optional* `year_format` (defaults to `false`) adds the padded week to the year so that the output can be graphed on a contiguous timeline. Some examples using the year format would be: *202102* and *199853*.
<br />
