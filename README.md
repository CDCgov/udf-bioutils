# User-defined bioinformatics utilities for Impala SQL

Functions are created in the **udx** schema and can be shown via `use udx; show functions;` commands. Tables in **udx** show function input arguments and expected return values (*outcome*). The tables are named after the function, but with a prefix for user-defined functions (_udf_) or user-defined aggregate functions (_uda_) and with a numeric suffix if the function has been [overloaded](https://en.wikipedia.org/wiki/Function_overloading). For convenience, I have written SQL files to re-create the function bindings in the event the library has been updated ([udx_refresh.sql](https://git.biotech.cdc.gov/vfn4/udf-bioutils/blob/master/udx_refresh.sql)), to perform regression testing ([udx_tests.sql](https://git.biotech.cdc.gov/vfn4/udf-bioutils/blob/master/udx_tests.sql)), or to ensure functions exist after a server restart ([udx_ensure.sql](https://git.biotech.cdc.gov/vfn4/udf-bioutils/blob/master/udx_ensure.sql), not necessary after CDH6+). If the behavior of the function changes over time, one can re-create the example tables using the [udx_create_tables.sql](https://git.biotech.cdc.gov/vfn4/udf-bioutils/blob/master/udx_create_table.sql) file.

For further reading related to function development:
* [Impala User-Defined Functions](https://www.cloudera.com/documentation/enterprise/6/6.0/topics/impala_udf.html)
* [Impala UDF Samples](https://github.com/cloudera/impala-udf-samples)
* [Impala GitHub mirror](https://github.com/apache/impala)


## Function Descriptions
<pre><b>contains_sym(<i>string str1, string str2</i>)</b></pre>
<p> **Return type:** `boolean`
<p> **Purpose:** Returns true if `str1` is a substring of `str2` or vice-versa. If *just one* argument is an empty string, the function returns false. A `NULL` in any argument will return a null value.

<br />

<pre><b>contains_element(<i>string str, string list, string delim</i>)</b></pre>
<p> **Return type:** `boolean`
<p> **Purpose:** Return true if `str` contains *any* element in `list` delimited by `delim` as a substring. The delimiter may be a sequence of characters, but if it is empty the list is split by character. A `NULL` in any argument will return a null value.

<br />

<pre><b>is_element(<i>string str, string list, string delim</i>)</b></pre>
<p> **Return type:** `boolean`
<p> **Purpose:** Returns true if `str` is equal to any element of `list` delimited by `delim`. The delimiter may be a sequence of characters, but if it is empty the list is split by character.

<br />

<pre><b>hamming_distance(<i>string sequence1, string sequence2[, string pairwise_deletion_set]</i>)</b></pre>
<p> **Return type:** `int`
<p> **Purpose:** Counts the [number of differences](https://en.wikipedia.org/wiki/Hamming_distance) between two sequences (though any strings may be used). If one sequence is longer than the other, the extra characters are discarded from the calculation. By default, any pair of characters with a `.` as an element is ignored by the calculation. In *DAIS*, the `.` character is used for missing data. Optionally, one may explicitly add a pairwise deletion character set. If any pair of characters contain any of the characters in the argument, that position is ignored from the calculation. If any argument is `NULL` or either sequence argument is an empty string, a null value is returned. If the optional `pairwise_deletion_set` argument is an empty string, no pairwise deletion is performed.

<br />

<pre><b>mutation_list(<i>string sequence1, string sequence2</i>)</b>, <b>mutation_list_nt(<i>string sequence1, string sequence2</i>)</b></pre>
<p> **Return type:** `string`
<p> **Purpose:** Returns a list of mutations from `sequence1` to `sequence2`, delimited by a comma and space. For example: `A2G, T160K, G340R`. If any argument is `NULL` or empty, a null value is returned. The function `mutation_list` returns differences and may be used for nucleotide, amino acid, or any other sequence. Alternatively, the function `mutation_list_nt` is *suitable only for nucleotide sequences* and ignores resolvable differences involving ambiguous nucleotides (e.g., "R2G" would not be listed).

<br />

<pre><b>reverse_complement(<i>string nucleotides</i>)</b></pre>
<p> **Return type:** `string`
<p> **Purpose:** Returns the reverse complement nucleotide sequence. Case is preserved and ambiguous nucleotides are mapped. Non-nucleotide characters are left as-is but in reverse order. Null values return `NULL` and an empty string remains empty.

<br />

<pre><b>sort_alleles(<i>string allele_or_mutation_list, string delim</i>)</b></b></pre>
<p> **Return type:** `string`
<p> **Purpose:** Returns a list of sorted mutations (e.g., `A2G, T160K`) or alleles (e.g., `2G 160K`) where the list delimiter `delim` is used both for splitting elements and for the output of the sorted list. Alleles and mutations are sorted first by their integer position, unlike other list sorting which looks at the element as a string. If any argument is `NULL` a null value is returned.