# User-defined bioinformatics utilities for Impala SQL

Functions are created in the `udx` schema and can be shown via `use udx; show functions;` commands. Tables in `udx` show function input arguments and expected return values (outcomes). The table are named after the function, but with  prefix for user-defined functions (_udf_) or user-defined aggregate functions (_uda_) and with a numeric suffix if the function has been overloaded. SQL is attached to re-create the function bindings after the library has been updated (`udx_refresh.sql`), to perform regression testing (`udx_tests.sql`), or to ensure functions exist after a server restart (`udx_ensure.sql`, not necessary after CDH6+). If the behavior of the function changes over time, one can recreate the examle tables using the `udx_create_tables.sql` file.

For further reading:
* [Impala User-Defined Functions](https://www.cloudera.com/documentation/enterprise/6/6.0/topics/impala_udf.html)
* [Impala UDF Samples](https://github.com/cloudera/impala-udf-samples)
* [Impala GitHub mirror](https://github.com/apache/impala)

---

## Function Descriptions


<pre><b>contains_element(<i>string str, string list, string delimiter</i>)</b></pre>
<p> **Return type:** `boolean`
<p> **Purpose:** Examines if *any* element in `list` split by `delimiter` is contained in string `str`. If the delimiter is empty, each element is a character. A `NULL` in any argument will return a null value.

<br />

<pre><b>contains_sym(<i>string str1, string str2</i>)</b></pre>
<p> **Return type:** `boolean`
<p> **Purpose:** Returns true if `str1` is a substring of `str2` or vice-versa. If *just one* argument is an empty string, the function returns false. A `NULL` in any argument will return a null value.
