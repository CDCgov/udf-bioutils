# Benchmarking Tool

## Overview

The Google [benchmark](https://github.com/google/benchmark) library is
implemented in this library to generate, on a large-scale, code that will
benchmark the functions in the udf-bio library. At minimum, this functionality
requires:

- [Google benchmark library](https://github.com/google/benchmark)
- Python 3.x.x
- JSON input file

The input for this is based on a JSON file that is located in `json/udf-bmark.json`. The JSON file specifies the (in the order that it appears in each key-value pair):

1. name of the user-specified function in
`udf-bioutils-bmark.cc`
2. name of the function in `udf-bioutils.cc`
3. name of the argument
4. data type of the argument
5. value of the argument.

This script is automatically triggered during `make` unless specifically
disabled during `cmake` with `-DEXCLUDE_BENCHMARK=true`. This may be desirable
if the necessary dependencies are not available. To run the benchmark itself,
run the executable (`build/udf-bioutils-bmark`). For advanced options, there is
a help prompt associated with the executable which can be called with the flag
`-h`.

## Example

Below is an example of the structure of the JSON file. The first key-value pair
is based on actual function in `udf-bioutils.cc`, while the second key-value
pair contains placeholder as an illustrative example. The order of the values in
`bm_argument_values` is important.

```json
{
    "BM_Sort_List_By_Substring": {
        "function_name": "Sort_List_By_Substring",
        "bm_argument_values": {
            "listVal": [
                "StringVal",
                "D;Z;K;X;Y;L;Q;J;R;P;W;G;H;S;N;M;I;F;V;B;E;A;C;O;T"
            ],
            "delimVal": [
                "StringVal",
                ";"
            ]
        }
    },
    "BM_Benchmark_Function_Name": {
        "function_name": "Bioutils_Function_Name",
        "bm_argument_values": {
            "Bioutils_Argument_1": [
                "StringVal",
                "DATA"
            ],
            "BioUtils_Argument_2": [
                "IntVal",
                5
            ]
        }
    }
}
```

## Optional Information

The script (`generate_bmark_code.py`) itself utilizes only natively available
Python module (json) to avoid introducing non-native dependencies on a
non-critical aspect of the codebase. However, as YAML is much more
human-readable, the script `convert_file.py` with the flags `-t json` and `-t
yaml` will convert your file between these formats and will print it to
`stdout` which you can save to a JSON format.
