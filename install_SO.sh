#!/bin/bash

output_path=${UDF_BIOUTILS_PATH:-"/udx/ncird_id/${1:-"prod"}"}

SO=(
    build/libudabioutils.so
    build/libudfbioutils.so
    build/libudfmathutils.so
)

hdfs dfs -ls "$output_path" || {
    echo "'$output_path' may not exist"
    exit 1
}
for s in "${SO[@]}"; do
    echo "Depositing $s to '$output_path'"
    hdfs dfs -put -f "$s" "$output_path/$(basename "$s")"
done
hdfs dfs -ls "$output_path"
