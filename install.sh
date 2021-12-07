#!/bin/bash

if [[ "${1,,}" =~ "dev" ]];then
	deployment=dev
else
	deployment=prod
fi

output_path=/udx/ncird_id/${deployment}


SO=(
	build/libudabioutils.so
	build/libudfbioutils.so
	build/libudfmathutils.so
	build/libudf-sero.so
)

SO=(
	build/libudfbioutils.so
)
hdfs dfs -ls $output_path
for s in "${SO[@]}";do
	echo "Depositing $s"
	hdfs dfs -put -f $s $output_path/$(basename $s)
done
hdfs dfs -ls $output_path
