#!/bin/bash

cat udf-*.yaml > udf.yaml \
    && ../convert_file.py -t json udf.yaml > ../../json/udf-bmark.json \
    && rm udf.yaml \
    || echo "Something went wrong."
