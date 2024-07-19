#!/bin/python

import argparse
import json
import re
import string

code_header = """
#include <benchmark/benchmark.h>
#include <impala_udf/udf.h>
#include <impala_udf/udf-test-harness.h>
#include "uda-bioutils.h"
#include "udf-bioutils.h"

using namespace impala;
using namespace impala_udf;
"""


def generate_json(file_in):
    # For identifying function and extracting function name and arguments
    r = re.compile("IMPALA_UDF_EXPORT\\s+\\S+\\s+(\\S+)\\s*\\((.+?)\\)", re.DOTALL)
    # For splitting the arugments (removes 'const' keyword)
    rarg = re.compile("[\\s,]+(?:const[&*]?[\\s,]+)?(?:[&*]\\s*)?")

    # Read the code in for udf
    fd_in = open(file_in, "r")
    cgs = r.findall(fd_in.read())
    fd_in.close()

    # Collect function names and arugments
    obj_json = {}
    for cg in cgs:
        bm_fun_json = {}
        # Parse the C++ function names
        args = rarg.split(cg[1].strip())
        args = args[2:]

        # Populate function information
        bm_fun_json["function_name"] = cg[0]

        # Populate argument information
        arg_json = {}
        for i in range(0, len(args), 2):
            arg_json[args[i + 1]] = [args[i], "REPLACE_VALUE (type: " + args[i] + ")"]
        bm_fun_json["bm_argument_values"] = arg_json

        obj_json["BM_" + cg[0]] = bm_fun_json

    return json.dumps(obj_json, indent=4)


def generate_code(json_in, tmpl_in):
    # Load JSON
    fd_json = open(json_in, "r")
    dict_fun = json.load(fd_json)
    fd_json.close()

    # Load template file
    fd_tmpl = open(tmpl_in, "r")
    tmpl = string.Template(fd_tmpl.read())
    fd_tmpl.close()

    # Initialize variables to store rest of the file
    str_header = code_header
    str_body = ""
    str_footer = ""

    # Generate the line that calls benchmarking function
    for fun_name, fun_values in dict_fun.items():
        s_call_fun = fun_values["function_name"] + "(context, "
        for bm_args, bm_args_values in fun_values["bm_argument_values"].items():
            if type(bm_args_values[1]) == str:
                s_call_fun += bm_args_values[0] + '("' + bm_args_values[1] + '"), '
            else:
                s_call_fun += bm_args_values[0] + "(" + str(bm_args_values[1]) + "), "
        s_call_fun = s_call_fun[:-2]
        s_call_fun += ");"
        str_body += (
            tmpl.substitute({"bm_function_name": fun_name, "function_call": s_call_fun})
            + "\n"
        )
        str_footer += "BENCHMARK(" + fun_name + ");\n"

    # Set up the footer
    str_footer = str_footer[:-1]
    str_footer += "\nBENCHMARK_MAIN();"

    # Combine all the pieces together
    return str_header + "\n" + str_body + str_footer


if __name__ == "__main__":
    desc = """
This script will generate code to benchmark the udf-bioutils based on the Google
'benchmark' library.
"""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument(
        "input_file",
        help="""
input file that will be processed
    """,
    )
    parser.add_argument(
        "-t",
        "--create-template",
        action="store_true",
        help="""
generates a template .json file containing all the functions in the source code
to be filled in by user
    """,
    )
    parser.add_argument(
        "-l",
        "--code-template",
        default="benchmark/function_tmpl.txt",
        help="""
code template file to use for generating the code (default:
benchmark/function_tmpl.txt)
    """,
    )
    args = parser.parse_args()

    if args.create_template:
        print(generate_json(args.input_file))
        # generate_json(file_uda)
    else:
        print(generate_code(args.input_file, args.code_template))
