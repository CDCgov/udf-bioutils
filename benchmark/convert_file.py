#!/bin/python

import json
import yaml
import sys
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""
Script will convert between YAML and JSON. Useful for creating test cases.
    """
    )
    parser.add_argument("input_file", help="input file")
    parser.add_argument(
        "-t",
        "--to-type",
        choices=["json", "yaml"],
        help="""
What type to convert to
    """,
    )
    args = parser.parse_args()

    if args.to_type == "json":
        f = open(args.input_file, "r")
        d = yaml.load(f, yaml.Loader)
        f.close()
        print(json.dumps(d, indent=4))
    elif args.to_type == "yaml":
        f = open(args.input_file, "r")
        d = json.load(f)
        f.close()
        print(yaml.dump(d, sort_keys=False))
