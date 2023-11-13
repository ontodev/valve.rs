#!/usr/bin/env python3

import csv
import json
import random
import re
import subprocess
import sys
import time

from argparse import ArgumentParser

# TODO: Remove this import later (used only for debugging):
from pprint import pprint, pformat


def has_ncolumn(sample, ncolumn):
    return bool([label for label in sample if sample[label]["normalized"] == ncolumn])


def get_random_sample(table, sample_size):
    # Get the number of rows in the file (we substract 1 for the header row):
    with open(table, "rb") as f:
        total_rows = sum(1 for _ in f) - 1

    if total_rows <= sample_size:
        sample_size = total_rows
        sample_row_numbers = range(1, total_rows + 1)
    else:
        sample_row_numbers = sorted(random.sample(range(1, total_rows + 1), sample_size))
    with open(table) as f:
        rows = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        sample = {}
        for i, row in enumerate(rows, start=1):
            if i in sample_row_numbers:
                for label, value in row.items():
                    if label not in sample:
                        ncolumn = re.sub(r"[^0-9a-zA-Z_]+", "", label).casefold()
                        if has_ncolumn(sample, ncolumn):
                            print(
                                "The data has more than one column with the normalized name "
                                f"{ncolumn}"
                            )
                            sys.exit(1)
                        sample[label] = {
                            "normalized": ncolumn,
                            "values": [],
                        }
                    sample[label]["values"].append(value)
    return sample


def get_valve_config(valve_table):
    result = subprocess.run(["./valve", "--dump_config", valve_table], capture_output=True)
    if result.returncode != 0:
        error = result.stderr.decode()
        output = result.stdout.decode()
        if output:
            error = f"{error}\n{output}"
        print(f"{error}", file=sys.stderr)
        sys.exit(result.returncode)
    return json.loads(result.stdout.decode())


def get_datatype_hierarchy(config):
    """
    Given a VALVE configuration, return a datatype hierarchy that looks like this:
    {'dt_name_1': [{'datatype': 'dt_name_1',
                    'description': 'a description',
                    ...},
                   {'datatype': 'parent datatype',
                    'description': 'a description',
                    ...},
                   {'datatype': 'grandparent datatype',
                    'description': 'a description',
                    ...},
                   ...],
     'dt_name_2': etc.
    """

    def get_hierarchy_for_dt(primary_dt_name):
        def get_parents(dt_name):
            datatypes = []
            if dt_name is not None:
                datatype = config["datatype"][dt_name]
                if datatype["datatype"] != primary_dt_name:
                    datatypes.append(datatype)
                datatypes += get_parents(datatype.get("parent"))
            return datatypes

        return [config["datatype"][primary_dt_name]] + get_parents(primary_dt_name)

    dt_config = config["datatype"]
    dt_names = [dt_name for dt_name in dt_config]
    leaf_dts = []
    for dt in dt_names:
        children = [child for child in dt_names if dt_config[child].get("parent") == dt]
        if not children:
            leaf_dts.append(dt)

    dt_hierarchy = {}
    for leaf_dt in leaf_dts:
        dt_hierarchy[leaf_dt] = get_hierarchy_for_dt(leaf_dt)
    return dt_hierarchy


def get_foreign_column_data(config):
    # TODO.
    pass


def annotate(label, sample, config, error_rate, is_primary_candidate):
    def has_nulltype(target):
        num_values = len(target["values"])
        num_empties = target["values"].count("")
        return num_empties / num_values > error_rate

    def has_duplicates(target, ignore_empties):
        if ignore_empties:
            values = [v for v in target["values"] if v != ""]
        else:
            values = target["values"]
        distinct_values = set(values)
        return (len(values) - len(distinct_values)) > (error_rate * len(values))

    def get_datatype(target, dt_hierarchy):
        # For each tree in the hierarchy:
        #    Look for a match with the 0th element and possibly add it to matching_datatypes.
        # If there are matches in matching_datatypes:
        #    Use the tiebreaker rules to find the best match and annotate the target with it.
        # Else:
        #    Try again with the next highest element of each tree (if one exists)
        #
        # Note that this is guaranteed to work since the get_datatype_hierarchy() function includes
        # the 'text' datatype which matches anything. So if no matches are found raise an error.

        def is_match(datatype):
            # If the datatype has no associated condition then it matches anything:
            if not datatype.get("condition"):
                return True
            # TODO: Replace this with actual code to check if there is a match:
            return bool(random.getrandbits(1))

        def tiebreak(datatypes):
            # TODO: Replace this with actual code to implement the tiebreaker rules:
            return random.choice(datatypes)

        curr_index = 0
        while True:
            matching_datatypes = []
            datatypes_to_check = []
            for dt_name in dt_hierarchy:
                if len(dt_hierarchy[dt_name]) > curr_index:
                    datatypes_to_check.append(dt_hierarchy[dt_name][curr_index])
            if len(datatypes_to_check) == 0:
                print(f"Could not find a datatype match for column '{label}'")
                sys.exit(1)

            for datatype in datatypes_to_check:
                if is_match(datatype):
                    matching_datatypes.append(datatype)

            if len(matching_datatypes) == 0:
                continue
            elif len(matching_datatypes) == 1:
                return matching_datatypes[0]
            else:
                return tiebreak(matching_datatypes)

            curr_index += 1

    def get_from(target, foreign_column_data):
        # TODO.
        pass

    target = sample[label]
    if has_nulltype(target):
        target["nulltype"] = "empty"
    # Since the target has no nulltype (because the previous branch of the if-statement did not
    # apply), all empties are assumed to be errors, so we pass True here:
    elif not has_duplicates(target, True):
        if is_primary_candidate:
            target["structure"] = "primary"
        else:
            target["structure"] = "unique"

    # Use the valve config to retrieve the valve datatype hierarchy:
    dt_hierarchy = get_datatype_hierarchy(config)
    target["datatype"] = get_datatype(target, dt_hierarchy)["datatype"]

    # TODO: Use the valve config to get a list of columns already loaded to the database, then
    # compare the contents of each column with the contents of the target column and possibly
    # annotate the target with a from() structure.
    if not target.get("structure"):
        foreign_column_data = get_foreign_column_data(config)
        from_structure = get_from(target, foreign_column_data)
        if from_structure:
            target["structure"] = from_structure


if __name__ == "__main__":
    parser = ArgumentParser(description="VALVE guesser (prototype)")
    parser.add_argument(
        "--sample_size",
        type=int,
        default=10000,
        help="Sample size to use when guessing (default: 10,000)",
    )
    parser.add_argument(
        "--error_rate", type=float, default=0.1, help="Proportion of errors expected (default: 10%)"
    )
    parser.add_argument(
        "--enum_size",
        type=int,
        default=10,
        help="The maximum number of values to use for in(...) datatype conditions",
    )
    parser.add_argument(
        "--seed", type=int, help="Seed to use for random sampling (default: current epoch time)"
    )
    parser.add_argument(
        "VALVE_TABLE", help="The VALVE table table from which to read the VALVE configuration"
    )
    parser.add_argument(
        "DATABASE",
        help="""Can be one of (A) A URL of the form `postgresql://...` or
        `sqlite://...` (B) The filename (including path) of a sqlite database.""",
    )
    parser.add_argument(
        "TABLE", help="A .TSV file containing the data for which we will be guessing"
    )
    args = parser.parse_args()

    # Use the seed argument, or the epoch time if no seed is given, to set up the random generator:
    if args.seed is not None:
        seed = args.seed
    else:
        seed = time.time_ns()
    random.seed(seed)

    # Get the valve configuration and database info:
    config = get_valve_config(args.VALVE_TABLE)
    config["db"] = args.DATABASE

    sample = get_random_sample(args.TABLE, args.sample_size)
    for i, label in enumerate(sample):
        annotate(label, sample, config, args.error_rate, i == 0)

    pprint(sample)
    # For debugging
    # for label in sample:
    #     print(f"{label}: ", end="")
    #     for annotation in sample[label]:
    #         print(f"{annotation} ", end="")
    #     print()
