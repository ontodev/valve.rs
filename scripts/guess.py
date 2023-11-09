#!/usr/bin/env python3

import csv
import random
import re
import sys
import time

from argparse import ArgumentParser


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


def annotate(label, sample, error_rate, is_primary_candidate):
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
        "TABLE", help="The name of the .TSV file containing the data for which we will be guessing"
    )
    args = parser.parse_args()

    # Use the seed argument, or the epoch time if no seed is given, to set up the random generator:
    if args.seed is not None:
        seed = args.seed
    else:
        seed = time.time_ns()
    random.seed(seed)

    sample = get_random_sample(args.TABLE, args.sample_size)
    for i, label in enumerate(sample):
        annotate(label, sample, args.error_rate, i == 0)

    # For debugging
    # for label in sample:
    #     print(f"{label}: ", end="")
    #     for annotation in sample[label]:
    #         print(f"{annotation} ", end="")
    #     print()

    from pprint import pprint

    pprint(sample)
