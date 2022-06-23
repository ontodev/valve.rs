#!/usr/bin/env python3

import json
import sys
import time
import valve

from argparse import ArgumentParser


def log(message, suppress_time=True):
    if not suppress_time:
        print(f"{time.asctime()} {message}", file=sys.stderr)
    else:
        print(f"{message}", file=sys.stderr)


def warn(message, suppress_time=True):
    log(f"WARNING: {message}", suppress_time)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "table",
        help="A TSV file containing high-level information about the data in the database",
    )
    parser.add_argument("db_dir", help="The directory in which to save the database file")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--load", action="store_true")
    group.add_argument("--insert_update", action="store_true")
    args = parser.parse_args()

    if args.load:
        config = valve.py_configure_and_or_load(args.table, args.db_dir, True)
    elif args.insert_update:
        config = valve.py_configure_and_or_load(args.table, args.db_dir, False)
        matching_values = valve.py_get_matching_values(config, args.db_dir, "foobar", "child")
        matching_values = json.loads(matching_values)
        assert matching_values == [
            {"id": "a", "label": "a", "order": 1},
            {"id": "b", "label": "b", "order": 2},
            {"id": "c", "label": "c", "order": 3},
            {"id": "d", "label": "d", "order": 4},
            {"id": "e", "label": "e", "order": 5},
            {"id": "f", "label": "f", "order": 6},
            {"id": "g", "label": "g", "order": 7},
            {"id": "h", "label": "h", "order": 8},
        ]

        # NOTE: No validation of the validate/insert/update functions is done below. You must use an
        # external script to fetch the data from the database and run a diff against a known good
        # sample.

        row = {
            "child": {"messages": [], "valid": True, "value": "b"},
            "parent": {"messages": [], "valid": True, "value": "f"},
            "xyzzy": {"messages": [], "valid": True, "value": "w"},
            "foo": {"messages": [], "valid": True, "value": "A"},
            "bar": {
                "messages": [
                    {"level": "error", "message": "An unrelated error", "rule": "custom:unrelated"}
                ],
                "valid": False,
                "value": "B",
            },
        }

        result_row = valve.py_validate_row(config, args.db_dir, "foobar", json.dumps(row), True, 1)
        valve.py_update_row(args.db_dir, "foobar", result_row, 1)

        row = {
            "id": {"messages": [], "valid": True, "value": "BFO:0000027"},
            "label": {"messages": [], "valid": True, "value": "car"},
            "parent": {
                "messages": [
                    {"level": "error", "message": "An unrelated error", "rule": "custom:unrelated"}
                ],
                "valid": False,
                "value": "barrie",
            },
            "source": {"messages": [], "valid": True, "value": "BFOBBER"},
            "type": {"messages": [], "valid": True, "value": "owl:Class"},
        }

        result_row = valve.py_validate_row(config, args.db_dir, "import", json.dumps(row), False)
        new_row_num = valve.py_insert_new_row(args.db_dir, "import", result_row)
