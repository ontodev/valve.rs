#!/usr/bin/env python3

import json
import random
import sys

from argparse import ArgumentParser

# Remove this later:
from pprint import pformat


config = {
    "table1": {
        "prefix": {
            "allow_empty": False,
            "datatype": "prefix",
            "structure": {
                "type": "primary",
            },
        },
        "base": {
            "allow_empty": False,
            "datatype": "IRI",
            "structure": {
                "type": "unique",
            },
        },
        "ontology IRI": {
            "allow_empty": True,
            "datatype": "IRI",
        },
        "version IRI": {
            "allow_empty": True,
            "datatype": "IRI",
        },
    },
    "table2": {
        "child": {
            "allow_empty": False,
            "datatype": "trimmed_line",
            "structure": {
                "type": "foreign",
                "ftable": "table4",
                "fcolumn": "other_foreign_column",
            },
        },
        "parent": {
            "allow_empty": True,
            "datatype": "trimmed_line",
            "structure": {
                "type": "tree",
                "tcolumn": "child",
            },
        },
        "xyzzy": {
            "allow_empty": True,
            "datatype": "trimmed_line",
            "structure": {
                "type": "under",
                "ttable": "table2",
                "tcolumn": "child",
                "uval": "d",
            },
        },
        "foo": {
            "allow_empty": True,
            "datatype": "integer",
            "structure": {
                "type": "foreign",
                "ftable": "table4",
                "fcolumn": "numeric_foreign_column",
            },
        },
        "bar": {
            "allow_empty": True,
            "datatype": "text",
        },
    },
    "table3": {
        "source": {
            "allow_empty": False,
            "datatype": "prefix",
            "structure": {
                "type": "foreign",
                "ftable": "table1",
                "fcolumn": "prefix",
            },
        },
        "id": {
            "allow_empty": False,
            "datatype": "curie",
            "structure": {
                "type": "unique",
            },
        },
        "label": {
            "allow_empty": False,
            "datatype": "label",
            "structure": {
                "type": "primary",
            },
        },
        "parent": {
            "allow_empty": True,
            "datatype": "label",
            "structure": {
                "type": "tree",
                "tcolumn": "label",
            },
        },
        "related": {
            "allow_empty": True,
            "datatype": "trimmed_line",
        },
    },
    "table4": {
        "foreign_column": {
            "allow_empty": False,
            "datatype": "text",
            "structure": {
                "type": "unique",
            },
        },
        "other_foreign_column": {
            "allow_empty": False,
            "datatype": "text",
            "structure": {
                "type": "unique",
            },
        },
        "numeric_foreign_column": {
            "allow_empty": False,
            "datatype": "integer",
            "structure": {
                "type": "primary",
            },
        },
    },
    "table5": {
        "foo": {
            "allow_empty": False,
            "datatype": "word",
            "structure": {
                "type": "primary",
            },
        },
        "bar": {
            "allow_empty": False,
            "datatype": "integer",
        },
    },
    "table6": {
        "child": {
            "allow_empty": False,
            "datatype": "integer",
            "structure": {
                "type": "foreign",
                "ftable": "table4",
                "fcolumn": "numeric_foreign_column",
            },
        },
        "parent": {
            "allow_empty": True,
            "datatype": "integer",
            "structure": {
                "type": "tree",
                "tcolumn": "child",
            },
        },
        "xyzzy": {
            "allow_empty": True,
            "datatype": "integer",
            "structure": {
                "type": "under",
                "ttable": "table6",
                "tcolumn": "child",
                "uval": "4",
            },
        },
        "foo": {
            "allow_empty": True,
            "datatype": "text",
        },
        "bar": {
            "allow_empty": True,
            "datatype": "integer",
        },
    },
}


if __name__ == "__main__":
    parser = ArgumentParser(
        description="""
    Deterministically generate a specified amount of data, a specified percentage of which are
    errors, using a hard-coded VALVE configuration, given the specified seed, to a specified
    output directory.
    """
    )
    parser.add_argument(
        "seed", help="The seed to use to generate the random data"
    )
    parser.add_argument(
        "num_rows", help="The number of rows per table to generate"
    )
    parser.add_argument(
        "pct_errors", help="The percentage of rows in each table that should have errors"
    )
    parser.add_argument(
        "output_dir", help="The output directory to write the new table configuration to"
    )
    args = parser.parse_args()
    seed = args.seed
    num_rows = args.num_rows
    pct_errors = args.pct_errors
    outdir = args.output_dir

    print(f"Num rows: {num_rows}, Pct Errors: {pct_errors}, Out dir: {outdir}. Using seed: {seed}")

    for table in ["table4", "table1", "table2", "table3", "table5", "table6"]:
        columns = [column for column in config[table]]
        print("\t".join(columns))
