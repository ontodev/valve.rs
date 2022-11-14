#!/usr/bin/env python3

import random
import string

# import sys

from argparse import ArgumentParser

# TODO: Remove this later:
# from pprint import pformat


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

TOKEN_LENGTH = 9

if __name__ == "__main__":
    parser = ArgumentParser(
        description="""
    Deterministically generate a specified amount of data, a specified percentage of which are
    errors, using a hard-coded VALVE configuration, given the specified seed, to a specified
    output directory.
    """
    )
    parser.add_argument("seed", help="The seed to use to generate the random data")
    parser.add_argument("num_rows", help="The number of rows per table to generate")
    parser.add_argument(
        "pct_errors", help="The percentage of rows in each table that should have errors"
    )
    parser.add_argument(
        "output_dir", help="The output directory to write the new table configuration to"
    )
    args = parser.parse_args()
    seed = int(args.seed)
    num_rows = int(args.num_rows)
    pct_errors = int(args.pct_errors)
    outdir = args.output_dir

    random.seed(seed)

    print(f"Num rows: {num_rows}, Pct Errors: {pct_errors}, Out dir: {outdir}. Using seed: {seed}")

    # This is a record of the last inserted values for each table and column. When one column
    # takes its values from another column, then we look here and fetch the last inserted value of
    # the second column.
    last_inserts = {}
    tsv_files = {}
    tables_in_order = ["table4", "table1", "table2", "table3", "table5", "table6"]
    for table in tables_in_order:
        tsv_files[table] = open(f"{outdir}/{table}.tsv", "w")
        columns = [column for column in config[table]]
        print("\t".join(columns), file=tsv_files[table])

    for row_number in range(1, num_rows + 1):
        for table in tables_in_order:
            print(f"Processing row {row_number} for {table} ...")
            columns = [column for column in config[table]]
            row = {}
            for column in columns:
                # If the column allows empty values, assign an empty value "sometimes":
                if (
                    config[table][column]["allow_empty"]
                    and row_number % random.randrange(2, num_rows) == 1
                ):
                    cell = ""
                else:
                    structure = config[table][column].get("structure")
                    # Note that since each successive value generated by the algorithm should be
                    # unique, we can just ignore "unique" and "primary" structure types.
                    if structure and structure["type"] == "foreign":
                        ftable = structure["ftable"]
                        fcolumn = structure["fcolumn"]
                        cell = last_inserts[ftable][fcolumn]
                    elif structure and structure["type"] == "tree":
                        tcolumn = structure["tcolumn"]
                        cell = last_inserts[table][tcolumn]
                    elif structure and structure["type"] == "under":
                        # Note that properly satisfying the under constraint requires, not only that
                        # the cell is in the specified tree column, but also (a) that the tree
                        # actually exists, and (b) that the value is "under" the under value. To do
                        # this properly, though, would require a decent amount of memory. So perhaps
                        # it's not worth it to check for (a) and (b) and allow any offending cells
                        # to generate errors which we can then verify are handled properly by valve.
                        ttable = structure["ttable"]
                        tcolumn = structure["tcolumn"]
                        cell = last_inserts[ttable][tcolumn]
                    elif config[table][column]["datatype"] == "prefix":
                        # TODO: To be implemented
                        pass
                    elif config[table][column]["datatype"] == "IRI":
                        # TODO: To be implemented
                        pass
                    elif config[table][column]["datatype"] == "trimmed_line":
                        # TODO: To be implemented
                        pass
                    elif config[table][column]["datatype"] == "curie":
                        # TODO: To be implemented
                        pass
                    elif config[table][column]["datatype"] == "label":
                        # TODO: To be implemented
                        pass
                    elif config[table][column]["datatype"] == "text":
                        # TODO: To be implemented
                        pass
                    elif config[table][column]["datatype"] == "word":
                        # TODO: To be implemented
                        pass
                    elif config[table][column]["datatype"] == "integer":
                        cell = "".join(random.choices(string.digits, k=TOKEN_LENGTH))
                    else:
                        cell = "".join(random.choices(string.ascii_lowercase, k=TOKEN_LENGTH))

                row[column] = cell
                # TODO: Instead of only saving the last insert, save the last, say, 5 inserts
                # and when getting values from here, randomly select from those 5. Although note
                # that we will need to be careful because we don't want to select the same value
                # more than once.
                if not last_inserts.get(table):
                    last_inserts[table] = {}
                last_inserts[table][column] = cell

            row = "\t".join([row[column] for column in row])
            print(row, file=tsv_files[table])
