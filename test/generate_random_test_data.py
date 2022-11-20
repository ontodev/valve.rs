#!/usr/bin/env python3

import math
import random
import string

from argparse import ArgumentParser

# TODO: Remove this later:
# from pprint import pformat


TOKEN_LENGTH = 9


CONFIG = {
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


def get_value_from_prev_insert(prev_inserts, from_table, from_column, to_table, to_column):
    global CONFIG

    # Note: because we are loading the tables and columns in the correct order (i.e. such that
    # all dependencies are loaded before the tables and columns they depend on), the list of
    # previous inserts for the from_table/from_column will never be empty.
    if len(prev_inserts[from_table][from_column]) == 1:
        if CONFIG[to_table][to_column]["allow_empty"]:
            return ""
        else:
            return prev_inserts[from_table][from_column][0]
    else:
        from_values = prev_inserts[from_table][from_column]
        # We'd ideally like to exclude the last inserted value from consideration, but we save it
        # here in case we cannot:
        last_val = from_values[len(from_values) - 1]
        from_values = from_values[0 : len(from_values) - 1]

        to_values = set(prev_inserts[to_table][to_column])
        values_to_choose_from = [f for f in from_values if f not in to_values]

        if not values_to_choose_from:
            return last_val
        else:
            return values_to_choose_from[random.randrange(len(values_to_choose_from))]


def get_constrained_cell_value(table, column, row_num, prev_inserts):
    global TOKEN_LENGTH
    global CONFIG

    structure = CONFIG[table][column].get("structure")
    if structure and structure["type"] == "foreign":
        ftable = structure["ftable"]
        fcolumn = structure["fcolumn"]
        cell = get_value_from_prev_insert(prev_inserts, ftable, fcolumn, table, column)
    elif structure and structure["type"] == "tree":
        tcolumn = structure["tcolumn"]
        cell = get_value_from_prev_insert(prev_inserts, table, tcolumn, table, column)
    elif structure and structure["type"] == "under":
        # Note that properly satisfying the under constraint requires, not only that
        # the cell is in the specified tree column, but also (a) that the tree
        # actually exists, and (b) that the value is "under" the under value. To do
        # this properly, though, would require a decent amount of memory. So perhaps
        # it's not worth it to check for (a) and (b) and allow any offending cells
        # to generate errors which we can then verify are handled properly by valve.
        ttable = structure["ttable"]
        tcolumn = structure["tcolumn"]
        cell = get_value_from_prev_insert(prev_inserts, ttable, tcolumn, table, column)
    elif CONFIG[table][column]["datatype"] in [
        "prefix",
        "IRI",
        "trimmed_line",
        "label",
        "word",
    ]:
        cell = "".join(random.choices(string.ascii_lowercase, k=TOKEN_LENGTH))
    elif CONFIG[table][column]["datatype"] == "curie":
        cell = (
            "".join(random.choices(string.ascii_lowercase, k=3)).upper()
            + ":"
            + "".join(random.choices(string.ascii_lowercase, k=TOKEN_LENGTH))
        )
    elif CONFIG[table][column]["datatype"] == "text":
        cell = (
            "".join(random.choices(string.ascii_lowercase, k=TOKEN_LENGTH))
            + " "
            + "".join(random.choices(string.ascii_lowercase, k=TOKEN_LENGTH))
        )
    elif CONFIG[table][column]["datatype"] == "integer":
        # No leading 0s:
        cell = "".join(random.choices("123456789", k=1)) + "".join(
            random.choices(string.digits, k=TOKEN_LENGTH - 1)
        )
    else:
        print(
            f"Warning: Unknown datatype: {CONFIG[table][column]['datatype']}. "
            "Generating a random string."
        )
        cell = "".join(random.choices(string.ascii_lowercase, k=TOKEN_LENGTH))

    return cell


def main():
    global TOKEN_LENGTH
    global CONFIG

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

    # This is a record of the last inserted values for each table and column. When one column
    # takes its values from another column, then we look here and fetch the last inserted value of
    # the second column.
    prev_inserts = {}
    tsv_files = {}
    tables_in_order = ["table4", "table1", "table2", "table3", "table5", "table6"]
    for table in tables_in_order:
        tsv_files[table] = open(f"{outdir}/{table}.tsv", "w")
        columns = [column for column in CONFIG[table]]
        print("\t".join(columns), file=tsv_files[table])

    num_error_rows = math.ceil((pct_errors / 100) * num_rows)
    error_proportion = None if not num_error_rows else math.floor(num_rows / num_error_rows)
    for row_num in range(1, num_rows + 1):
        for table in tables_in_order:
            is_error_row = error_proportion and row_num % error_proportion == (
                random.randrange(1, num_rows + 1)
            )
            if is_error_row:
                print(f"Generating error row for {table} at row number {row_num}.")

            columns = [column for column in CONFIG[table]]
            row = {}
            for column in columns:
                if (
                    not is_error_row
                    and CONFIG[table][column]["allow_empty"]
                    and row_num % random.randrange(2, num_rows) == 1
                ):
                    # If the column allows empty values, assign an empty value "sometimes":
                    cell = ""
                elif not is_error_row:
                    cell = get_constrained_cell_value(table, column, row_num, prev_inserts)
                else:
                    # If this is an error row, just generate a random token. This might end up
                    # being valid, but given that some cells have structure constraints, then
                    # chances are this will result in at least some errors:
                    if CONFIG[table][column]["datatype"] == "integer":
                        # No leading 0s:
                        cell = "".join(random.choices("123456789", k=1)) + "".join(
                            random.choices(string.digits, k=TOKEN_LENGTH - 1)
                        )
                    else:
                        cell = "".join(random.choices(string.ascii_lowercase, k=TOKEN_LENGTH))

                row[column] = cell
                # TODO: We are generally only going to be generating about 100,000 rows at maximum,
                # so don't worry too much about memory constraints. In other words instead of just
                # saving the _last_ inserted values, just save _all_ of them. Then, when we are
                # looking up a foreign or tree column, randomly select one of the previous values.
                #
                # Don't be too clever. It's ok if, once in awhile, you select a value that has
                # already been used. Just make the random.range() from which you make the selection
                # big enough so that this doesn't happen too often. A few "duplicate value" errors
                # is ok. Valve will just handle them and generae the appropriate errors.
                if not prev_inserts.get(table):
                    prev_inserts[table] = {}
                if not prev_inserts[table].get(column):
                    prev_inserts[table][column] = []
                prev_inserts[table][column].append(cell)

            row = "\t".join([row[column] for column in row])
            print(row, file=tsv_files[table])


if __name__ == "__main__":
    main()
