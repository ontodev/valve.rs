#!/usr/bin/env python3

import json
import sys

from argparse import ArgumentParser
from graphlib import TopologicalSorter

# Remove this later:
from pprint import pformat


def get_sql_type(dt_config, datatype_name):
    datatype = dt_config.get(datatype_name)
    if not datatype:
        raise Exception(f"No such datatype: {datatype_name}")

    sql_type = datatype.get("SQL type")
    if sql_type:
        return sql_type

    return get_sql_type(dt_config, datatype["parent"])


def get_constraints(constraints_config, table, column):
    foreign = [c for c in constraints_config["foreign"][table] if c["column"] == column]
    primary = [c for c in constraints_config["primary"][table] if c == column]
    unique = [c for c in constraints_config["unique"][table] if c == column]
    tree = [c for c in constraints_config["tree"][table] if c["parent"] == column]
    under = [c for c in constraints_config["under"][table] if c["column"] == column]
    constraints = {
        "foreign": next(
            iter([{"ftable": f["ftable"], "fcolumn": f["fcolumn"]} for f in foreign]), None
        ),
        "primary": bool(primary),
        "unique": bool(unique),
        "tree": next(iter([t["child"] for t in tree]), None),
        "under": next(
            iter(
                [
                    {"ttable": u["ttable"], "tcolumn": u["tcolumn"], "value": u["value"]}
                    for u in under
                ]
            ),
            None,
        ),
    }
    return constraints


def generate_column_value(sql_type, row_number):
    if sql_type.casefold() == "integer":
        return row_number
    else:
        div = row_number
        value = ""
        while div:
            (div, mod) = divmod(div, 26)
            if mod == 0:
                mod = 26
                div -= 1
            value = chr(mod + 64) + value
        return value


def rule_applies(config, when_column, when_column_condition, when_column_value):
    if when_column_condition in ["null", "not null"]:
        print(
            "Checking to see if {} value: '{}' satisfies {}".format(
                when_column, when_column_value, when_column_condition
            )
        )
        when_column_nulltype = row[when_column]["nulltype"]
        when_column_nulltype_function = config["datatype_conditions"][when_column_nulltype][
            "compiled_condition"
        ]["parsed"]["function"]

        if when_column_nulltype_function["name"] == "equals":
            when_column_nulltype_equals = when_column_nulltype_function["args"][0]["label"].strip(
                "'\""
            )
            if when_column_condition == "null":
                print(
                    f"(I.e., checking to see if '{when_column_value}' == '{when_column_nulltype_equals}')"
                )
                return when_column_value == when_column_nulltype_equals
            else:
                print(
                    f"(I.e., checking to see if '{when_column_value}' != '{when_column_nulltype_equals}')"
                )
                return when_column_value != when_column_nulltype_equals
        elif when_column_nulltype_function["name"] == "match":
            # TODO: To be implemented.
            return False
        elif when_column_nulltype_function["name"] == "search":
            # TODO: To be implemented.
            return False
        else:
            # TODO: To be implemented.
            return False
    else:
        # TODO: To be implemented.
        return False


def get_rule_restricted_cell_values(config, row, rules):
    for rule in rules:
        when_column = rule["when column"]
        when_column_condition = rule["when condition"]
        when_column_value = row[when_column]["value"]
        if not rule_applies(config, when_column, when_column_condition, when_column_value):
            print("Rule does not apply or check for this rule not yet implemented.")
            continue

        print("Rule applies.")


def generate_row(config, table, row, column_dependency_order, row_number, last_insert):
    for column in column_dependency_order:
        cell_info = row[column]
        print(f'Cell info for row #{row_number} of "{table}"."{column}":\n{pformat(cell_info)}')

        # Rule restrictions:
        if cell_info["rules"]:
            possible_values = get_rule_restricted_cell_values(config, row, cell_info["rules"])

        # TODO: tree constraints

        # TODO: under constraints

        # TODO: foreign constraints

        # TODO: Since our algorithm is being written so that it generates a value corresponding
        # to the given row number, we do not need to check unique and primary keys or nulltypes.
        # However, when we later implement the 'pct_errors' command line argument, we will need
        # to check those values so that we can generate that kind of error. And note that we will
        # want to generate errors corresponding to rule/tree/under/foreign constraint violations
        # as well.

        # The fallback/default case, where no restrictions on the generated cell value apply:
        value = generate_column_value(cell_info["sql_type"], row_number)
        cell_info["value"] = value

        # Add the newly generated value to the last_insert structure:
        if not last_insert.get(table):
            last_insert[table] = {}
        last_insert[table][column] = {"row_number": row_number, "value": value}
        print(f'Value for "{table}"."{column}": {value}')
        print("-------------------------------------")


def write_row(table, row, output_dir):
    pass


def sort_columns_by_dependency(table, row):
    ts = TopologicalSorter()
    all_columns = [column for column in row]
    deps = []
    for column in all_columns:
        if row[column]["constraints"].get("tree"):
            deps.append(row[column]["constraints"]["tree"])
        if (
            row[column]["constraints"].get("under")
            and row[column]["constraints"]["under"]["ttable"] == table
        ):
            deps.append(row[column]["constraints"]["under"]["tcolumn"])
        for rule in row[column]["rules"]:
            deps.append(rule["when column"])

    ts.add(column, *deps)
    enforced_order = list(ts.static_order())
    sorted_columns = enforced_order + [c for c in all_columns if c not in enforced_order]
    return sorted_columns


if __name__ == "__main__":
    parser = ArgumentParser(
        description="""
    Read a JSON-formatted string representing a VALVE configuration from STDIN and
    deterministically generate a specified amount of data, a specified percentage of
    which are errors, in accordance with the given configuration, to a specified
    output directory.
    """
    )
    parser.add_argument(
        "num_rows", help="The number of rows per table to generate (default: 10,000)"
    )
    parser.add_argument(
        "pct_errors", help="The percentage of rows in each table that should have errors"
    )
    parser.add_argument(
        "output_dir", help="The output directory to write the new table configuration to"
    )
    args = parser.parse_args()
    outdir = args.output_dir
    num_rows = args.num_rows

    config = json.loads("\n".join(sys.stdin.readlines()))

    tables_config = config["table"]
    constraints_config = config["constraints"]
    datatype_conditions = config["datatype_conditions"]
    structure_conditions = config["structure_conditions"]
    rule_conditions = config["rule_conditions"]
    sorted_table_list = [
        t for t in config["sorted_table_list"] if t not in ["table", "column", "datatype", "rule"]
    ]

    # This is a record of the last inserted values for each table and column. Since the algorithm
    # we are using to generate the data is deterministic, this should be enough to infer all
    # previously inserted data.
    last_insert = {}

    for table in sorted_table_list:
        table_config = tables_config[table]
        outfile = open(f"{outdir}/{table}.tsv", "w")

        # Write the header:
        column_output_order = table_config["column_order"]
        print("\t".join(column_output_order), file=outfile)

        # Initialize an empty row, which we will then fill keeping in mind whatever constraints
        # and conditions are relevant:
        row = {c: None for c in column_output_order}

        for column in column_output_order:
            column_config = table_config["column"]
            # Note that Datatype parsings are in config['datatype_conditions']
            datatype_name = column_config[column]["datatype"]
            sql_type = get_sql_type(config["datatype"], datatype_name)
            nulltype = column_config[column].get("nulltype")
            constraints = get_constraints(constraints_config, table, column)
            # Note that Structure parsings are in config['structure_conditions']
            structure = column_config[column].get("structure")
            # Note that rule parsings are in config['rule_conditions']
            rules = []
            if config["rule"].get(table):
                table_rules = config["rule"][table]
                for rule_column in table_rules:
                    for rule in table_rules[rule_column]:
                        if rule["then column"] == column:
                            rules.append(
                                {
                                    "when column": rule["when column"],
                                    "when condition": rule["when condition"],
                                    "then column": rule["then column"],
                                    "then condition": rule["then condition"],
                                }
                            )

            row[column] = {
                "datatype_name": datatype_name,
                "sql_type": sql_type,
                "nulltype": nulltype,
                "constraints": constraints,
                "structure": structure,
                "rules": rules,
                # To be filled in later:
                "value": None,
            }

        # TODO: Generate errors according to args.pct_errors
        column_dependency_order = sort_columns_by_dependency(table, row)
        for row_number in range(1, int(args.num_rows) + 1):
            output_row = generate_row(
                config, table, row, column_dependency_order, row_number, last_insert
            )
            write_row(table, output_row, args.output_dir)
