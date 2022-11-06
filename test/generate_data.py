#!/usr/bin/env python3

import json
import sys

from argparse import ArgumentParser
from graphlib import TopologicalSorter


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


def generate_row(config, table, row_number, row, column_dependency_order, last_insert):
    for column in column_dependency_order:
        # TODO: Take into account the constraints, trees, etc. that must restrict the values we can
        # assign to this column.
        value = generate_column_value(row[column]["sql_type"], row_number)
        row[column]["value"] = value
        if not last_insert.get(table):
            last_insert[table] = {}
        last_insert[table][column] = value
        print(f"Value for {table}.{column}: {value}")


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
    parser = ArgumentParser()
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
                config, table, row_number, row, column_dependency_order, last_insert
            )
            write_row(table, output_row, args.output_dir)
