#!/usr/bin/env python3

import csv
import json
import os.path
import psycopg2
import re
import sqlite3
import sys

from argparse import ArgumentParser
from collections import OrderedDict


def get_column_order_and_info_for_postgres(cursor, table):
    """
    Given a database cursor (for a PostgreSQL database) and a table name, determine which columns
    rows should be sorted by. For tables with primary keys, sort by primary key first, then by all
    other columns from left to right. For tables without primary keys, sort by row_number. Returns a
    dictionary consisting of an unsorted and a sorted list of column names, a list of the table's
    primary keys sorted by priority, and a list of the table's unique keys. I.e., returns a dict of
    the form: {"unsorted_columns": [], "sorted_columns": [], "primary_keys": [], "unique_keys": []}
    """
    constraints_query_template = f"""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tco
        JOIN information_schema.key_column_usage kcu
          ON kcu.constraint_name = tco.constraint_name
             AND kcu.constraint_schema = tco.constraint_schema
             AND kcu.table_name = '{table}'
        WHERE tco.constraint_type = '{{}}' -- A placeholder to be filled in in python using format()
        ORDER BY kcu.column_name, kcu.ordinal_position
        """

    cursor.execute(constraints_query_template.format("PRIMARY KEY"))
    primary_keys = [row[0] for row in cursor]

    cursor.execute(
        f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table}'
        ORDER BY ordinal_position
        """
    )
    if not primary_keys:
        sorted_columns = ["row_number"]
        unsorted_columns = [row[0] for row in cursor]
    else:

        def add_meta(columns):
            columns_with_meta = []
            for column in columns:
                columns_with_meta.append(column)
                columns_with_meta.append(column + "_meta")
            return columns_with_meta

        unsorted_columns = []
        non_pk_columns = []
        for row in cursor:
            column_name = row[0]
            unsorted_columns.append(column_name)
            if (
                column_name not in primary_keys
                and not column_name.endswith("_meta")
                and not column_name == "row_number"
            ):
                non_pk_columns.append(column_name)
        sorted_columns = add_meta(primary_keys + non_pk_columns)

    cursor.execute(constraints_query_template.format("UNIQUE"))
    unique_keys = [row[0] for row in cursor]

    return {
        "unsorted_columns": unsorted_columns,
        "sorted_columns": sorted_columns,
        "primary_keys": primary_keys,
        "unique_keys": unique_keys,
    }


def get_column_order_and_info_for_sqlite(cursor, table):
    """
    Given a database cursor (for a SQLite database) and a table name, determine which columns rows
    should be sorted by. For tables with primary keys, sort by primary key first, then by all other
    columns from left to right. For tables without primary keys, sort by row_number. Returns a
    dictionary consisting of an unsorted and a sorted list of column names, a list of the table's
    primary keys sorted by priority, and a list of the table's unique keys. I.e., returns a dict of
    the form: {"unsorted_columns": [], "sorted_columns": [], "primary_keys": [], "unique_keys": []}
    """
    cursor.execute(f'PRAGMA TABLE_INFO("{table}")')
    columns_info = [d[0] for d in cursor.description]
    pragma_rows = list(map(lambda r: OrderedDict(zip(columns_info, r)), cursor))
    primary_keys = OrderedDict()
    if not any([row["pk"] == 1 for row in pragma_rows]):
        sorted_columns = ["row_number"]
        unsorted_columns = [p["name"] for p in pragma_rows]
    else:

        def add_meta(columns):
            columns_with_meta = []
            for column in columns:
                columns_with_meta.append(column)
                columns_with_meta.append(column + "_meta")
            return columns_with_meta

        unsorted_columns = []
        non_pk_columns = []
        for row in pragma_rows:
            unsorted_columns.append(row["name"])
            if row["pk"] != 0:
                primary_keys[row["pk"]] = row["name"]
            elif not row["name"].endswith("_meta") and not row["name"] == "row_number":
                non_pk_columns.append(row["name"])
        primary_keys = OrderedDict(sorted(primary_keys.items()))
        sorted_columns = add_meta([primary_keys[key] for key in primary_keys] + non_pk_columns)

    # Two steps are needed (INDEX_LIST and INDEX_INFO) to get the list of unique keys:
    cursor.execute(f'PRAGMA INDEX_LIST("{table}")')
    columns_info = [d[0] for d in cursor.description]
    pragma_rows = list(map(lambda r: dict(zip(columns_info, r)), cursor))
    unique_constraints = [
        key["name"] for key in pragma_rows if key["unique"] == 1 and key["origin"] == "u"
    ]
    unique_keys = []
    for uni in unique_constraints:
        cursor.execute(f'PRAGMA INDEX_INFO("{uni}")')
        columns_info = [d[0] for d in cursor.description]
        pragma_rows = list(map(lambda r: dict(zip(columns_info, r)), cursor))
        [unique_keys.append(p["name"]) for p in pragma_rows]

    return {
        "unsorted_columns": unsorted_columns,
        "sorted_columns": sorted_columns,
        "primary_keys": [primary_keys[key] for key in primary_keys],
        "unique_keys": unique_keys,
    }


def export_data(cursor, is_sqlite, args):
    """
    Given a database cursor, a flag indicating whether this is a sqlite or postgres db, and a
    dictionary containing: an output directory, "output", a list of tables, "tables", optionally
    a flag, "raw" (which defaults to False), which if True indicates that the data should be
    exported as is (i.e., including meta columns and simply sorted by row number), and optionally a
    flag, "nosort", which if True indicates that the data should not be sorted by anything other
    than the row number; given all of this, export all of the given database tables to .tsv files
    in the output directory. Note that if "raw" is True, then "nosort" is ignored.
    """
    output_dir = os.path.normpath(args["output_dir"])
    tables = args["tables"]
    raw = bool(args.get("raw"))
    nosort = bool(args.get("nosort"))

    for table in tables:
        try:
            if is_sqlite:
                columns_info = get_column_order_and_info_for_sqlite(cursor, table)
            else:
                columns_info = get_column_order_and_info_for_postgres(cursor, table)
            sorted_columns = columns_info["sorted_columns"]
            unsorted_columns = columns_info["unsorted_columns"]

            select = []
            for column in unsorted_columns:
                if raw or column == "row_number":
                    select.append(f'"{column}"')
                elif not column.endswith("_meta"):
                    if is_sqlite:
                        else_stmt = f"""JSON_EXTRACT("{column}_meta", '$.value')"""
                    else:
                        else_stmt = f"(JSON(\"{column}_meta\") ->> 'value')::TEXT"
                    select.append(
                        f"""
                        CASE
                          WHEN "{column}" IS NOT NULL THEN CAST("{column}" AS TEXT)
                          ELSE {else_stmt}
                          END AS "{column}"
                        """
                    )
            select = ", ".join(select)

            if raw or nosort:
                order_by = ["row_number"]
            else:
                order_by = list(map(lambda x: f'"{x}"', sorted_columns))
            order_by = ", ".join(order_by)

            # Fetch the rows from the table and write them to a corresponding TSV file in the
            # output directory:
            cursor.execute(f'SELECT {select} FROM "{table}_view" ORDER BY {order_by}')
            colnames = [d[0] for d in cursor.description]
            rows = map(lambda r: dict(zip(colnames, r)), cursor)

            if raw:
                fieldnames = [c for c in colnames if c != "row_number"]
            else:
                fieldnames = [c for c in colnames if not c.endswith("_meta") and c != "row_number"]
            with open(f"{output_dir}/{table}.tsv", "w", newline="\n") as csvfile:
                writer = csv.DictWriter(
                    csvfile,
                    fieldnames=fieldnames,
                    delimiter="\t",
                    doublequote=False,
                    strict=True,
                    lineterminator="\n",
                    quoting=csv.QUOTE_NONE,
                    escapechar="",
                    quotechar="",
                )
                writer.writeheader()
                for row in rows:
                    del row["row_number"]
                    writer.writerow(row)
        except sqlite3.OperationalError as e:
            print(f"ERROR while exporting {table}: {e}", file=sys.stderr)


def export_messages(cursor, is_sqlite, args):
    """
    Given a database cursor, a flag indicating whether this is a sqlite or postgres db, and a
    dictionary containing: an output directory, "output", a list of tables, "tables", and a flag,
    "a1", indicating whether to use A1 format, export all of the error messages contained in the
    given database tables to a file called messages.tsv in the output directory.
    """
    output_dir = os.path.normpath(args["output_dir"])
    tables = args["tables"]
    a1 = args["a1"]

    def col_to_a1(column, columns):
        col = columns.index(column) + 1
        div = col
        columnid = ""
        while div:
            (div, mod) = divmod(div, 26)
            if mod == 0:
                mod = 26
                div -= 1
            columnid = chr(mod + 64) + columnid
        return columnid

    def create_message_rows(table, row, primary_keys):
        if a1 or not primary_keys:
            row_number = "{}".format(row["row_number"])
        else:
            row_number = []
            for pk in primary_keys:
                rn = str(row[pk]) if type(row[pk]) == int else row[pk]
                if not rn:
                    if is_sqlite:
                        rn = json.loads(row[f"{pk}_meta"])["value"]
                    else:
                        rn = row[f"{pk}_meta"]["value"]
                row_number.append(rn)
            row_number = "###".join(row_number)

        message_rows = []
        for column_key in [ckey for ckey in row if ckey.endswith("_meta")]:
            meta = json.loads(row[column_key]) if is_sqlite else row[column_key]
            if not meta["valid"]:
                columnid = re.sub("_meta", "", column_key)
                if a1:
                    columnid = col_to_a1(
                        columnid, [c for c in row if c != "row_number" and not c.endswith("_meta")]
                    )

                for message in meta["messages"]:
                    m = {
                        "table": table,
                        "level": message["level"],
                        "rule_id": message["rule"],
                        "message": message["message"],
                        "value": meta["value"],
                    }
                    if not a1:
                        m.update({"row": row_number, "column": columnid})
                    else:
                        m.update({"cell": f"{columnid}{row_number}"})
                    message_rows.append(m)
        return message_rows

    def select_column(column):
        if not column.endswith("_meta"):
            sql = f'"{column}"'
        else:
            json_func = "JSON" if is_sqlite else "JSONB"
            sql = (
                f'CASE WHEN "{column}" IS NOT NULL THEN {json_func}("{column}") '
                f'ELSE {json_func}(\'{{"valid": true, "messages": []}}\') '
                f'END AS "{column}"'
            )

        return sql

    if a1:
        fieldnames = ["table", "cell", "level", "rule_id", "message", "value"]
    else:
        fieldnames = ["table", "row", "column", "level", "rule_id", "message", "value"]
    with open(f"{output_dir}/messages.tsv", "w", newline="\n") as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=fieldnames,
            delimiter="\t",
            doublequote=False,
            strict=True,
            lineterminator="\n",
            quoting=csv.QUOTE_NONE,
            escapechar="\\",
            quotechar="",
        )
        writer.writeheader()
        for table in tables:
            try:
                if is_sqlite:
                    columns_info = get_column_order_and_info_for_sqlite(cursor, table)
                else:
                    columns_info = get_column_order_and_info_for_postgres(cursor, table)
                unsorted_columns = columns_info["unsorted_columns"]
                sorted_columns = columns_info["sorted_columns"]
                primary_keys = columns_info["primary_keys"]

                select = ", ".join([select_column(c) for c in unsorted_columns])
                # For PostgreSQL we need to explicitly cast to TEXT, otherwise it will complain
                # when we try to call LOWER() on non-character fields like row_number.
                cast = "" if is_sqlite else "::TEXT"
                order_by = ", ".join([f'LOWER("{c}"{cast}) NULLS FIRST' for c in sorted_columns])
                cursor.execute(f'SELECT {select} FROM "{table}_view" ORDER BY {order_by}')
                columns_info = [d[0] for d in cursor.description]
                rows = map(lambda r: OrderedDict(zip(columns_info, r)), cursor)
                for row in rows:
                    message_rows = create_message_rows(table, row, primary_keys)
                    writer.writerows(message_rows)
            except sqlite3.OperationalError as e:
                print(f"ERROR while exporting messages for {table}: {e}", file=sys.stderr)


if __name__ == "__main__":
    prog_parser = ArgumentParser(description="Database table export utility")
    sub_parsers = prog_parser.add_subparsers(help="Possible sub-commands")

    sub1 = sub_parsers.add_parser(
        "data",
        description="Export table data",
        help="Export table data. For command-line options, run: `%(prog)s data --help`",
    )

    sub1.add_argument(
        "--raw", action="store_true", help="Include _meta columns in table data export"
    )
    sub1.add_argument("--nosort", action="store_true", help="Do not sort the data by primary key")
    sub1.set_defaults(func=export_data)

    sub2 = sub_parsers.add_parser(
        "messages",
        description="Export error messages",
        help="Export error messages. For command-line options, run: `%(prog)s messages --help`",
    )
    sub2.add_argument("--a1", action="store_true", help="Output error messages in A1 format")
    sub2.set_defaults(func=export_messages)

    for sub in [sub1, sub2]:
        sub.add_argument(
            "db",
            help="""Either a database connection URL or a path to a SQLite database file. In the
            case of a URL, you must use one of the following schemes: potgresql://<URL>
            (for postgreSQL), sqlite://<relative path> or file:<relative path> (for SQLite).
            """,
        )
        sub.add_argument("output_dir", help="The name of the directory in which to save TSV files")
        sub.add_argument(
            "tables", metavar="table", nargs="+", help="The name of a table to export to TSV"
        )

    args = prog_parser.parse_args()
    func = args.func
    args = vars(args)

    if not os.path.isdir(args["output_dir"]):
        print(f"The directory: {args.output_dir} does not exist", file=sys.stderr)
        sys.exit(1)

    db = args["db"]
    params = ""
    if db.startswith("postgresql://"):
        with psycopg2.connect(db) as conn:
            cursor = conn.cursor()
            func(cursor, False, args)
    else:
        m = re.search(r"(^(file:|sqlite://))?(.+?)(\?.+)?$", db)
        if m:
            path = m[3]
            if not os.path.exists(path):
                print(f"The database '{path}' does not exist.", file=sys.stderr)
                sys.exit(1)
            params = m[4] or ""
            db = f"file:{path}{params}"
            with sqlite3.connect(db) as conn:
                cursor = conn.cursor()
                func(cursor, True, args)
        else:
            print(f"Could not parse database specification: {db}", file=sys.stderr)
            sys.exit(1)
