#!/usr/bin/env python3

import csv
import os.path
import psycopg2
import re
import sqlite3
import sys

from argparse import ArgumentParser


def get_column_order_and_info_for_postgres(cursor, table):
    """
    Given a database cursor (for a PostgreSQL database) and a table name, returns a dictionary
    consisting of an unsorted and a sorted list of column names, a list of the table's primary keys
    sorted by priority, and a list of the table's unique keys. I.e., returns a dict of the form:
    {"unsorted_columns": [], "sorted_columns": [], "primary_keys": [], "unique_keys": []}. Note that
    for tables with primary keys, we sort by primary key first, then by all other columns from left
    to right. For tables without primary keys, we sort by row_number.
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
        unsorted_columns = []
        non_pk_columns = []
        for row in cursor:
            column_name = row[0]
            unsorted_columns.append(column_name)
            if column_name not in primary_keys and not column_name == "row_number":
                non_pk_columns.append(column_name)
        sorted_columns = primary_keys + non_pk_columns

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
    Given a database cursor (for a SQLite database) and a table name, returns a dictionary
    consisting of an unsorted and a sorted list of column names, a list of the table's primary keys
    sorted by priority, and a list of the table's unique keys. I.e., returns a dict of the form:
    {"unsorted_columns": [], "sorted_columns": [], "primary_keys": [], "unique_keys": []}. Note that
    for tables with primary keys, we sort by primary key first, then by all other columns from left
    to right. For tables without primary keys, we sort by row_number.
    """
    cursor.execute(f'PRAGMA TABLE_INFO("{table}")')
    columns_info = [d[0] for d in cursor.description]
    pragma_rows = list(map(lambda r: dict(zip(columns_info, r)), cursor))
    primary_keys = dict()
    if not any([row["pk"] == 1 for row in pragma_rows]):
        sorted_columns = ["row_number"]
        unsorted_columns = [p["name"] for p in pragma_rows]
    else:
        unsorted_columns = []
        non_pk_columns = []
        for row in pragma_rows:
            unsorted_columns.append(row["name"])
            if row["pk"] != 0:
                primary_keys[row["pk"]] = row["name"]
            elif not row["name"] == "row_number":
                non_pk_columns.append(row["name"])
        primary_keys = dict(sorted(primary_keys.items()))
        sorted_columns = [primary_keys[key] for key in primary_keys] + non_pk_columns

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
    dictionary containing: an output directory, "output", and a list of tables, "tables": export all
    of the given database tables to .tsv files in the output directory.
    """
    output_dir = os.path.normpath(args["output_dir"])
    tables = args["tables"]

    for table in tables:
        try:
            if is_sqlite:
                columns_info = get_column_order_and_info_for_sqlite(cursor, table)
            else:
                columns_info = get_column_order_and_info_for_postgres(cursor, table)
            unsorted_columns = columns_info["unsorted_columns"]

            select = []
            for column in unsorted_columns:
                if column == "row_number":
                    select.append(f'"{column}"')
                else:
                    cast = "" if is_sqlite else "::TEXT"
                    select.append(
                        f"""
                        CASE
                          WHEN "{column}" IS NULL THEN (
                            SELECT VALUE
                            FROM "message"
                            WHERE "row" = "row_number"
                              AND "column" = '{column}'
                              AND "table" = '{table}'
                            LIMIT 1
                          )
                          ELSE "{column}"{cast}
                        END AS "{column}"
                        """
                    )
            select = ", ".join(select)

            # Fetch the rows from the table and write them to a corresponding TSV file in the
            # output directory:
            cursor.execute(f'SELECT {select} FROM "{table}_view" ORDER BY "row_number"')
            colnames = [d[0] for d in cursor.description]
            rows = map(lambda r: dict(zip(colnames, r)), cursor)
            fieldnames = [c for c in colnames if c != "row_number"]
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
    dictionary containing: an output directory, "output", a list of tables, "tables", a flag, "pk",
    indicating whether rows should be identified using the primary key value(s) for their table, and
    a flag, "a1", indicating whether to use A1 format: export all of the error messages contained in
    the given database tables to a file called messages.tsv in the output directory.
    """
    output_dir = os.path.normpath(args["output_dir"])
    tables = args["tables"]
    a1 = args["a1"]
    pk = args["pk"]

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

    if a1:
        fieldnames = ["table", "cell", "level", "rule", "message", "value"]
    elif pk:
        fieldnames = ["table", "primary_key", "row", "column", "level", "rule", "message", "value"]
    else:
        fieldnames = ["table", "row", "column", "level", "rule", "message", "value"]
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

        in_clause = [f"'{t}'" for t in tables]
        in_clause = ", ".join(in_clause)
        try:
            if not pk:
                message_select = f"""
                SELECT "table", "row", "column", "level", "rule", "message", "value"
                FROM "message"
                WHERE "table" in ({in_clause})
                ORDER by "table", "row", "column", "rule"
                """
                if not a1:
                    cursor.execute(message_select)
                    message_columns_info = [d[0] for d in cursor.description]
                    message_rows = map(lambda r: dict(zip(message_columns_info, r)), cursor)
                    writer.writerows(message_rows)
                else:
                    table_columns = {}
                    for table in tables:
                        if is_sqlite:
                            columns = get_column_order_and_info_for_sqlite(cursor, table)
                        else:
                            columns = get_column_order_and_info_for_postgres(cursor, table)
                        columns = [c for c in columns["unsorted_columns"] if c != "row_number"]
                        table_columns[table] = columns

                    cursor.execute(message_select)
                    message_columns_info = [d[0] for d in cursor.description]
                    message_rows = map(lambda r: dict(zip(message_columns_info, r)), cursor)
                    a1_rows = []
                    for row in message_rows:
                        columns = table_columns[row["table"]]
                        column_id = col_to_a1(row["column"], columns)
                        row_number = row["row"]
                        column_id = f"{column_id}{row_number}"
                        a1_rows.append(
                            {
                                "table": row["table"],
                                "cell": column_id,
                                "level": row["level"],
                                "rule": row["rule"],
                                "message": row["message"],
                                "value": row["value"],
                            }
                        )
                    writer.writerows(a1_rows)
            else:
                for table in sorted(tables):
                    cast = ""
                    if is_sqlite:
                        columns = get_column_order_and_info_for_sqlite(cursor, table)
                    else:
                        columns = get_column_order_and_info_for_postgres(cursor, table)

                    if not is_sqlite and len(columns["primary_keys"]) > 1:
                        cast = "::TEXT"
                    pk_name = "###".join(columns["primary_keys"])
                    pk_name = f"'{pk_name}'" if pk_name else "'row_number'"
                    pk_value = [f't."{k}"{cast}' for k in columns["primary_keys"]]
                    pk_value = " || '###' || ".join(pk_value) if pk_value else 't."row_number"'
                    message_select = f"""
                    SELECT
                      m."table",
                      {pk_name} AS "primary_key",
                      {pk_value} AS "row",
                      m."column",
                      m."level",
                      m."rule",
                      m."message",
                      m."value"
                    FROM "message" m
                      INNER JOIN "{table}_view" t ON m."row" = t."row_number"
                    WHERE m."table" = '{table}'
                    ORDER by "row", m."column", m."rule"
                    """
                    cursor.execute(message_select)
                    message_columns_info = [d[0] for d in cursor.description]
                    message_rows = map(lambda r: dict(zip(message_columns_info, r)), cursor)
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

    sub1.set_defaults(func=export_data)

    sub2 = sub_parsers.add_parser(
        "messages",
        description="Export error messages",
        help="Export error messages. For command-line options, run: `%(prog)s messages --help`",
    )
    sub2_group = sub2.add_mutually_exclusive_group()
    sub2_group.add_argument("--a1", action="store_true", help="Output error messages in A1 format")
    sub2_group.add_argument("--pk", action="store_true", help="Identify rows using primary keys")
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
            with sqlite3.connect(db, uri=True) as conn:
                cursor = conn.cursor()
                func(cursor, True, args)
        else:
            print(f"Could not parse database specification: {db}", file=sys.stderr)
            sys.exit(1)
