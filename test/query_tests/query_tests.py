#!/usr/bin/env python3

import json
import psycopg2
import os
import re
import sqlite3
import sys
import time

from argparse import ArgumentParser

vacuum = False
include_rows = False


def test_query(cursor, query, headers, runs):
    min_time = -1
    max_time = 0
    total_time = 0
    result = {"query": query, "runs": runs, "rows": []}
    for i in range(1, runs + 1):
        start = time.perf_counter()
        cursor.execute(query)
        end = time.perf_counter()
        execution_time = end - start
        if not result["rows"]:
            if include_rows:
                result["rows"] = [row for row in map(lambda r: dict(zip(headers, r)), cursor)]
            else:
                result["rows"] = len([row for row in map(lambda r: dict(zip(headers, r)), cursor)])

        if min_time == -1 or execution_time < min_time:
            min_time = execution_time
        if execution_time > max_time:
            max_time = execution_time
        total_time += execution_time
        if vacuum:
            # Clear the cache after every query for more meaningful performance tests:
            cursor.execute("vacuum")

    result["min_time"] = f"{min_time:06f}"
    result["max_time"] = f"{max_time:06f}"
    avg_time = total_time / runs
    result["avg_time"] = f"{avg_time:06f}"
    return result


def count(cursor, table, table_suffix, column, runs, like=""):
    table = f"{table}_{table_suffix}" if table_suffix else table
    query = f"SELECT COUNT(1) FROM {table}"
    if like:
        query = f"{query} WHERE {column} LIKE '{like}%'"
    headers = ["count"]
    return test_query(cursor, query, headers, runs)


def count_view(cursor, table, column, runs, like=""):
    return count(cursor, table, "view", column, runs, like)


def count_separate(cursor, table, column, runs, like=""):
    return {
        "normal": count(cursor, table, "", column, runs, like),
        "conflict": count(cursor, table, "conflict", column, runs, like),
    }


def json_simple(cursor, table, table_suffix, limit, offset, runs):
    table = f"{table}_{table_suffix}" if table_suffix else table
    cursor.execute(f'PRAGMA TABLE_INFO("{table}")')
    columns_info = [d[0] for d in cursor.description]
    pragma_rows = list(map(lambda r: dict(zip(columns_info, r)), cursor))
    headers = [f'"{p["name"]}"' for p in pragma_rows if not p["name"].endswith("_meta")]
    select = ", ".join(headers)
    query = f"SELECT {select} FROM {table} LIMIT {limit}"
    if offset:
        query = f"{query} OFFSET {offset}"
    return test_query(cursor, query, headers, runs)


def json_simple_view(cursor, table, limit, offset, runs):
    return json_simple(cursor, table, "view", limit, offset, runs)


def json_simple_separate(cursor, table, limit, offset, runs):
    return {
        "normal": json_simple(cursor, table, "", limit, offset, runs),
        "conflict": json_simple(cursor, table, "conflict", limit, offset, runs),
    }


def json_cell(cursor, table, table_suffix, limit, offset, runs):
    table = f"{table}_{table_suffix}" if table_suffix else table
    cursor.execute(f'PRAGMA TABLE_INFO("{table}")')
    columns_info = [d[0] for d in cursor.description]
    pragma_rows = list(map(lambda r: dict(zip(columns_info, r)), cursor))
    headers = []
    select = []
    for p in pragma_rows:
        column = p["name"]
        if column == "row_number":
            headers.append(column)
            select.append(column)
        elif column.endswith("_meta"):
            headers.append(column)
            select.append(
                f'CASE WHEN "{column}" IS NOT NULL THEN JSON("{column}") '
                f'ELSE JSON(\'{{"valid": true, "messages": []}}\') '
                f'END AS "{column}"'
            )
    select = ", ".join(select)
    query = f"SELECT {select} FROM {table} LIMIT {limit}"
    if offset:
        query = f"{query} OFFSET {offset}"

    return test_query(cursor, query, headers, runs)


def json_cell_view(cursor, table, limit, offset, runs):
    return json_cell(cursor, table, "view", limit, offset, runs)


def json_cell_separate(cursor, table, limit, offset, runs):
    return {
        "normal": json_cell(cursor, table, "", limit, offset, runs),
        "conflict": json_cell(cursor, table, "conflict", limit, offset, runs),
    }


def alt_json_cell(cursor, table, table_suffix, limit, offset, runs):
    cursor.execute(f'PRAGMA TABLE_INFO("{table}")')
    columns_info = [d[0] for d in cursor.description]
    pragma_rows = list(map(lambda r: dict(zip(columns_info, r)), cursor))
    columns = [p["name"] for p in pragma_rows if p["name"] != "row_number"]
    main_select = ", ".join(columns)
    main_select = f"""
    WITH hundred (row_number, child, parent, xyzzy, foo, bar) AS (
      SELECT row_number, {main_select}
        FROM {table}_{table_suffix}
      LIMIT 100
    """
    if offset:
        main_select = f"{main_select} OFFSET {offset}"
    main_select = f"""
    {main_select}
    )"""

    sub_selects = []
    for column in columns:
        sub_selects.append(
            f"""
        SELECT h.row_number,
               '{column}' AS "column",
               h.{column} AS value,
               m.level,
               m.rule,
               m.message
        FROM hundred h
             LEFT JOIN message m ON h.row_number = m.row AND m."table" = '{table}'
                                  AND m."column" = '{column}'
        """
        )

    query = (
        main_select
        + """
    UNION ALL
    """.join(
            sub_selects
        )
    )
    headers = ["row_number", "column", "value", "valid", "level", "rule", "message"]
    return test_query(cursor, query, headers, runs)


def alt_json_errors_cell(cursor, table, table_suffix, limit, offset, runs):
    cursor.execute(f'PRAGMA TABLE_INFO("{table}")')
    columns_info = [d[0] for d in cursor.description]
    pragma_rows = list(map(lambda r: dict(zip(columns_info, r)), cursor))
    columns = [p["name"] for p in pragma_rows if p["name"] != "row_number"]
    main_select = ", ".join(columns)
    main_select = f"""
    WITH hundred (row_number, child, parent, xyzzy, foo, bar) AS (
      SELECT row_number, {main_select}
        FROM {table}_{table_suffix} t
      WHERE EXISTS (
        SELECT 1
        FROM message m
        WHERE m."table" = '{table}'
        AND m.row = t.row_number
      )
      LIMIT 100
    """
    if offset:
        main_select = f"{main_select} OFFSET {offset}"
    main_select = f"""
    {main_select}
    )"""

    sub_selects = []
    for column in columns:
        sub_selects.append(
            f"""
        SELECT h.row_number,
               '{column}' AS "column",
               h.{column} AS value,
               m.level,
               m.rule,
               m.message
        FROM hundred h
             LEFT JOIN message m ON h.row_number = m.row AND m."table" = '{table}'
                                  AND m."column" = '{column}'
        """
        )

    query = (
        main_select
        + """
    UNION ALL
    """.join(
            sub_selects
        )
    )
    headers = ["row_number", "column", "value", "valid", "level", "rule", "message"]
    return test_query(cursor, query, headers, runs)


def alt_json_cell_view(cursor, table, limit, offset, runs):
    return alt_json_cell(cursor, table, "view", limit, offset, runs)


def alt_json_errors_cell_view(cursor, table, limit, offset, runs):
    return alt_json_errors_cell(cursor, table, "view", limit, offset, runs)


def json_errors_cell(cursor, table, table_suffix, limit, offset, runs):
    table = f"{table}_{table_suffix}" if table_suffix else table
    cursor.execute(f'PRAGMA TABLE_INFO("{table}")')
    columns_info = [d[0] for d in cursor.description]
    pragma_rows = list(map(lambda r: dict(zip(columns_info, r)), cursor))
    headers = []
    where = []
    select = []
    for p in pragma_rows:
        column = p["name"]
        if column == "row_number":
            headers.append(column)
            select.append(column)
        elif column.endswith("_meta"):
            headers.append(column)
            select.append(
                f'CASE WHEN "{column}" IS NOT NULL THEN JSON("{column}") '
                f'ELSE JSON(\'{{"valid": true, "messages": []}}\') '
                f'END AS "{column}"'
            )
            where.append(f'"{column}" IS NOT NULL')

    select = ", ".join(select)
    where = " OR ".join(where)
    query = f"SELECT {select} FROM {table} WHERE {where} LIMIT {limit}"
    if offset:
        query = f"{query} OFFSET {offset}"

    return test_query(cursor, query, headers, runs)


def json_errors_cell_view(cursor, table, limit, offset, runs):
    return json_errors_cell(cursor, table, "view", limit, offset, runs)


def json_errors_cell_separate(cursor, table, limit, offset, runs):
    return {
        "normal": json_errors_cell(cursor, table, "", limit, offset, runs),
        "conflict": json_errors_cell(cursor, table, "conflict", limit, offset, runs),
    }


def query_tests(cursor, table, column, like, limit, offset, runs):
    result = {
        "A_count_view_all": count_view(cursor, table, column, runs),
        "B_count_separate_all": count_separate(cursor, table, column, runs),
        "C_count_view_like": count_view(cursor, table, column, runs, like),
        "D_count_separate_like": count_separate(cursor, table, column, runs, like),
        "E_json_simple_view": json_simple_view(cursor, table, limit, 0, runs),
        "F_json_simple_separate": json_simple_separate(cursor, table, limit, 0, runs),
        "G_json_simple_view_offset": json_simple_view(cursor, table, limit, offset, runs),
        "H_json_simple_separate_offset": json_simple_separate(cursor, table, limit, offset, runs),
        # Cells
        "I_json_cell_view": json_cell_view(cursor, table, limit, 0, runs),
        "J_json_cell_separate": json_cell_separate(cursor, table, limit, 0, runs),
        "K_json_cell_view_offset": json_cell_view(cursor, table, limit, offset, runs),
        "L_json_cell_separate_offset": json_cell_separate(cursor, table, limit, offset, runs),
        "M_json_errors_cell_view": json_errors_cell_view(cursor, table, limit, 0, runs),
        "N_json_errors_cell_separate": json_errors_cell_separate(cursor, table, limit, 0, runs),
        "O_json_errors_cell_view_offset": json_errors_cell_view(cursor, table, limit, offset, runs),
        "P_json_errors_cell_separate_offset": json_errors_cell_separate(
            cursor, table, limit, offset, runs
        ),
        # Cells (alternate schema): value, nulltype, valid, level, rule, message
        "Q_alt_json_cell_view": alt_json_cell_view(cursor, f"{table}_alt", limit, 0, runs),
        "R_alt_json_cell_view_offset": alt_json_cell_view(
            cursor, f"{table}_alt", limit, offset, runs
        ),
        "S_alt_json_errors_cell_view": alt_json_errors_cell_view(
            cursor, f"{table}_alt", limit, 0, runs
        ),
        "T_alt_json_errors_cell_view_offset": alt_json_errors_cell_view(
            cursor, f"{table}_alt", limit, offset, runs
        ),
    }
    for test in result:
        print(f"Test: {test}")
        for key in result[test]:
            print(f"{key}: {result[test][key]}")
        print()


def main():
    parser = ArgumentParser()
    parser.add_argument("--vacuum", action="store_true", help="Clear cache after every query")
    parser.add_argument("--include_rows", action="store_true", help="Include rows in result")
    parser.add_argument("runs", help="The number of times to repeat each test")
    parser.add_argument("table", help="The name of a table to run the tests on")
    parser.add_argument("column", help="The name of the column to run the tests on")
    parser.add_argument("like", help="The initial substring to match the column value against")
    parser.add_argument("limit", help="Maximum number of rows to fetch in LIMIT queries")
    parser.add_argument("offset", help="Offset to use for LIMIT queries")
    parser.add_argument("db", help="The database to query against")
    args = parser.parse_args()

    global vacuum
    global include_rows

    runs = int(args.runs)
    table = args.table
    column = args.column
    like = args.like
    limit = int(args.limit)
    offset = int(args.offset)
    db = args.db
    vacuum = args.vacuum
    include_rows = args.include_rows
    params = ""
    if db.startswith("postgresql://"):
        with psycopg2.connect(db) as conn:
            # cursor = conn.cursor()
            print("PostgreSQL is not yet supported.")
            sys.exit(1)
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
                query_tests(cursor, table, column, like, limit, offset, runs)
        else:
            print(f"Could not parse database specification: {db}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
