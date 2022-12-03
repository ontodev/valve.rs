#!/usr/bin/env python3

import json
import psycopg2
import os
import re
import sqlite3
import sys
import time

from argparse import ArgumentParser


def test_query(cursor, query, headers, runs, vacuum):
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
            result["rows"] = [row for row in map(lambda r: dict(zip(headers, r)), cursor)]
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


def count(cursor, table, table_suffix, column, runs, vacuum, like=""):
    table = f"{table}_{table_suffix}" if table_suffix else table
    query = f"SELECT COUNT(1) FROM {table}"
    if like:
        query = f"{query} WHERE {column} LIKE '{like}%'"
    headers = ["count"]
    return test_query(cursor, query, headers, runs, vacuum)


def count_view(cursor, table, column, runs, vacuum, like=""):
    return count(cursor, table, "view", column, runs, vacuum, like)


def count_separate(cursor, table, column, runs, vacuum, like=""):
    return {
        "normal": count(cursor, table, "", column, runs, vacuum, like),
        "conflict": count(cursor, table, "conflict", column, runs, vacuum, like),
    }


def json_simple(cursor, table, table_suffix, limit, offset, runs, vacuum):
    table = f"{table}_{table_suffix}" if table_suffix else table
    cursor.execute(f'PRAGMA TABLE_INFO("{table}")')
    columns_info = [d[0] for d in cursor.description]
    pragma_rows = list(map(lambda r: dict(zip(columns_info, r)), cursor))
    headers = [f'"{p["name"]}"' for p in pragma_rows if not p["name"].endswith("_meta")]
    select = ", ".join(headers)
    query = f"SELECT {select} FROM {table} LIMIT {limit}"
    if offset:
        query = f"{query} OFFSET {offset}"
    return test_query(cursor, query, headers, runs, vacuum)


def json_simple_view(cursor, table, limit, offset, runs, vacuum):
    return json_simple(cursor, table, "view", limit, offset, runs, vacuum)


def json_simple_separate(cursor, table, limit, offset, runs, vacuum):
    return {
        "normal": json_simple(cursor, table, "", limit, offset, runs, vacuum),
        "conflict": json_simple(cursor, table, "conflict", limit, offset, runs, vacuum),
    }


def json_cell(cursor, table, table_suffix, limit, offset, runs, vacuum):
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

    return test_query(cursor, query, headers, runs, vacuum)


def json_cell_view(cursor, table, limit, offset, runs, vacuum):
    return json_cell(cursor, table, "view", limit, offset, runs, vacuum)


def json_cell_separate(cursor, table, limit, offset, runs, vacuum):
    return {
        "normal": json_cell(cursor, table, "", limit, offset, runs, vacuum),
        "conflict": json_cell(cursor, table, "conflict", limit, offset, runs, vacuum),
    }


def json_errors_cell(cursor, table, table_suffix, limit, offset, runs, vacuum):
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

    return test_query(cursor, query, headers, runs, vacuum)


def json_errors_cell_view(cursor, table, limit, offset, runs, vacuum):
    return json_errors_cell(cursor, table, "view", limit, offset, runs, vacuum)


def json_errors_cell_separate(cursor, table, limit, offset, runs, vacuum):
    return {
        "normal": json_errors_cell(cursor, table, "", limit, offset, runs, vacuum),
        "conflict": json_errors_cell(cursor, table, "conflict", limit, offset, runs, vacuum),
    }


def query_tests(cursor, table, column, like, limit, offset, runs, vacuum):
    result = {
        "A_count_view_all": count_view(cursor, table, column, runs, vacuum),
        "B_count_separate_all": count_separate(cursor, table, column, runs, vacuum),
        "C_count_view_like": count_view(cursor, table, column, runs, vacuum, like),
        "D_count_separate_like": count_separate(cursor, table, column, runs, vacuum, like),
        "E_json_simple_view": json_simple_view(cursor, table, limit, 0, runs, vacuum),
        "F_json_simple_separate": json_simple_separate(cursor, table, limit, 0, runs, vacuum),
        "G_json_simple_view_offset": json_simple_view(cursor, table, limit, offset, runs, vacuum),
        "H_json_simple_separate_offset": json_simple_separate(
            cursor, table, limit, offset, runs, vacuum
        ),
        "I_json_cell_view": json_cell_view(cursor, table, limit, 0, runs, vacuum),
        "J_json_cell_separate": json_cell_separate(cursor, table, limit, 0, runs, vacuum),
        "K_json_cell_view_offset": json_cell_view(cursor, table, limit, offset, runs, vacuum),
        "L_json_cell_separate_offset": json_cell_separate(
            cursor, table, limit, offset, runs, vacuum
        ),
        "M_json_errors_cell_view": json_errors_cell_view(cursor, table, limit, 0, runs, vacuum),
        "N_json_errors_cell_separate": json_errors_cell_separate(
            cursor, table, limit, 0, runs, vacuum
        ),
        "O_json_errors_cell_view_offset": json_errors_cell_view(
            cursor, table, limit, offset, runs, vacuum
        ),
        "P_json_errors_cell_separate_offset": json_errors_cell_separate(
            cursor, table, limit, offset, runs, vacuum
        ),
    }
    print(json.dumps(result))


def main():
    parser = ArgumentParser()
    parser.add_argument("--vacuum", action="store_true", help="Clear cache after every query")
    parser.add_argument("runs", help="The number of times to repeat each test")
    parser.add_argument("table", help="The name of a table to run the tests on")
    parser.add_argument("column", help="The name of the column to run the tests on")
    parser.add_argument("like", help="The initial substring to match the column value against")
    parser.add_argument("limit", help="Maximum number of rows to fetch in LIMIT queries")
    parser.add_argument("offset", help="Offset to use for LIMIT queries")
    parser.add_argument("db", help="The database to query against")
    args = parser.parse_args()

    runs = int(args.runs)
    table = args.table
    column = args.column
    like = args.like
    limit = int(args.limit)
    offset = int(args.offset)
    db = args.db
    vacuum = args.vacuum
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
                query_tests(cursor, table, column, like, limit, offset, runs, vacuum)
        else:
            print(f"Could not parse database specification: {db}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
