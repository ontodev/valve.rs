#!/usr/bin/env python3

import json
import psycopg2
import os
import re
import sqlite3
import sys
import time

from argparse import ArgumentParser
from collections import OrderedDict
from pprint import pformat


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
            result["rows"] = [row for row in map(lambda r: OrderedDict(zip(headers, r)), cursor)]
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


def count_view(cursor, table, column, runs, vacuum, like=""):
    query = f"SELECT COUNT(1) FROM {table}_view"
    if like:
        query = f"{query} WHERE {column} LIKE '{like}%'"
    headers = ["count"]
    return test_query(cursor, query, headers, runs, vacuum)


def json_simple_view(cursor, table, limit, offset, runs, vacuum):
    cursor.execute(f'PRAGMA TABLE_INFO("{table}")')
    columns_info = [d[0] for d in cursor.description]
    pragma_rows = list(map(lambda r: OrderedDict(zip(columns_info, r)), cursor))
    headers = [p["name"] for p in pragma_rows if not p["name"].endswith("_meta")]
    select = ", ".join(headers)
    query = f"SELECT {select} FROM {table}_view LIMIT {limit}"
    if offset:
        query = f"{query} OFFSET {offset}"
    return test_query(cursor, query, headers, runs, vacuum)


def json_full_view(cursor, table, limit, offset, runs, vacuum):
    cursor.execute(f'PRAGMA TABLE_INFO("{table}")')
    columns_info = [d[0] for d in cursor.description]
    pragma_rows = list(map(lambda r: OrderedDict(zip(columns_info, r)), cursor))
    headers = []
    select = []
    for p in pragma_rows:
        column = p["name"]
        if column == "row_number":
            headers.append(column)
        elif column.endswith("_meta"):
            headers.append(column)
            select.append(
                f'CASE WHEN "{column}" IS NOT NULL THEN JSON("{column}") '
                f'ELSE JSON(\'{{"valid": true, "messages": []}}\') '
                f'END AS "{column}"'
            )
    select = ", ".join(select)
    query = f"SELECT {select} FROM {table}_view LIMIT {limit}"
    if offset:
        query = f"{query} OFFSET {offset}"

    return test_query(cursor, query, headers, runs, vacuum)


def query_tests(cursor, table, column, like, limit, offset, runs, vacuum):
    result = {
        "count_view_all": count_view(cursor, table, column, runs, vacuum),
        "count_view_like": count_view(cursor, table, column, runs, vacuum, like),
        "json_simple_view": json_simple_view(cursor, table, limit, 0, runs, vacuum),
        "json_simple_view_offset": json_simple_view(cursor, table, limit, offset, runs, vacuum),
        "json_full_view": json_full_view(cursor, table, limit, 0, runs, vacuum),
        "json_full_view_offset": json_full_view(cursor, table, limit, offset, runs, vacuum),
    }
    print(pformat(json.loads(json.dumps(result))))


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
