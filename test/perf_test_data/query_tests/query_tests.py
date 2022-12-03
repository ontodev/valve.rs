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


def test_query(cursor, query, headers, num_repetitions, vacuum):
    min_time = -1
    max_time = 0
    total_time = 0
    result = {"query": query, "runs": num_repetitions, "rows": []}
    for i in range(1, num_repetitions + 1):
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
    avg_time = total_time / num_repetitions
    result["avg_time"] = f"{avg_time:06f}"
    return result


def count_view(cursor, table, column, num_repetitions, vacuum, like=""):
    query = f"SELECT COUNT(1) FROM {table}_view"
    if like:
        query = f"{query} WHERE {column} LIKE '{like}%'"
    headers = ["count"]
    return test_query(cursor, query, headers, num_repetitions, vacuum)


def json_simple_view(cursor, table, num_rows, offset, num_repetitions, vacuum):
    cursor.execute(f'PRAGMA TABLE_INFO("{table}")')
    columns_info = [d[0] for d in cursor.description]
    pragma_rows = list(map(lambda r: OrderedDict(zip(columns_info, r)), cursor))
    headers = [p["name"] for p in pragma_rows if not p["name"].endswith("_meta")]
    select = ", ".join(headers)
    query = f"SELECT {select} FROM {table}_view LIMIT {num_rows}"
    return test_query(cursor, query, headers, num_repetitions, vacuum)


def query_tests(cursor, table, column, like, num_repetitions, vacuum):
    result = {
        "count_view_all": count_view(cursor, table, column, num_repetitions, vacuum),
        "count_view_like": count_view(cursor, table, column, num_repetitions, vacuum, like),
        "json_simple_view": json_simple_view(cursor, table, 100, 0, num_repetitions, vacuum),
    }
    print(pformat(json.loads(json.dumps(result))))


def main():
    parser = ArgumentParser()
    parser.add_argument("--vacuum", action="store_true", help="Clear cache after every query")
    parser.add_argument("num_repetitions", help="The number of times to repeat each test")
    parser.add_argument("table", help="The name of a table to run the tests on")
    parser.add_argument("column", help="The name of the column to run the tests on")
    parser.add_argument("like", help="The initial substring to match the column value against")
    parser.add_argument("db", help="The database to query against")
    args = parser.parse_args()

    num_repetitions = int(args.num_repetitions)
    table = args.table
    column = args.column
    like = args.like
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
                query_tests(cursor, table, column, like, num_repetitions, vacuum)
        else:
            print(f"Could not parse database specification: {db}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
