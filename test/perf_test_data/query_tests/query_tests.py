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


def count(cursor, table, column, num_repetitions, like=""):
    min_time = -1
    max_time = 0
    total_time = 0
    query = f"SELECT COUNT(1) FROM {table}_view"
    if like:
        query = f"{query} WHERE {column} LIKE '{like}%'"
    headers = ["count"]
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
        # Clear the cache after every query for more meaningful performance tests:
        cursor.execute("vacuum")

    result["min_time"] = f"{min_time:06f}"
    result["max_time"] = f"{max_time:06f}"
    avg_time = total_time / num_repetitions
    result["avg_time"] = f"{avg_time:06f}"
    return result


def query_tests(cursor, table, column, like, num_repetitions):
    result = {
        "count_all": count(cursor, table, column, num_repetitions),
        "count_like": count(cursor, table, column, num_repetitions, like),
    }
    print(pformat(json.loads(json.dumps(result))))


def main():
    parser = ArgumentParser()
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
                query_tests(cursor, table, column, like, num_repetitions)
        else:
            print(f"Could not parse database specification: {db}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
