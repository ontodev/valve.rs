#!/usr/bin/env python3

import json
import sqlite3

from argparse import ArgumentParser


def load_messages(cursor):
    headers = ['"table"', '"row"', '"column"', '"nullvalue"', '"valid"']
    select = ", ".join(headers)
    cursor.execute(f"SELECT {select} FROM cell WHERE valid = FALSE")
    rows = [row for row in map(lambda r: dict(zip(headers, r)), cursor)]
    for row in rows:
        table = row['"table"']
        column = row['"column"']
        row = row['"row"']
        oldtable = table.removesuffix("_alt")
        query = f"""SELECT JSON_EXTRACT("{column}_meta", \'$.messages\') AS messages
                    FROM "{oldtable}_view"
                    WHERE row_number = {row}"""
        cursor.execute(query)
        result = [row for row in map(lambda r: dict(zip(["messages"], r)), cursor)][0]["messages"]
        messages = json.loads(result)
        for m in messages:
            level = m["level"]
            rule = m["rule"]
            message = m["message"]
            insert = f"""INSERT INTO message ("table", "row", "column", "level", "rule", "message")
                         VALUES ('{table}', '{row}', '{column}', '{level}', '{rule}', '{message}')
            """
            cursor.execute(insert)


def main():
    parser = ArgumentParser()
    parser.add_argument("db", help="The database to query against")
    args = parser.parse_args()
    with sqlite3.connect(args.db) as conn:
        cursor = conn.cursor()
        load_messages(cursor)


if __name__ == "__main__":
    main()
