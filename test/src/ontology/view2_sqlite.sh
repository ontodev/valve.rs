#!/usr/bin/env sh

db=$1
table=$2

sqlite3 $db < test/output/$table.sql
