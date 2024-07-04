#!/usr/bin/env sh

db=$1
table=$2

psql $db < test/output/$table.sql
