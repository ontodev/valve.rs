#!/usr/bin/env bash

pwd=$(dirname $(readlink -f $0))
export_script=$pwd/../scripts/export.py
db=$pwd/../build/valve.db
output_dir=$pwd/output
expected_dir=$pwd/expected

for table_path in import.tsv foobar.tsv
do
    table_path=${table_path#test/}
    table_path=$pwd/output/$table_path
    table_file=$(basename $table_path)
    table=${table_file%.*}
    ${export_script} data --nosort $db $output_dir $table
    diff -q $expected_dir/${table}.tsv ${table_path}
done
