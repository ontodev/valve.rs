#!/usr/bin/env bash

pwd=$(dirname $(readlink -f $0))
table_defs=$pwd/src/table.tsv
export_script=$pwd/../scripts/export.py
db=$pwd/../build/cmi-pb.db
output_dir=$pwd/output

num_tables=$(expr $(cat $table_defs | wc -l) - 1)
table_paths=$(tail -$num_tables $table_defs | cut -f 2)
for table_path in $table_paths
do
    table_path=${table_path#test/}
    table_path=$pwd/$table_path
    table_file=$(basename $table_path)
    table=${table_file%.*}
    ${export_script} data --nosort $db $output_dir $table
    diff -q ${table_path} $output_dir/${table}.tsv
done
