#!/usr/bin/env bash

if [[ $# -lt 1 ]]
then
    echo "Usage: $(basename $0) DATABASE"
    exit 1
fi

db=$1
shift
if [[ $# -gt 0 ]]
then
    echo "Warning: Extra arguments: '$*' will be ignored"
fi

pwd=$(dirname $(readlink -f $0))
export_script=$pwd/../scripts/export.py
output_dir=$pwd/output
expected_dir=$pwd/expected

ret_value=0
for table_path in import.tsv foobar.tsv
do
    table_path=${table_path#test/}
    table_path=$pwd/output/$table_path
    table_file=$(basename $table_path)
    table=${table_file%.*}
    ${export_script} data --nosort $db $output_dir $table
    diff -q $expected_dir/${table}.tsv ${table_path}
    ret_value=$(expr $ret_value + $?)
done

exit $ret_value
