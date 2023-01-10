#!/usr/bin/env bash

if [[ $# -lt 2 ]]
then
    echo "Usage: $(basename $0) DATABASE TABLE_CONFIG"
    exit 1
fi

db=$1
table_defs=$2
shift 2
if [[ $# -gt 0 ]]
then
    echo "Warning: Extra arguments: '$*' will be ignored"
fi

pwd=$(dirname $(readlink -f $0))
export_script=$pwd/../scripts/export.py
output_dir=$pwd/output

num_tables=$(expr $(cat $table_defs | wc -l) - 1)
table_paths=$(tail -$num_tables $table_defs | cut -f 2)

ret_value=0
for table_path in $table_paths
do
    table_path=${table_path#test/}
    table_path=$pwd/$table_path
    table_file=$(basename $table_path)
    table=${table_file%.*}
    ${export_script} data $db $output_dir $table
    diff --strip-trailing-cr -q ${table_path} $output_dir/${table}.tsv
    ret_value=$(expr $ret_value + $?)
done

exit $ret_value
