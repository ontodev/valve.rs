#!/usr/bin/env sh

db=$1
table=$2

sqlite3 $db<<EOF
.mode tabs
.import test/src/ontology/$table.tsv $table
EOF
