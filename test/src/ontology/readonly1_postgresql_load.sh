#!/usr/bin/env sh

db=$1
table=$2

psql $db<<EOF
\COPY $table FROM \'test/src/ontology/$table\' WITH CSV HEADER DELIMITER E'\t';
EOF
