#!/usr/bin/env sh

db=$1
table=$2

psql $db<<EOF
drop table if exists $table;
create table $table (
  study_name text,
  row_number bigint,
  sample_number integer,
  species text,
  region text,
  island text,
  stage text,
  individual_id text,
  clutch_completion text,
  date_egg text,
  culmen_length numeric,
  culmen_depth numeric,
  flipper_length integer,
  body_mass integer,
  sex text,
  delta_15_n numeric,
  delta_13_c numeric,
  comments text
);
EOF
