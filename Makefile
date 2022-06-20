MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := test
.DELETE_ON_ERROR:
.SUFFIXES:

build:
	mkdir build

.PHONY: run time test

run:
	cargo run src/table.tsv build

time: clean | build
	cargo build --release
	time cargo run --release TestData/build/table.tsv build

test/output:
	mkdir -p test/output

test: clean | build test/output
	cargo run test/src/table.tsv build > /dev/null
	test/test_round_trip.sh
	scripts/export.py messages build/valve.db test/output/ column datatype prefix rule table foobar foreign_table import
	diff -q test/expected/messages.tsv test/output/messages.tsv
	cargo run -- --test test/src/table.tsv build > /dev/null
	test/test_insert_update.sh


clean:
	rm -Rf build test/output
