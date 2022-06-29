MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := build/valve.db
.DELETE_ON_ERROR:
.SUFFIXES:

build:
	mkdir build

.PHONY: doc time test

doc:
	cargo doc --document-private-items

readme:
	cargo readme --no-title > README.md

build/valve.db: test/src/table.tsv | build
	cargo run $< $@

time: clean | build
	cargo build --release
	time cargo run --release TestData/build/table.tsv build/valve.db

test/output:
	mkdir -p test/output

test: clean | build test/output
	cargo run test/src/table.tsv build/valve.db > /dev/null
	test/round_trip.sh
	scripts/export.py messages build/valve.db test/output/ column datatype prefix rule table foobar foreign_table import
	diff -q test/expected/messages.tsv test/output/messages.tsv

clean:
	rm -Rf build test/output
