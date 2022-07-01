MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := valve
.DELETE_ON_ERROR:
.SUFFIXES:

build:
	mkdir build

.PHONY: doc time test

doc:
	cargo doc --document-private-items

readme:
	cargo readme --no-title > README.md

valve: src/*.rs src/*.lalrpop
	rm -f valve
	cargo build --release
	ln -s target/release/ontodev_valve valve

build/valve.db: test/src/table.tsv valve clean | build
	valve $< $@ > /dev/null

time: clean valve | build
	time valve TestData/build/table.tsv build/valve.db

test/output:
	mkdir -p test/output

test: build/valve.db | build test/output
	test/round_trip.sh
	scripts/export.py messages build/valve.db test/output/ column datatype prefix rule table foobar foreign_table import
	diff -q test/expected/messages.tsv test/output/messages.tsv

clean:
	rm -Rf build test/output

cleanall: clean
	cargo clean
