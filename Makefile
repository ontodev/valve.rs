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

target/debug/valve: src/*.rs src/*.lalrpop
	cargo build
	maturin develop

test: clean target/debug/valve | build test/output
	test/main.py --load test/src/table.tsv build > /dev/null
	test/round_trip.sh
	scripts/export.py messages build/valve.db test/output/ column datatype prefix rule table foobar foreign_table import
	diff -q test/expected/messages.tsv test/output/messages.tsv
	test/main.py --insert_update test/src/table.tsv build > /dev/null
	test/insert_update.sh

# For python bindings to work, make sure that you are in a virtualenv that has `maturin` installed:
# python3 -m venv .venv
# source .venv/bin/activate
# pip install -U pip maturin
python: clean | build
	cargo build
	maturin develop
	python3 run_python.py > /dev/null

clean:
	rm -Rf build test/output
