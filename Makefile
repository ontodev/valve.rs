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

time:
	cargo build --release
	time cargo run --release TestData/build/table.tsv build

test:
	cargo run src/table.tsv build | sort > actual_output.txt && diff -q expected_output.txt actual_output.txt


clean:
	rm -Rf build
