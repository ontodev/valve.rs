MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := run
.DELETE_ON_ERROR:
.SUFFIXES:

build:
	mkdir build

.PHONY: run run_release

run: src/table.tsv | build
	cargo $@ $^ $|

run_release: src/table.tsv | build
	cargo $@ --release $^ $|

clean:
	rm -Rf build
