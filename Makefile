MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := run
.DELETE_ON_ERROR:
.SUFFIXES:

build:
	mkdir build

run: src/table.tsv | build
	cargo $@ $^ $|

clean:
	rm -Rf build
