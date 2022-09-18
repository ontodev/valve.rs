MAKEFLAGS += --warn-undefined-variables
.DEFAULT_GOAL := valve
.DELETE_ON_ERROR:
.SUFFIXES:

build:
	mkdir build

.PHONY: doc time test sqlite_test pg_test

doc:
	cargo doc --document-private-items

readme:
	cargo readme --no-title > README.md

valve: src/*.rs src/*.lalrpop
	rm -f valve
	cargo build --release
	ln -s target/release/ontodev_valve valve
	# cargo build
	# ln -s target/debug/ontodev_valve valve

build/valve.db: test/src/table.tsv valve clean | build
	./valve $< $@ > /dev/null

time: clean valve | build
	time ./valve TestData/build/table.tsv build/valve.db

test/output:
	mkdir -p test/output

test: sqlite_test pg_test

sqlite_test: build/valve.db | build test/output
	test/round_trip.sh $<
	scripts/export.py messages build/valve.db test/output/ column datatype prefix rule table foobar foreign_table import
	diff --strip-trailing-cr -q test/expected/messages.tsv test/output/messages.tsv

pg_test: valve test/src/table.tsv | test/output
	# This target assumes that we have a postgresql server, accessible by the current user via the
	# UNIX socket /var/run/postgresql, in which a database called `valve_postgres` has been created.
	# It also requires that `psycopg2` has been installed.
	./$^ postgresql:///valve_postgres
	test/round_trip.sh postgresql:///valve_postgres
	scripts/export.py messages postgresql:///valve_postgres test/output/ column datatype prefix rule table foobar foreign_table import
	diff --strip-trailing-cr -q test/expected/messages.tsv test/output/messages.tsv

clean:
	rm -Rf build test/output

cleanall: clean
	cargo clean
