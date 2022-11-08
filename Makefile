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

test: sqlite_test pg_test api_test

tables_to_test = column datatype rule table table1 table2 table3 table4 table5

sqlite_test: build/valve.db | test/output
	@echo "Testing valve on sqlite ..."
	test/round_trip.sh $<
	scripts/export.py messages build/valve.db test/output/ $(tables_to_test)
	diff --strip-trailing-cr -q test/expected/messages.tsv test/output/messages.tsv
	@echo "Test succeeded!"

pg_test: valve test/src/table.tsv | test/output
	@echo "Testing valve on postgresql ..."
	# This target assumes that we have a postgresql server, accessible by the current user via the
	# UNIX socket /var/run/postgresql, in which a database called `valve_postgres` has been created.
	# It also requires that `psycopg2` has been installed.
	./$^ postgresql:///valve_postgres > /dev/null
	test/round_trip.sh postgresql:///valve_postgres
	scripts/export.py messages postgresql:///valve_postgres test/output/ $(tables_to_test)
	diff --strip-trailing-cr -q test/expected/messages.tsv test/output/messages.tsv
	@echo "Test succeeded!"

api_test: valve test/src/table.tsv build/valve.db test/insert_update.sh | test/output
	@echo "Testing API functions on sqlite and postgresql ..."
	./$< $(word 2,$^) postgresql:///valve_postgres > /dev/null
	./$< --api_test $(word 2,$^) postgresql:///valve_postgres
	./$< --api_test $(word 2,$^) $(word 3,$^)
	$(word 4,$^) postgresql:///valve_postgres
	$(word 4,$^) $(word 3,$^)
	@echo "Test succeeded!"

clean:
	rm -Rf build test/output

cleanall: clean
	cargo clean
