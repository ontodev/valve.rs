MAKEFLAGS += --warn-undefined-variables
.DEFAULT_GOAL := valve
.DELETE_ON_ERROR:
.SUFFIXES:

# NOTE:
# -----
# The test targets assume that we have a postgresql server, accessible by the current user via the
# UNIX socket /var/run/postgresql, in which a database called `valve_postgres` has been created.
# They also requires that `psycopg2` has been installed.

build:
	mkdir build

.PHONY: doc time test sqlite_test pg_test perf_test_data perf_test

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

test/output:
	mkdir -p test/output

test: sqlite_test pg_test api_test

tables_to_test = column datatype rule table table1 table2 table3 table4 table5 table6

sqlite_test: build/valve.db test/src/table.tsv | test/output
	@echo "Testing valve on sqlite ..."
	test/round_trip.sh $^
	scripts/export.py messages $< $| $(tables_to_test)
	diff --strip-trailing-cr -q test/expected/messages.tsv test/output/messages.tsv
	@echo "Test succeeded!"

pg_test: valve test/src/table.tsv | test/output
	@echo "Testing valve on postgresql ..."
	./$^ postgresql:///valve_postgres > /dev/null
	test/round_trip.sh postgresql:///valve_postgres $(word 2,$^)
	scripts/export.py messages postgresql:///valve_postgres $| $(tables_to_test)
	diff --strip-trailing-cr -q test/expected/messages.tsv test/output/messages.tsv
	@echo "Test succeeded!"

api_test: sqlite_api_test pg_api_test

sqlite_api_test: valve test/src/table.tsv build/valve.db test/insert_update.sh | test/output
	@echo "Testing API functions on sqlite ..."
	./$< --api_test $(word 2,$^) $(word 3,$^)
	$(word 4,$^) $(word 3,$^)
	@echo "Test succeeded!"

pg_api_test: valve test/src/table.tsv test/insert_update.sh | test/output
	@echo "Testing API functions on postgresql ..."
	./$< $(word 2,$^) postgresql:///valve_postgres > /dev/null
	./$< --api_test $(word 2,$^) postgresql:///valve_postgres
	$(word 3,$^) postgresql:///valve_postgres
	@echo "Test succeeded!"

sqlite_perf_db = build/valve_perf.db
perf_test_dir = test/perf_test_data

perf_test: sqlite_perf_test pg_perf_test

$(perf_test_dir)/ontology:
	mkdir -p $(perf_test_dir)/ontology

perf_test_data: test/generate_perf_test_data.py | $(perf_test_dir)/ontology
	$< 1 100 0 $|

sqlite_perf_test: valve clean perf_test_data | build test/output
	$< $(perf_test_dir)/table.tsv $(sqlite_perf_db) > /dev/null
	time -p test/round_trip.sh $(sqlite_perf_db) $(perf_test_dir)/table.tsv
	time -p scripts/export.py messages $(sqlite_perf_db) $(word 2,$|) $(tables_to_test)
	diff --strip-trailing-cr -q $(perf_test_dir)/expected/messages.tsv test/output/messages.tsv

pg_perf_test: valve clean perf_test_data | build test/output
	$< $(perf_test_dir)/table.tsv postgresql:///valve_postgres > /dev/null
	time -p test/round_trip.sh postgresql:///valve_postgres $(perf_test_dir)/table.tsv
	time -p scripts/export.py messages postgresql:///valve_postgres $(word 2,$|) $(tables_to_test)
	diff --strip-trailing-cr -q $(perf_test_dir)/expected/messages.tsv test/output/messages.tsv

clean:
	rm -Rf build test/output $(perf_test_dir)/ontology

cleanall: clean
	cargo clean
