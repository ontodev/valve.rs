MAKEFLAGS += --warn-undefined-variables
.DEFAULT_GOAL := valve
.DELETE_ON_ERROR:
.SUFFIXES:

# NOTE:
# -----
# The test targets assume that we have a postgresql server, accessible by the current user via the
# UNIX socket /var/run/postgresql, in which a database called `valve_postgres` has been created.
# They also requires that `psycopg2` has been installed.

.PHONY: build
build:
	mkdir -p build

.PHONY: doc
doc:
	cargo doc --document-private-items

.PHONY: readme
readme:
	cargo readme --no-title > README.md

valve: src/*.rs src/*.lalrpop
	@$(MAKE) valve_debug

.PHONY: valve_release
valve_release:
	rm -f valve
	cargo build --release
	ln -s target/release/ontodev_valve valve

.PHONY: valve_debug
valve_debug:
	rm -f valve
	cargo build
	ln -s target/debug/ontodev_valve valve

build/valve.db: valve | build test/output
	cp -f test/src/ontology/view1_sqlite.sql test/output/view1.sql
	cp -f test/src/ontology/view2_sqlite.sql test/output/view2.sql
	cp -f test/src/ontology/view2_sqlite.sh test/output/view2.sh
	cp -f test/src/ontology/readonly1_sqlite_load.sh test/output/readonly1.sh
	cp -f test/src/ontology/readonly3_sqlite_load.sql test/output/readonly3.sql
	chmod u+x test/output/readonly1.sh
	sqlite3 $@ < test/src/ontology/view3_sqlite.sql

test/output:
	mkdir -p test/output

.PHONY: test
test: clean_test_db sqlite_test pg_test api_test random_test

tables_to_test := $(shell cut -f 1 test/src/table.tsv)
pg_connect_string := postgresql:///valve_postgres

.PHONY: sqlite_test
sqlite_test: build/valve.db test/src/table.tsv | test/output
	@echo "Testing valve on sqlite ..."
	./valve test/src/table.tsv build/valve.db
	test/round_trip.sh $^
	scripts/export_messages.py $< $| $(tables_to_test)
	diff --strip-trailing-cr -q test/expected/messages.tsv test/output/messages.tsv
	scripts/export_messages.py --a1 $< $| $(tables_to_test)
	diff --strip-trailing-cr -q test/expected/messages_a1.tsv test/output/messages.tsv
	# The "pk" test is run on table7 only since it is the only table whose primary keys are all valid:
	scripts/export_messages.py --pk $< $| table7
	diff --strip-trailing-cr -q test/expected/messages_pk.tsv test/output/messages.tsv
	@echo "Test succeeded!"

.PHONY: pg_test
pg_test: valve test/src/table.tsv | test/output
	@echo "Testing valve on postgresql ..."
	cp -f test/src/ontology/view1_postgresql.sql test/output/view1.sql
	cp -f test/src/ontology/view2_postgresql.sql test/output/view2.sql
	cp -f test/src/ontology/view2_postgresql.sh test/output/view2.sh
	cp -f test/src/ontology/readonly1_postgresql_load.sh test/output/readonly1.sh
	cp -f test/src/ontology/readonly3_postgresql_load.sql test/output/readonly3.sql
	chmod u+x test/output/readonly1.sh
	psql $(pg_connect_string) < test/src/ontology/view3_postgresql.sql
	./$^ $(pg_connect_string)
	test/round_trip.sh $(pg_connect_string) $(word 2,$^)
	scripts/export_messages.py $(pg_connect_string) $| $(tables_to_test)
	diff --strip-trailing-cr -q test/expected/messages.tsv test/output/messages.tsv
	scripts/export_messages.py --a1 $(pg_connect_string) $| $(tables_to_test)
	diff --strip-trailing-cr -q test/expected/messages_a1.tsv test/output/messages.tsv
	# The "pk" test is run on table7 only since it is the only table whose primary keys are all valid:
	scripts/export_messages.py --pk $(pg_connect_string) $| table7
	diff --strip-trailing-cr -q test/expected/messages_pk.tsv test/output/messages.tsv
	@echo "Test succeeded!"

.PHONY: api_test
api_test: sqlite_api_test pg_api_test

.PHONY: sqlite_api_test
sqlite_api_test: valve test/src/table.tsv build/valve.db test/insert_update.sh | test/output
	@echo "Testing API functions on sqlite ..."
	./$< --api_test $(word 2,$^) $(word 3,$^)
	$(word 4,$^) $(word 3,$^) $(word 2,$^)
	scripts/export_messages.py $(word 3,$^) $| $(tables_to_test)
	diff --strip-trailing-cr -q test/expected/messages_after_api_test.tsv test/output/messages.tsv
	echo "select \"history_id\", \"table\", \"row\", \"from\", \"to\", \"summary\", \"user\", \"undone_by\" from history where history_id < 15 order by history_id" | sqlite3 -header -tabs build/valve.db > test/output/history.tsv
	diff --strip-trailing-cr -q test/expected/history.tsv test/output/history.tsv
	# We drop all of the db tables because the schema for the next test (random test) is different
	# from the schema used for this test.
	./$< --drop_all $(word 2,$^) $(word 3,$^)
	@echo "Test succeeded!"

.PHONY: pg_api_test
pg_api_test: valve test/src/table.tsv test/insert_update.sh | test/output
	@echo "Testing API functions on postgresql ..."
	./$< $(word 2,$^) $(pg_connect_string)
	./$< --api_test $(word 2,$^) $(pg_connect_string)
	$(word 3,$^) $(pg_connect_string) $(word 2,$^)
	scripts/export_messages.py $(pg_connect_string) $| $(tables_to_test)
	diff --strip-trailing-cr -q test/expected/messages_after_api_test.tsv test/output/messages.tsv
	# TODO: Add summary here (for move test)
	psql $(pg_connect_string) -c "COPY (select \"history_id\", \"table\", \"row\", \"from\", \"to\", \"summary\", \"user\", \"undone_by\" from history where history_id < 15 order by history_id) TO STDOUT WITH NULL AS ''" > test/output/history.tsv
	tail -n +2 test/expected/history.tsv | diff --strip-trailing-cr -q test/output/history.tsv -
	# We drop all of the db tables because the schema for the next test (random test) is different
	# from the schema used for this test.
	./$< --drop_all $(word 2,$^) $(pg_connect_string)
	@echo "Test succeeded!"

sqlite_random_db = build/valve_random.db
random_test_dir = test/random_test_data

.PHONY: random_test
random_test: sqlite_random_test pg_random_test

$(random_test_dir)/ontology:
	mkdir -p $@

.PHONY: random_test_data
random_test_data: test/generate_random_test_data.py valve valve test/random_test_data/table.tsv | $(random_test_dir)/ontology
	./$< $$(date +"%s") 100 5 $(word 3,$^) $|

.PHONY: sqlite_random_test
sqlite_random_test: valve random_test_data | build test/output
	@echo "Testing with random data on sqlite ..."
	./$< $(random_test_dir)/table.tsv $(sqlite_random_db)
	test/round_trip.sh $(sqlite_random_db) $(random_test_dir)/table.tsv
	@echo "Test succeeded!"

.PHONY: pg_random_test
pg_random_test: valve random_test_data | build test/output
	@echo "Testing with random data on postgresql ..."
	./$< $(random_test_dir)/table.tsv $(pg_connect_string)
	test/round_trip.sh $(pg_connect_string) $(random_test_dir)/table.tsv
	@echo "Test succeeded!"

test/penguins/src/data:
	mkdir -p $@

penguin_test_threshold = 45
num_penguin_rows = 100000
penguin_command = ./valve --initial_load src/schema/table.tsv penguins.db

.PHONY: penguin_test
penguin_test: valve | test/penguins/src/data
	@echo "Running penguin test ..."
	rm -f test/penguins/penguins.db
	cd test/penguins && ./generate.py $(num_penguin_rows)
	cd test/penguins && ln -f -s ../../target/debug/ontodev_valve valve
	@echo "cd test/penguins && $(penguin_command)"
	@cd test/penguins && \
		timeout $(penguin_test_threshold) \
		time -p \
		$(penguin_command) || \
		(echo "Penguin test took longer than $(penguin_test_threshold) seconds." && false)
	@echo "Test succeeded!"

guess_test_dir = test/guess_test_data
guess_test_db = build/valve_guess.db

$(guess_test_dir)/table1.tsv: test/generate_random_test_data.py valve $(guess_test_dir)/*.tsv
	./$< 0 30000 5 $(guess_test_dir)/table.tsv $(guess_test_dir)

$(guess_test_dir)/ontology:
	mkdir -p $@

.PHONY: guess_test_data
guess_test_data: test/generate_random_test_data.py $(guess_test_dir)/table1.tsv valve confirm_overwrite.sh $(guess_test_dir)/*.tsv | $(guess_test_dir)/ontology
	./confirm_overwrite.sh $(guess_test_dir)/ontology
	rm -f $(guess_test_dir)/table1.tsv
	./$< 0 30000 5 $(guess_test_dir)/table.tsv $(guess_test_dir)
	rm -f $(guess_test_dir)/ontology/*.tsv
	./$< 0 30000 5 $(guess_test_dir)/table_expected.tsv $|
	rm -f $(guess_test_dir)/ontology/table1.tsv

$(guess_test_db): valve guess_test_data $(guess_test_dir)/*.tsv | build $(guess_test_dir)/ontology
	rm -f $@
	./$< $(guess_test_dir)/table.tsv $@

perf_test_dir = test/perf_test_data
perf_test_db = build/valve_perf.db

$(perf_test_dir)/ontology:
	mkdir -p $@

.PHONY: perf_test_data
perf_test_data: test/generate_random_test_data.py valve confirm_overwrite.sh $(perf_test_dir)/*.tsv | $(perf_test_dir)/ontology
	./confirm_overwrite.sh $(perf_test_dir)/ontology
	rm -f $(perf_test_dir)/ontology/*.tsv
	./$< $$(date +"%s") 1000 5 $(perf_test_dir)/table.tsv $|

$(perf_test_db): valve perf_test_data $(perf_test_dir)/*.tsv | build $(perf_test_dir)/ontology
	rm -f $@
	time -p ./$< --verbose --interactive --initial_load $(perf_test_dir)/table.tsv $@

.PHONY: sqlite_perf_test
sqlite_perf_test: $(perf_test_db) | test/output
	time -p scripts/export_messages.py $< $| $(tables_to_test)

.PHONY: pg_perf_test
pg_perf_test: valve $(perf_test_dir)/ontology | test/output
	time -p ./$< --verbose --interactive $(perf_test_dir)/table.tsv $(pg_connect_string)
	time -p scripts/export_messages.py $(pg_connect_string) $| $(tables_to_test)

.PHONY: perf_test
perf_test: sqlite_perf_test pg_perf_test

.PHONY: clean
clean:
	rm -Rf build/valve.db* build/valve_random.db* test/output $(random_test_dir)/ontology valve

.PHONY: clean_test_db
clean_test_db:
	rm -Rf build/valve.db

.PHONY: clean_guess_db
clean_guess_db:
	rm -Rf build/valve_guess.db

.PHONY: clean_guess_data
clean_guess_data:
	rm -Rf $(guess_test_dir)/table1.tsv $(guess_test_dir)/ontology

.PHONY: clean_perf_db
clean_perf_db:
	rm -Rf build/valve_perf.db

.PHONY: clean_perf_data
clean_perf_data:
	rm -Rf $(perf_test_dir)/ontology

.PHONY: cleanall
cleanall: clean clean_perf_db clean_perf_data clean_guess_db clean_guess_data clean_pg
	cargo clean
	rm -f valve
