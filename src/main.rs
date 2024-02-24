mod api_test;

use crate::api_test::run_api_tests;
use argparse::{ArgumentParser, Store, StoreTrue};
use futures::executor::block_on;
use ontodev_valve::{valve::Valve, valve::ValveError};
use std::{env, process};

#[async_std::main]
async fn main() -> Result<(), ValveError> {
    // Command line parameters and their default values. See below for descriptions. Note that some
    // of these are mutually exclusive. This is accounted for below.

    // TODO: Use a more powerful command-line parser library that can automatically take care of
    // things like mutually exclusive options, since argparse doesn't seem to be able to do it.

    let mut api_test = false;
    let mut create_only = false;
    let mut destination = String::new();
    let mut drop_all = false;
    let mut dump_config = false;
    let mut dump_schema = false;
    let mut initial_load = false;
    let mut interactive = false;
    let mut save_all = false;
    let mut save_dir = String::new();
    let mut save = String::new();
    let mut show_deps_in = false;
    let mut show_deps_out = false;
    let mut source = String::new();
    let mut table_order = false;
    let mut verbose = false;
    // TODO: Add a "dry_run" parameter.

    {
        // this block limits scope of borrows by ap.refer() method
        let mut ap = ArgumentParser::new();
        ap.set_description(r#"Valve is a lightweight validation engine written in rust."#);
        ap.refer(&mut interactive).add_option(
            &["--interactive"],
            StoreTrue,
            r#"Ask for confirmation before performing destructive operations on the database."#,
        );
        ap.refer(&mut verbose).add_option(
            &["--verbose"],
            StoreTrue,
            r#"Write informative messages about what Valve is doing to stderr."#,
        );
        ap.refer(&mut initial_load).add_option(
            &["--initial_load"],
            StoreTrue,
            r#"(SQLite only) When this flag is set, the database settings will be tuned for initial
               loading. Note that these settings are unsafe and should be used for initial loading
               only, as data integrity will not be guaranteed in the case of an interrupted
               transaction."#,
        );
        ap.refer(&mut dump_config).add_option(
            &["--dump_config"],
            StoreTrue,
            r#"Read the configuration referred to by SOURCE and print it as a JSON-formatted
               string."#,
        );
        ap.refer(&mut dump_schema).add_option(
            &["--dump_schema"],
            StoreTrue,
            r#"Read the configuration referred to by SOURCE and print the SQL that will be used to
               create the database to stdout."#,
        );
        ap.refer(&mut table_order).add_option(
            &["--table_order"],
            StoreTrue,
            r#"Read the configuration referred to by SOURCE and print the order in which the
               configured tables will be created, as determined by their dependency relations."#,
        );
        ap.refer(&mut show_deps_in).add_option(
            &["--show_deps_in"],
            StoreTrue,
            r#"Read the configuration referred to by SOURCE and print the incoming dependencies
               for each configured table."#,
        );
        ap.refer(&mut show_deps_out).add_option(
            &["--show_deps_out"],
            StoreTrue,
            r#"Read the configuration referred to by SOURCE andprint the outgoing dependencies
               for each configured table."#,
        );
        ap.refer(&mut drop_all).add_option(
            &["--drop_all"],
            StoreTrue,
            r#"Read the configuration referred to by SOURCE and drop all of the configured tables
               in the given database."#,
        );
        ap.refer(&mut create_only).add_option(
            &["--create_only"],
            StoreTrue,
            r#"Read the configuration referred to by SOURCE, and create a corresponding database in
               DESTINATION but do not load it."#,
        );
        ap.refer(&mut save).add_option(
            &["--save"],
            Store,
            r#"Read the configuration referred to by SOURCE and save the configured data tables
               from the given list as TSV files to their configured paths (as specified in the table
               configuration). Optionally, specify --save-path to save the files at an alternative
               location."#,
        );
        ap.refer(&mut save_all).add_option(
            &["--save_all"],
            StoreTrue,
            r#"Read the configuration referred to by SOURCE and save the all configured data tables
               as TSV files to their configured paths (as specified in the table configuration).
               Optionally, specify --save-path to save the files at an alternative location."#,
        );
        ap.refer(&mut save_dir).add_option(
            &["--save_dir"],
            Store,
            r#"Ignored if neither --save nor --save-all has been specified. Saves the tables to the
               given path instead of to their configured paths."#,
        );
        ap.refer(&mut api_test).add_option(
            &["--api_test"],
            StoreTrue,
            r#"Read the configuration referred to by SOURCE and run a set of predefined tests on the
               existing, pre-loaded database indicated by DESTINATION."#,
        );
        ap.refer(&mut source).add_argument(
            "SOURCE",
            Store,
            r#"The location of the valve configuration entrypoint. Can be one of (A) A URL of the
               form `postgresql://...` or `sqlite://...` indicating a database connection where
               the valve configuration can be read from a table named "table"; (B) The filename
               (including path) of the table file (usually called table.tsv)."#,
        );
        ap.refer(&mut destination).add_argument(
            "DESTINATION",
            Store,
            r#"Can be one of (A) A URL of the form `postgresql://...` or `sqlite://...`
               (B) The filename (including path) of a sqlite database."#,
        );

        ap.parse_args_or_exit()
    }

    let args: Vec<String> = env::args().collect();
    let program_name = &args[0];
    let advice = format!("Run `{} --help` for command line usage.", program_name);

    let mutually_exclusive_options = vec![
        api_test,
        dump_config,
        dump_schema,
        table_order,
        show_deps_in,
        show_deps_out,
        drop_all,
        create_only,
        save != "" || save_all,
    ];

    if mutually_exclusive_options
        .iter()
        .filter(|&i| *i == true)
        .count()
        > 1
    {
        eprintln!(
            "More than one mutually exclusive option specified. {}.",
            advice
        );
        process::exit(1);
    }

    let destination_optional =
        dump_config || dump_schema || table_order || show_deps_in || show_deps_out;

    if source == "" {
        eprintln!("Parameter SOURCE is required. {}", advice);
        process::exit(1);
    } else if !destination_optional && destination == "" {
        eprintln!("Parameter DESTINATION is required. {}", advice);
        process::exit(1);
    }

    let build_valve = || -> Result<Valve, ValveError> {
        let mut valve = block_on(Valve::build(&source, &destination))?;
        valve.set_verbose(verbose);
        valve.set_interactive(interactive);
        if initial_load {
            block_on(valve.set_initial_load())?;
        }
        Ok(valve)
    };

    if api_test {
        run_api_tests(&source, &destination).await?;
    } else if save_all || save != "" {
        let valve = build_valve()?;
        let save_dir = {
            if save_dir == "" {
                None
            } else {
                Some(save_dir.clone())
            }
        };
        if save_all {
            valve.save_all_tables(&save_dir).unwrap();
        } else {
            let tables = save.split(',').collect::<Vec<_>>();
            valve.save_tables(&tables, &save_dir).unwrap();
        }
    } else if dump_config {
        let valve = build_valve()?;
        println!("{}", valve.config);
    } else if dump_schema {
        let valve = build_valve()?;
        let schema = valve.dump_schema().await?;
        println!("{}", schema);
    } else if table_order {
        let valve = build_valve()?;
        let sorted_table_list = valve.get_sorted_table_list(false);
        println!("{}", sorted_table_list.join(", "));
    } else if show_deps_in || show_deps_out {
        let valve = build_valve()?;
        let dependencies = valve.collect_dependencies(show_deps_in);
        for (table, deps) in dependencies.iter() {
            let deps = {
                let deps = deps.iter().map(|s| format!("'{}'", s)).collect::<Vec<_>>();
                if deps.is_empty() {
                    "None".to_string()
                } else {
                    deps.join(", ")
                }
            };
            let preamble = {
                if show_deps_in {
                    format!("Tables that depend on '{}'", table)
                } else {
                    format!("Table '{}' depends on", table)
                }
            };
            println!("{}: {}", preamble, deps);
        }
    } else if drop_all {
        let valve = build_valve()?;
        valve.drop_all_tables().await?;
    } else if create_only {
        let valve = build_valve()?;
        valve.create_all_tables().await?;
    } else {
        let valve = build_valve()?;
        valve.load_all_tables(true).await?;
    }

    Ok(())
}
