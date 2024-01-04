mod api_test;

use crate::api_test::run_api_tests;

use argparse::{ArgumentParser, Store, StoreTrue};

use ontodev_valve::Valve;
use serde_json::{from_str, Value as SerdeValue};
use std::{env, process};

fn cli_args_valid(source: &str, destination: &str, dump_config: bool) -> bool {
    source != "" && (dump_config || destination != "")
}

#[async_std::main]
async fn main() -> Result<(), sqlx::Error> {
    let mut verbose = false;
    let mut yes = false;
    let mut api_test = false;
    let mut dump_config = false;
    let mut dump_schema = false;
    let mut table_order = false;
    let mut show_deps_in = false;
    let mut show_deps_out = false;
    let mut drop_all = false;
    let mut create_only = false;
    let mut initial_load = false;
    let mut source = String::new();
    let mut destination = String::new();

    {
        // this block limits scope of borrows by ap.refer() method
        let mut ap = ArgumentParser::new();
        ap.set_description(
            r#"A lightweight validation engine written in rust. If neither
               --api_test nor --dump_config is specified, the configuration referred
               to by SOURCE will be read and a new database will be created and loaded
               with the indicated data."#,
        );
        ap.refer(&mut verbose).add_option(
            &["--verbose"],
            StoreTrue,
            r#"While loading the database, write progress messages to stderr."#,
        );
        ap.refer(&mut yes).add_option(
            &["--yes"],
            StoreTrue,
            r#"Do not prompt the user to confirm dropping/truncating tables."#,
        );
        ap.refer(&mut api_test).add_option(
            &["--api_test"],
            StoreTrue,
            r#"Read the configuration referred to by SOURCE and test the functions that
               are callable externally on the existing, pre-loaded database indicated by
               DESTINATION."#,
        );
        ap.refer(&mut dump_config).add_option(
            &["--dump_config"],
            StoreTrue,
            r#"Read the configuration referred to by SOURCE and send it to stdout as a
               JSON-formatted string."#,
        );
        ap.refer(&mut dump_schema).add_option(
            &["--dump_schema"],
            StoreTrue,
            r#"Write the SQL used to create the database to stdout."#,
        );
        ap.refer(&mut table_order).add_option(
            &["--table_order"],
            StoreTrue,
            r#"Display the order in which tables must be created or dropped."#,
        );
        ap.refer(&mut show_deps_in).add_option(
            &["--show_deps_in"],
            StoreTrue,
            r#"Display the incoming dependencies for each configured table."#,
        );
        ap.refer(&mut show_deps_out).add_option(
            &["--show_deps_out"],
            StoreTrue,
            r#"Display the outgoing dependencies for each configured table."#,
        );
        ap.refer(&mut drop_all).add_option(
            &["--drop_all"],
            StoreTrue,
            r#"Drop all tables in the database."#,
        );
        ap.refer(&mut create_only).add_option(
            &["--create_only"],
            StoreTrue,
            r#"Read the configuration referred to by SOURCE, and create a corresponding database in
               DESTINATION but do not load it."#,
        );
        ap.refer(&mut initial_load).add_option(
            &["--initial_load"],
            StoreTrue,
            r#"(SQLite only) When this flag is set, the database settings will be tuned for initial
               loading. Note that these settings are unsafe and should be used for initial loading
               only, as data integrity will not be guaranteed in the case of an interrupted
               transaction."#,
        );
        ap.refer(&mut source).add_argument(
            "SOURCE",
            Store,
            r#"(Required.) The location of the valve configuration entrypoint. Can be
               one of (A) A URL of the form `postgresql://...` or `sqlite://...` indicating a
               database connection where the valve configuration can be read from a table named
               "table"; (B) The filename (including path) of the table file (usually called
               table.tsv)."#,
        );
        ap.refer(&mut destination).add_argument(
            "DESTINATION",
            Store,
            r#"(Required unless the --dump_config option has been specified.) Can be
               one of (A) A URL of the form `postgresql://...` or `sqlite://...`
               (B) The filename (including path) of a sqlite database."#,
        );

        ap.parse_args_or_exit()
    }

    let args: Vec<String> = env::args().collect();
    let program_name = &args[0];
    if !cli_args_valid(&source, &destination, dump_config) {
        if source == "" {
            eprintln!("Parameter SOURCE is required.");
        } else if destination == "" {
            eprintln!("Parameter DESTINATION is required.");
        }
        eprintln!("To see command-line usage, run {} --help", program_name);
        process::exit(1);
    }

    let interactive = !yes;
    if api_test {
        run_api_tests(&source, &destination).await?;
    } else if dump_config {
        let valve = Valve::build(&source, &destination, verbose, initial_load, interactive).await?;
        let mut config = valve.config.clone();
        let datatype_conditions =
            format!("{:?}", valve.compiled_datatype_conditions).replace(r"\", r"\\");
        let datatype_conditions: SerdeValue = from_str(&datatype_conditions).unwrap();
        config.insert(String::from("datatype_conditions"), datatype_conditions);

        let structure_conditions =
            format!("{:?}", valve.parsed_structure_conditions).replace(r"\", r"\\");
        let structure_conditions: SerdeValue = from_str(&structure_conditions).unwrap();
        config.insert(String::from("structure_conditions"), structure_conditions);

        let rule_conditions = format!("{:?}", valve.compiled_rule_conditions).replace(r"\", r"\\");
        let rule_conditions: SerdeValue = from_str(&rule_conditions).unwrap();
        config.insert(String::from("rule_conditions"), rule_conditions);

        let config = serde_json::to_string(&config).unwrap();
        println!("{}", config);
    } else if dump_schema {
        let valve = Valve::build(&source, &destination, verbose, initial_load, interactive).await?;
        valve.dump_schema().await?;
    } else if table_order {
        let valve = Valve::build(&source, &destination, verbose, initial_load, interactive).await?;
        let sorted_table_list = valve.get_sorted_table_list(false);
        println!("{}", sorted_table_list.join(", "));
    } else if show_deps_in || show_deps_out {
        let valve = Valve::build(&source, &destination, verbose, initial_load, interactive).await?;
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
        let valve = Valve::build(&source, &destination, verbose, initial_load, interactive).await?;
        valve.drop_all_tables().await?;
    } else if create_only {
        let valve = Valve::build(&source, &destination, verbose, initial_load, interactive).await?;
        valve.create_all_tables().await?;
    } else {
        let valve = Valve::build(&source, &destination, verbose, initial_load, interactive).await?;
        valve.load_all_tables(true).await?;
    }

    Ok(())
}
