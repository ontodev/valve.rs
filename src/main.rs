mod api_test;

use crate::api_test::run_api_tests;

use argparse::{ArgumentParser, Store, StoreTrue};

use ontodev_valve::{
    get_compiled_datatype_conditions, get_compiled_rule_conditions,
    get_parsed_structure_conditions, valve, valve_grammar::StartParser, ValveCommand,
};
use serde_json::{from_str, Value as SerdeValue};
use std::{env, process};

fn cli_args_valid(source: &str, destination: &str, dump_config: bool) -> bool {
    source != "" && (dump_config || destination != "")
}

#[async_std::main]
async fn main() -> Result<(), sqlx::Error> {
    let mut api_test = false;
    let mut dump_config = false;
    let mut create_only = false;
    let mut config_table = String::new();
    let mut verbose = false;
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
        ap.refer(&mut create_only).add_option(
            &["--create_only"],
            StoreTrue,
            r#"Read the configuration referred to by SOURCE, and create a corresponding database in
               DESTINATION but do not load it."#,
        );
        ap.refer(&mut config_table).add_option(
            &["--config_table"],
            Store,
            r#"When reading configuration from a database, the name to use to refer to the main
               configuration table (defaults to "table")"#,
        );
        ap.refer(&mut verbose).add_option(
            &["--verbose"],
            StoreTrue,
            r#"Write the SQL used to create the database to stdout after configuring it, and then
               while loading the database, write progress messages to stderr."#,
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

    if config_table.trim() == "" {
        config_table = "table".to_string();
    }

    if api_test {
        run_api_tests(&source, &destination).await?;
    } else if dump_config {
        let config = valve(
            &source,
            &String::from(":memory:"),
            &ValveCommand::Config,
            false,
            &config_table,
        )
        .await?;
        let mut config: SerdeValue = serde_json::from_str(config.as_str()).unwrap();
        let config = config.as_object_mut().unwrap();
        let parser = StartParser::new();

        let datatype_conditions = get_compiled_datatype_conditions(&config, &parser);
        let structure_conditions = get_parsed_structure_conditions(&config, &parser);
        let rule_conditions =
            get_compiled_rule_conditions(&config, datatype_conditions.clone(), &parser);

        let datatype_conditions = format!("{:?}", datatype_conditions).replace(r"\", r"\\");
        let datatype_conditions: SerdeValue = from_str(&datatype_conditions).unwrap();
        config.insert(String::from("datatype_conditions"), datatype_conditions);

        let structure_conditions = format!("{:?}", structure_conditions).replace(r"\", r"\\");
        let structure_conditions: SerdeValue = from_str(&structure_conditions).unwrap();
        config.insert(String::from("structure_conditions"), structure_conditions);

        let rule_conditions = format!("{:?}", rule_conditions).replace(r"\", r"\\");
        let rule_conditions: SerdeValue = from_str(&rule_conditions).unwrap();
        config.insert(String::from("rule_conditions"), rule_conditions);

        let config = serde_json::to_string(config).unwrap();
        println!("{}", config);
    } else if create_only {
        valve(
            &source,
            &destination,
            &ValveCommand::Create,
            verbose,
            &config_table,
        )
        .await?;
    } else {
        valve(
            &source,
            &destination,
            &ValveCommand::Load,
            verbose,
            &config_table,
        )
        .await?;
    }

    Ok(())
}
