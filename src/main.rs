mod api_test;

use crate::api_test::run_api_tests;

use ontodev_valve::{
    configure_and_or_load, get_compiled_datatype_conditions, get_compiled_rule_conditions,
    get_parsed_structure_conditions, valve_grammar::StartParser,
};
use serde_json::{from_str, Value as SerdeValue};
use std::{env, process};

#[async_std::main]
async fn main() -> Result<(), sqlx::Error> {
    let usage = r#"
Usage: valve [OPTION] TABLE [DATABASE]
Where:
  OPTION may be one of:
    --api_test: Read the configuration referred to by TABLE and test the
                functions that are callable externally on the existing,
                pre-loaded database indicated by DATABASE.
    --dump_config: Read the configuration referred to by TABLE and send it to
                   stdout as a JSON-formatted string.
      If no option is specified, the configuration referred to by the 'table table'
      will be read and a new database will be created and loaded with the indicated
      data.
  TABLE is a filename referring to a specific valve configuration.
  DATABASE (required unless the --dump_config option has been specified) can be one of:
    - A URL of the form `postgresql://...` or `sqlite://...`
    - The filename (including path) of a sqlite database."#;

    let args: Vec<String> = env::args().collect();
    if args.len() < 3 || (args[1] == "--api_test" && args.len() < 4) {
        eprintln!("{}", usage);
        process::exit(1);
    }

    let table;
    let database;
    if &args[1] == "--help" {
        eprintln!("{}", usage);
    } else if &args[1] == "--api_test" {
        table = &args[2];
        database = &args[3];
        run_api_tests(table, database).await?;
    } else if &args[1] == "--dump_config" {
        table = &args[2];
        let config = configure_and_or_load(table, &String::from(":memory:"), false).await?;
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
    } else {
        table = &args[1];
        database = &args[2];
        configure_and_or_load(table, database, true).await?;
    }

    Ok(())
}
