use sqlx::{
    any::{AnyConnectOptions, AnyPoolOptions},
    query as sqlx_query,
};
use std::{env, process, str::FromStr};
use valve::{
    configure_db, load_db, read_config_files, run_tests, valve_grammar::StartParser, ConfigMap,
};

use serde_json::Value as SerdeValue;

#[async_std::main]
async fn main() -> Result<(), sqlx::Error> {
    let args: Vec<String> = env::args().collect();
    let test;
    let table;
    let db_dir;
    if args.len() == 3 {
        test = false;
        table = &args[1];
        db_dir = &args[2];
    } else if args.len() == 4 && &args[1] == "--test" {
        test = true;
        table = &args[2];
        db_dir = &args[3];
    } else {
        eprintln!("Usage: valve [--test] table db_dir");
        process::exit(1);
    }
    let parser = StartParser::new();

    let (
        specials_config,
        mut tables_config,
        mut datatypes_config,
        rules_config,
        compiled_datatype_conditions,
        compiled_rule_conditions,
        parsed_structure_conditions,
    ) = read_config_files(table, &parser);

    let connection_options =
        AnyConnectOptions::from_str(format!("sqlite://{}/valve.db?mode=rwc", db_dir).as_str())?;
    let pool = AnyPoolOptions::new().max_connections(5).connect_with(connection_options).await?;
    sqlx_query("PRAGMA foreign_keys = ON").execute(&pool).await?;

    // To connect to a postgresql database listening to a unix domain socket:
    // ----------------------------------------------------------------------
    // let connection_options =
    //     AnyConnectOptions::from_str("postgres:///testdb?host=/var/run/postgresql")?;
    //
    // To query the connection type at runtime via the pool:
    // -----------------------------------------------------
    // let db_type = pool.any_kind();

    let constraints_config = configure_db(
        &mut tables_config,
        &mut datatypes_config,
        &pool,
        &parser,
        Some(true),
        Some(true),
    )
    .await?;

    let mut config = ConfigMap::new();
    config.insert(String::from("special"), SerdeValue::Object(specials_config.clone()));
    config.insert(String::from("table"), SerdeValue::Object(tables_config.clone()));
    config.insert(String::from("datatype"), SerdeValue::Object(datatypes_config.clone()));
    config.insert(String::from("rule"), SerdeValue::Object(rules_config.clone()));
    config.insert(String::from("constraints"), SerdeValue::Object(constraints_config.clone()));

    load_db(&config, &pool, &compiled_datatype_conditions, &compiled_rule_conditions).await?;

    if test {
        run_tests(
            &config,
            &compiled_datatype_conditions,
            &compiled_rule_conditions,
            &parsed_structure_conditions,
            &pool,
        )
        .await?;
    }

    Ok(())
}
