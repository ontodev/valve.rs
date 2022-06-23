use serde_json::Value as SerdeValue;
use sqlx::{
    any::{AnyConnectOptions, AnyPoolOptions},
    query as sqlx_query,
};
use std::{env, process, str::FromStr};
use valve::{
    configure_db, get_compiled_datatype_conditions, get_compiled_rule_conditions, load_db,
    read_config_files, valve_grammar::StartParser, ConfigMap,
};

#[async_std::main]
async fn main() -> Result<(), sqlx::Error> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: valve table db_dir");
        process::exit(1);
    }

    let table = &args[1];
    let db_dir = &args[2];

    let parser = StartParser::new();

    let (specials_config, mut tables_config, mut datatypes_config, rules_config) =
        read_config_files(table);

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

    let compiled_datatype_conditions = get_compiled_datatype_conditions(&config, &parser);
    let compiled_rule_conditions =
        get_compiled_rule_conditions(&config, compiled_datatype_conditions.clone(), &parser);

    load_db(&config, &pool, &compiled_datatype_conditions, &compiled_rule_conditions).await?;

    Ok(())
}
