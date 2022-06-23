use serde_json::{json, Value as SerdeValue};
use sqlx::{
    any::{AnyConnectOptions, AnyPool, AnyPoolOptions},
    query as sqlx_query,
};
use std::{collections::HashMap, env, process, str::FromStr};
use valve::{
    configure_db, get_compiled_datatype_conditions, get_compiled_rule_conditions,
    get_parsed_structure_conditions, insert_new_row, load_db, read_config_files, update_row,
    validate::{get_matching_values, validate_row},
    valve_grammar::StartParser,
    ColumnRule, CompiledCondition, ConfigMap, ParsedStructure,
};

async fn run_tests(
    config: &ConfigMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    parsed_structure_conditions: &HashMap<String, ParsedStructure>,
    pool: &AnyPool,
) -> Result<(), sqlx::Error> {
    let matching_values = get_matching_values(
        config,
        compiled_datatype_conditions,
        parsed_structure_conditions,
        pool,
        "foobar",
        "child",
        None,
    )
    .await?;
    assert_eq!(
        matching_values,
        json!([
            {"id":"a","label":"a","order":1},
            {"id":"b","label":"b","order":2},
            {"id":"c","label":"c","order":3},
            {"id":"d","label":"d","order":4},
            {"id":"e","label":"e","order":5},
            {"id":"f","label":"f","order":6},
            {"id":"g","label":"g","order":7},
            {"id":"h","label":"h","order":8}
        ])
    );

    // NOTE: No validation of the validate/insert/update functions is done below. You must use an
    // external script to fetch the data from the database and run a diff against a known good
    // sample.
    let row = json!({
        "child": {"messages": [], "valid": true, "value": "b"},
        "parent": {"messages": [], "valid": true, "value": "f"},
        "xyzzy": {"messages": [], "valid": true, "value": "w"},
        "foo": {"messages": [], "valid": true, "value": "A"},
        "bar": {
            "messages": [
                {"level": "error", "message": "An unrelated error", "rule": "custom:unrelated"}
            ],
            "valid": false,
            "value": "B",
        },
    });

    let result_row = validate_row(
        config,
        compiled_datatype_conditions,
        compiled_rule_conditions,
        pool,
        "foobar",
        row.as_object().unwrap(),
        true,
        Some(1),
    )
    .await?;
    update_row(pool, "foobar", &result_row, 1).await?;

    let row = json!({
        "id": {"messages": [], "valid": true, "value": "BFO:0000027"},
        "label": {"messages": [], "valid": true, "value": "car"},
        "parent": {
            "messages": [
                {"level": "error", "message": "An unrelated error", "rule": "custom:unrelated"}
            ],
            "valid": false,
            "value": "barrie",
        },
        "source": {"messages": [], "valid": true, "value": "BFOBBER"},
        "type": {"messages": [], "valid": true, "value": "owl:Class"},
    });

    let result_row = validate_row(
        config,
        compiled_datatype_conditions,
        compiled_rule_conditions,
        pool,
        "import",
        row.as_object().unwrap(),
        false,
        None,
    )
    .await?;
    let _new_row_num = insert_new_row(pool, "import", &result_row).await?;

    Ok(())
}

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

    if test {
        let parsed_structure_conditions = get_parsed_structure_conditions(&config, &parser);
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
