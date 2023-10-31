use ontodev_valve::{
    delete_row, get_compiled_datatype_conditions, get_compiled_rule_conditions,
    get_parsed_structure_conditions, insert_new_row, update_row,
    validate::{get_matching_values, validate_row},
    valve,
    valve_grammar::StartParser,
    ValveCommand,
};
use serde_json::{json, Value as SerdeValue};
use sqlx::{
    any::{AnyConnectOptions, AnyKind, AnyPoolOptions},
    query as sqlx_query,
};
use std::str::FromStr;

pub async fn run_api_tests(table: &str, database: &str) -> Result<(), sqlx::Error> {
    let config = valve(
        table,
        database,
        &ValveCommand::Config,
        false,
        false,
        "table",
    )
    .await?;
    let config: SerdeValue = serde_json::from_str(config.as_str()).unwrap();
    let config = config.as_object().unwrap();

    // To connect to a postgresql database listening to a unix domain socket:
    // ----------------------------------------------------------------------
    // let connection_options =
    //     AnyConnectOptions::from_str("postgres:///testdb?host=/var/run/postgresql")?;
    //
    // To query the connection type at runtime via the pool:
    // -----------------------------------------------------
    // let db_type = pool.any_kind();

    let connection_options;
    if database.starts_with("postgresql://") {
        connection_options = AnyConnectOptions::from_str(database)?;
    } else {
        let connection_string;
        if !database.starts_with("sqlite://") {
            connection_string = format!("sqlite://{}?mode=rwc", database);
        } else {
            connection_string = database.to_string();
        }
        connection_options = AnyConnectOptions::from_str(connection_string.as_str()).unwrap();
    }

    let pool = AnyPoolOptions::new()
        .max_connections(5)
        .connect_with(connection_options)
        .await?;
    if pool.any_kind() == AnyKind::Sqlite {
        sqlx_query("PRAGMA foreign_keys = ON")
            .execute(&pool)
            .await?;
    }

    let parser = StartParser::new();
    let compiled_datatype_conditions = get_compiled_datatype_conditions(&config, &parser);
    let parsed_structure_conditions = get_parsed_structure_conditions(&config, &parser);
    let compiled_rule_conditions =
        get_compiled_rule_conditions(&config, compiled_datatype_conditions.clone(), &parser);

    // Test the get_matching_values() function:
    let matching_values = get_matching_values(
        &config,
        &compiled_datatype_conditions,
        &parsed_structure_conditions,
        &pool,
        "table2",
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
            {"id":"h","label":"h","order":8},
            {"id":"k","label":"k","order":9}

        ])
    );

    let matching_values = get_matching_values(
        &config,
        &compiled_datatype_conditions,
        &parsed_structure_conditions,
        &pool,
        "table6",
        "child",
        Some("7"),
    )
    .await?;
    assert_eq!(
        matching_values,
        json!([
            {"id":"7","label":"7","order":1}
        ])
    );

    // We test that validate_row() is idempotent by running it multiple times on the same row:
    let row = json!({
        "child": {"messages": [], "valid": true, "value": "b"},
        "parent": {"messages": [], "valid": true, "value": "f"},
        "xyzzy": {"messages": [], "valid": true, "value": "w"},
        "foo": {"messages": [], "valid": true, "value": 1},
        "bar": {
            "messages": [
                {"level": "error", "message": "An unrelated error", "rule": "custom:unrelated"}
            ],
            "valid": false,
            "value": "B",
        },
    });

    let result_row_1 = validate_row(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        None,
        "table2",
        row.as_object().unwrap(),
        Some(1),
        None,
    )
    .await?;

    let result_row_2 = validate_row(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        None,
        "table2",
        &result_row_1,
        Some(1),
        None,
    )
    .await?;
    assert_eq!(result_row_1, result_row_2);

    let result_row = validate_row(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        None,
        "table2",
        &result_row_2,
        Some(1),
        None,
    )
    .await?;
    assert_eq!(result_row, result_row_2);

    // Test update, delete, and insert. NOTE that there are no calls to assert() below. You must use
    // an external script to fetch the data from the database and run a diff against a known good
    // sample.

    // Update the row we constructed and validated above in the database:
    update_row(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "table2",
        &row.as_object().unwrap(),
        &1,
        false,
        false,
    )
    .await?;

    // Validate and insert a new row:
    let row = json!({
        "id": {"messages": [], "valid": true, "value": "BFO:0000027"},
        "label": {"messages": [], "valid": true, "value": "bazaar"},
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
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        None,
        "table3",
        row.as_object().unwrap(),
        None,
        None,
    )
    .await?;

    let _new_row_num = insert_new_row(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "table3",
        &result_row,
        None,
        false,
    )
    .await?;

    // Validate and update an existing row:
    let row = json!({
        "child": {"messages": [], "valid": true, "value": 2},
        "parent": {"messages": [], "valid": true, "value": 6},
        "xyzzy": {"messages": [], "valid": true, "value": 23},
        "foo": {"messages": [], "valid": true, "value": 'a'},
        "bar": {
            "messages": [
                {"level": "error", "message": "An unrelated error", "rule": "custom:unrelated"}
            ],
            "valid": false,
            "value": 2,
        },
    });

    let result_row = validate_row(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        None,
        "table6",
        row.as_object().unwrap(),
        Some(1),
        None,
    )
    .await?;

    update_row(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "table6",
        &result_row,
        &1,
        false,
        false,
    )
    .await?;

    // Validate and insert a new row:
    let row = json!({
        "child": {"messages": [], "valid": true, "value": 2},
        "parent": {"messages": [], "valid": true, "value": 6},
        "xyzzy": {"messages": [], "valid": true, "value": 23},
        "foo": {"messages": [], "valid": true, "value": 'a'},
        "bar": {
            "messages": [
                {"level": "error", "message": "An unrelated error", "rule": "custom:unrelated"}
            ],
            "valid": false,
            "value": 2,
        },
    });

    let result_row = validate_row(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        None,
        "table6",
        row.as_object().unwrap(),
        None,
        None,
    )
    .await?;

    let _new_row_num = insert_new_row(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "table6",
        &result_row,
        None,
        false,
    )
    .await?;

    // Test cases for updates/inserts/deletes with dependencies.
    let row = json!({
        "foreign_column": {"messages": [], "valid": true, "value": "w"},
        "other_foreign_column": {"messages": [], "valid": true, "value": "z"},
        "numeric_foreign_column": {"messages": [], "valid": true, "value": ""},
    });

    update_row(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "table10",
        &row.as_object().unwrap(),
        &1,
        false,
        false,
    )
    .await?;

    let row = json!({
        "child": {"messages": [], "valid": true, "value": "b"},
        "parent": {"messages": [], "valid": true, "value": "c"},
        "xyzzy": {"messages": [], "valid": true, "value": "d"},
        "foo": {"messages": [], "valid": true, "value": "d"},
        "bar": {"messages": [], "valid": true, "value": "f"},
    });

    update_row(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "table11",
        &row.as_object().unwrap(),
        &2,
        false,
        false,
    )
    .await?;

    delete_row(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "table11",
        &4,
        false,
    )
    .await?;

    let row = json!({
        "foreign_column": {"messages": [], "valid": true, "value": "i"},
        "other_foreign_column": {"messages": [], "valid": true, "value": "i"},
        "numeric_foreign_column": {"messages": [], "valid": true, "value": "9"},
    });

    let _new_row_num = insert_new_row(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "table10",
        &row.as_object().unwrap(),
        None,
        false,
    )
    .await?;

    Ok(())
}
