use ontodev_valve::{
    delete_row, get_compiled_datatype_conditions, get_compiled_rule_conditions,
    get_parsed_structure_conditions, insert_new_row, redo, undo, update_row,
    validate::{get_matching_values, validate_row},
    valve,
    valve_grammar::StartParser,
    ColumnRule, CompiledCondition, ParsedStructure, SerdeMap, ValveCommand,
};
use serde_json::{json, Value as SerdeValue};
use sqlx::{
    any::{AnyConnectOptions, AnyKind, AnyPool, AnyPoolOptions},
    query as sqlx_query,
};
use std::{collections::HashMap, str::FromStr};

async fn test_matching(
    config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    parsed_structure_conditions: &HashMap<String, ParsedStructure>,
    pool: &AnyPool,
) -> Result<(), sqlx::Error> {
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

    Ok(())
}

async fn test_idempotent_validate_and_update(
    config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
) -> Result<(), sqlx::Error> {
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

    // Update the row we constructed and validated above in the database:
    update_row(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "table2",
        &row.as_object().unwrap(),
        &1,
        "VALVE",
    )
    .await?;

    Ok(())
}

async fn test_validate_and_insert_1(
    config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
) -> Result<(), sqlx::Error> {
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
        "VALVE",
    )
    .await?;

    Ok(())
}

async fn test_validate_and_update(
    config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
) -> Result<(), sqlx::Error> {
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
        "VALVE",
    )
    .await?;

    Ok(())
}

async fn test_validate_and_insert_2(
    config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
) -> Result<(), sqlx::Error> {
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
        "VALVE",
    )
    .await?;

    Ok(())
}

async fn test_dependencies(
    config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
) -> Result<(), sqlx::Error> {
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
        "VALVE",
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
        "VALVE",
    )
    .await?;

    delete_row(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "table11",
        &4,
        "VALVE",
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
        "VALVE",
    )
    .await?;

    Ok(())
}

async fn test_undo_redo(
    config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
) -> Result<(), sqlx::Error> {
    // Undo/redo tests
    let row_1 = json!({
        "foreign_column": {"messages": [], "valid": true, "value": "j"},
        "other_foreign_column": {"messages": [], "valid": true, "value": "j"},
        "numeric_foreign_column": {"messages": [], "valid": true, "value": "10"},
    });
    let row_2 = json!({
        "foreign_column": {"messages": [], "valid": true, "value": "k"},
        "other_foreign_column": {"messages": [], "valid": true, "value": "k"},
        "numeric_foreign_column": {"messages": [], "valid": true, "value": "11"},
    });

    // Undo/redo test 1:
    let _rn = insert_new_row(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "table10",
        &row_1.as_object().unwrap(),
        None,
        "VALVE",
    )
    .await?;

    undo(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "VALVE",
    )
    .await?;

    redo(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "VALVE",
    )
    .await?;

    undo(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "VALVE",
    )
    .await?;

    // Undo/redo test 2:
    update_row(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "table10",
        &row_2.as_object().unwrap(),
        &8,
        "VALVE",
    )
    .await?;

    undo(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "VALVE",
    )
    .await?;

    redo(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "VALVE",
    )
    .await?;

    undo(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "VALVE",
    )
    .await?;

    // Undo/redo test 3:
    delete_row(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "table10",
        &8,
        "VALVE",
    )
    .await?;

    undo(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "VALVE",
    )
    .await?;

    redo(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "VALVE",
    )
    .await?;

    undo(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "VALVE",
    )
    .await?;

    // Undo/redo test 4:
    let rn = insert_new_row(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "table10",
        &row_1.as_object().unwrap(),
        None,
        "VALVE",
    )
    .await?;

    update_row(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "table10",
        &row_2.as_object().unwrap(),
        &rn,
        "VALVE",
    )
    .await?;

    // Undo update:
    undo(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "VALVE",
    )
    .await?;

    // Redo update:
    redo(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "VALVE",
    )
    .await?;

    delete_row(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "table10",
        &rn,
        "VALVE",
    )
    .await?;

    // Undo delete:
    undo(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "VALVE",
    )
    .await?;

    // Undo update:
    undo(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "VALVE",
    )
    .await?;

    // Undo insert:
    undo(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        "VALVE",
    )
    .await?;

    Ok(())
}

pub async fn run_api_tests(table: &str, database: &str) -> Result<(), sqlx::Error> {
    let config = valve(table, database, &ValveCommand::Config, false, "table").await?;
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

    // NOTE that you must use an external script to fetch the data from the database and run a diff
    // against a known good sample to verify that these tests yield the expected results:
    test_matching(
        &config,
        &compiled_datatype_conditions,
        &parsed_structure_conditions,
        &pool,
    )
    .await?;
    test_idempotent_validate_and_update(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
    )
    .await?;
    test_validate_and_insert_1(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
    )
    .await?;
    test_validate_and_update(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
    )
    .await?;
    test_validate_and_insert_2(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
    )
    .await?;
    test_dependencies(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
    )
    .await?;
    test_undo_redo(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
    )
    .await?;

    Ok(())
}
