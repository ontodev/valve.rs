use ontodev_valve::{
    delete_row, get_compiled_datatype_conditions, get_compiled_rule_conditions,
    get_parsed_structure_conditions, insert_new_row, redo, undo, update_row,
    validate::{get_matching_values, validate_row},
    valve,
    valve_grammar::StartParser,
    ColumnRule, CompiledCondition, ParsedStructure, SerdeMap, ValveCommand,
};
use rand::distributions::{Alphanumeric, DistString, Distribution, Uniform};
use rand::{random, thread_rng};
use serde_json::{json, Value as SerdeValue};
use sqlx::{
    any::{AnyConnectOptions, AnyKind, AnyPool, AnyPoolOptions},
    query as sqlx_query,
    Error::Configuration as SqlxCErr,
    Row, ValueRef,
};
use std::{collections::HashMap, str::FromStr};

async fn test_matching(
    config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    parsed_structure_conditions: &HashMap<String, ParsedStructure>,
    pool: &AnyPool,
) -> Result<(), sqlx::Error> {
    eprint!("Running test_matching() ... ");
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

    eprintln!("done.");
    Ok(())
}

async fn test_idempotent_validate_and_update(
    config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
) -> Result<(), sqlx::Error> {
    eprint!("Running test_idempotent_validate_and_update() ... ");
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

    eprintln!("done.");
    Ok(())
}

async fn test_validate_and_insert_1(
    config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
) -> Result<(), sqlx::Error> {
    eprint!("Running test_validate_and_insert_1() ... ");
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

    eprintln!("done.");
    Ok(())
}

async fn test_validate_and_update(
    config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
) -> Result<(), sqlx::Error> {
    eprint!("Running test_validate_and_update() ... ");
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

    eprintln!("done.");
    Ok(())
}

async fn test_validate_and_insert_2(
    config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
) -> Result<(), sqlx::Error> {
    eprint!("Running test_validate_and_insert_2() ... ");
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

    eprintln!("done.");
    Ok(())
}

async fn test_dependencies(
    config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
) -> Result<(), sqlx::Error> {
    eprint!("Running test_dependencies() ... ");
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

    eprintln!("done.");
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DbOperation {
    Insert,
    Delete,
    Update,
    Undo,
    Redo,
}

async fn generate_operation_sequence(pool: &AnyPool) -> Result<Vec<DbOperation>, sqlx::Error> {
    /*
    Algorithm:
    ----------
    1. Determine the number of "modify" operations to randomly generate. Then for each operation do
       the following:
    2. Generate a modify/undo pair
    3. Do the modify
    4. Either add an undo immediately after the given modify, or defer the undo by adding it to an
       undo stack.
    5. Possibly generate a redo/undo pair such that the undo comes immediately after the undo, or
       is deferred to the undo stack.
    6. Once all of the modify operations have been processed, go through the undo stack:
       a. For each undo, once it's been processed, possibly generate a redo/undo pair such that the
          undo comes immediately after the undo, or is deferred to the undo stack.

    After this function returns, the database should be in the same logical state as it was before.
     */

    let list_len = {
        let between = Uniform::from(25..51);
        let mut rng = thread_rng();
        between.sample(&mut rng)
    };

    let mut operations = vec![];
    let mut undo_stack = vec![];
    for _ in 0..list_len {
        let between = Uniform::from(0..3);
        let mut rng = thread_rng();
        match between.sample(&mut rng) {
            0 => operations.push(DbOperation::Insert),
            1 => {
                let query = sqlx_query("SELECT MAX(row_number) AS row_number FROM table1_view");
                let rows = query.fetch_all(pool).await?;
                if rows.len() != 0 {
                    operations.push(DbOperation::Delete)
                } else {
                    operations.push(DbOperation::Insert)
                }
            }
            2 => {
                let query = sqlx_query("SELECT MAX(row_number) AS row_number FROM table1_view");
                let rows = query.fetch_all(pool).await?;
                if rows.len() != 0 {
                    operations.push(DbOperation::Update)
                } else {
                    operations.push(DbOperation::Insert)
                }
            }
            _ => unreachable!(),
        };
        // Randomly either add an undo immediately after the modify, or add it to the undo_stack:
        if random::<bool>() == true {
            operations.push(DbOperation::Undo);
            // Randomly add a redo as well:
            if random::<bool>() == true {
                operations.push(DbOperation::Redo);
                // Randomly either add an undo either immediately after the redo, or to the
                // undo_stack:
                if random::<bool>() == true {
                    operations.push(DbOperation::Undo);
                } else {
                    undo_stack.push(DbOperation::Undo);
                }
            }
        } else {
            undo_stack.push(DbOperation::Undo);
        }
    }

    // Go through the items in the undo stack:
    let mut further_operations = vec![];
    let mut further_undo_stack = vec![];
    while let Some(_) = undo_stack.pop() {
        // Add the undo to the list of further operations to perform:
        further_operations.push(DbOperation::Undo);
        // Randomly add a redo as well:
        if random::<bool>() == true {
            further_operations.push(DbOperation::Redo);
            // Randomly add an undo either immediately after the redo, or to a further
            // stack of undos to be performed at the end:
            if random::<bool>() == true {
                further_operations.push(DbOperation::Undo);
            } else {
                further_undo_stack.push(DbOperation::Undo);
            }
        }
    }

    operations.append(&mut further_operations);
    // Since further_undo_stack is a stack, we need to reverse it:
    further_undo_stack.reverse();
    operations.append(&mut further_undo_stack);
    Ok(operations)
}

async fn test_randomized_api_test_with_undo_redo(
    config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
) -> Result<(), sqlx::Error> {
    // Randomly generate a number of insert/update/delete operations, possibly followed by undos
    // and/or redos.
    eprint!("Running test_randomized_api_test_with_undo_redo() ... ");
    fn generate_value() -> String {
        let mut value = Alphanumeric.sample_string(&mut rand::thread_rng(), 10);
        while random::<bool>() && random::<bool>() {
            value.push_str(" ");
            value.push_str(&Alphanumeric.sample_string(&mut rand::thread_rng(), 10));
        }
        if random::<bool>() && random::<bool>() {
            value.push_str(" ");
        }
        value
    }

    fn generate_row() -> SerdeMap {
        let mut row = SerdeMap::new();
        row.insert(
            "prefix".to_string(),
            json!({"messages": [], "valid": true, "value": generate_value()}),
        );
        row.insert(
            "base".to_string(),
            json!({"messages": [], "valid": true, "value": generate_value()}),
        );
        row.insert(
            "ontology IRI".to_string(),
            json!({"messages": [], "valid": true, "value": generate_value()}),
        );
        row.insert(
            "version IRI".to_string(),
            json!({"messages": [], "valid": true, "value": generate_value()}),
        );
        row
    }

    let operations_list = generate_operation_sequence(pool).await?;
    for operation in operations_list {
        match operation {
            DbOperation::Delete => {
                let query = sqlx_query("SELECT MAX(row_number) AS row_number FROM table1_view");
                let sql_row = query.fetch_one(pool).await?;
                let raw_row_number = sql_row.try_get_raw("row_number")?;
                if raw_row_number.is_null() {
                    return Err(SqlxCErr("No rows in table1_view".into()));
                } else {
                    let row_number: i64 = sql_row.get("row_number");
                    let row_number = row_number as u32;
                    delete_row(
                        &config,
                        &compiled_datatype_conditions,
                        &compiled_rule_conditions,
                        &pool,
                        "table1",
                        &row_number,
                        "VALVE",
                    )
                    .await?;
                }
            }
            DbOperation::Update => {
                let query = sqlx_query("SELECT MAX(row_number) AS row_number FROM table1_view");
                let sql_row = query.fetch_one(pool).await?;
                let raw_row_number = sql_row.try_get_raw("row_number")?;
                if raw_row_number.is_null() {
                    return Err(SqlxCErr("No rows in table1_view".into()));
                } else {
                    let row_number: i64 = sql_row.get("row_number");
                    let row_number = row_number as u32;
                    let row = generate_row();
                    update_row(
                        &config,
                        &compiled_datatype_conditions,
                        &compiled_rule_conditions,
                        &pool,
                        "table1",
                        &row,
                        &row_number,
                        "VALVE",
                    )
                    .await?;
                }
            }
            DbOperation::Insert => {
                let row = generate_row();
                let _rn = insert_new_row(
                    &config,
                    &compiled_datatype_conditions,
                    &compiled_rule_conditions,
                    &pool,
                    "table1",
                    &row,
                    None,
                    "VALVE",
                )
                .await?;
            }
            DbOperation::Undo => {
                undo(
                    &config,
                    &compiled_datatype_conditions,
                    &compiled_rule_conditions,
                    &pool,
                    "VALVE",
                )
                .await?;
            }
            DbOperation::Redo => {
                redo(
                    &config,
                    &compiled_datatype_conditions,
                    &compiled_rule_conditions,
                    &pool,
                    "VALVE",
                )
                .await?;
            }
        };
    }

    eprintln!("done.");
    Ok(())
}

async fn test_undo_redo(
    config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
) -> Result<(), sqlx::Error> {
    eprint!("Running test_undo_redo() ... ");
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

    eprintln!("done.");
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

    test_randomized_api_test_with_undo_redo(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
    )
    .await?;

    Ok(())
}
