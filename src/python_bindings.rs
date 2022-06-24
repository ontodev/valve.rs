use crate::{
    configure_and_or_load, get_compiled_datatype_conditions, get_compiled_rule_conditions,
    get_matching_values, get_parsed_structure_conditions, insert_new_row, update_row, validate_row,
    valve_grammar::StartParser,
};
use futures::executor::block_on;
use pyo3::prelude::{pyfunction, pymodule, wrap_pyfunction, PyModule, PyResult, Python};
use serde_json::Value as SerdeValue;
use sqlx::{
    any::{AnyConnectOptions, AnyPoolOptions},
    query as sqlx_query,
};
use std::str::FromStr;

/// Given a path to a table table file (table.tsv), a directory in which to find/create a database:
/// configure the database using the configuration which can be looked up using the table table,
/// and optionally load it if the `load` flag is set to true.
#[pyfunction]
fn py_configure_and_or_load(table_table: &str, db_dir: &str, load: bool) -> PyResult<String> {
    let config = block_on(configure_and_or_load(table_table, db_dir, load)).unwrap();
    Ok(config)
}

/// Given a config map represented as a JSON string, a directory containing the database, the table
/// name and column name from which to retrieve matching values, return a JSON array (represented as
/// a string) of possible valid values for the given column which contain the matching string as a
/// substring (or all of them if no matching string is given). The JSON array returned is formatted
/// for Typeahead, i.e., it takes the form: [{"id": id, "label": label, "order": order}, ...].
#[pyfunction]
fn py_get_matching_values(
    config: &str,
    db_dir: &str,
    table_name: &str,
    column_name: &str,
    matching_string: Option<&str>,
) -> PyResult<String> {
    let config: SerdeValue = serde_json::from_str(config).unwrap();
    let config = config.as_object().unwrap();

    // Note that we use mode=ro here instead of mode=rwc
    let connection_options =
        AnyConnectOptions::from_str(format!("sqlite://{}/valve.db?mode=ro", db_dir).as_str())
            .unwrap();
    let pool = AnyPoolOptions::new().max_connections(5).connect_with(connection_options);
    let pool = block_on(pool).unwrap();

    let parser = StartParser::new();
    let compiled_datatype_conditions = get_compiled_datatype_conditions(&config, &parser);
    let parsed_structure_conditions = get_parsed_structure_conditions(&config, &parser);

    let matching_values = block_on(get_matching_values(
        &config,
        &compiled_datatype_conditions,
        &parsed_structure_conditions,
        &pool,
        table_name,
        column_name,
        matching_string,
    ))
    .unwrap();

    Ok(matching_values.to_string())
}

/// Given a config map represented as a JSON string, a directory in which to find the database,
/// a table name, a row, and if the row already exists in the database, its associated row number,
/// perform both intra- and inter-row validation and return the validated row as a JSON string.
#[pyfunction]
fn py_validate_row(
    config: &str,
    db_dir: &str,
    table_name: &str,
    row: &str,
    existing_row: bool,
    row_number: Option<u32>,
) -> PyResult<String> {
    let config: SerdeValue = serde_json::from_str(config).unwrap();
    let config = config.as_object().unwrap();
    let row: SerdeValue = serde_json::from_str(row).unwrap();
    let row = row.as_object().unwrap();

    // Note that we use mode=ro here instead of mode=rwc
    let connection_options =
        AnyConnectOptions::from_str(format!("sqlite://{}/valve.db?mode=ro", db_dir).as_str())
            .unwrap();
    let pool = AnyPoolOptions::new().max_connections(5).connect_with(connection_options);
    let pool = block_on(pool).unwrap();

    let parser = StartParser::new();
    let compiled_datatype_conditions = get_compiled_datatype_conditions(&config, &parser);
    let compiled_rule_conditions =
        get_compiled_rule_conditions(&config, compiled_datatype_conditions.clone(), &parser);

    let result_row = block_on(validate_row(
        &config,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &pool,
        table_name,
        &row,
        existing_row,
        row_number,
    ))
    .unwrap();

    Ok(SerdeValue::Object(result_row).to_string())
}

/// Given a directory in which the database is located, a table name, a row represented as a
/// JSON string, and its associated row number, update the row in the database.
#[pyfunction]
fn py_update_row(db_dir: &str, table_name: &str, row: &str, row_number: u32) -> PyResult<()> {
    let row: SerdeValue = serde_json::from_str(row).unwrap();
    let row = row.as_object().unwrap();

    // Note that we use mode=rw here instead of mode=rwc
    let connection_options =
        AnyConnectOptions::from_str(format!("sqlite://{}/valve.db?mode=rw", db_dir).as_str())
            .unwrap();
    let pool = AnyPoolOptions::new().max_connections(5).connect_with(connection_options);
    let pool = block_on(pool).unwrap();
    block_on(sqlx_query("PRAGMA foreign_keys = ON").execute(&pool)).unwrap();

    block_on(update_row(&pool, table_name, &row, row_number)).unwrap();

    Ok(())
}

/// Given a directory in which the database is located, a table name, and a row represented as a
/// JSON string, insert the new row to the database.
#[pyfunction]
fn py_insert_new_row(db_dir: &str, table_name: &str, row: &str) -> PyResult<u32> {
    let row: SerdeValue = serde_json::from_str(row).unwrap();
    let row = row.as_object().unwrap();

    // Note that we use mode=rw here instead of mode=rwc
    let connection_options =
        AnyConnectOptions::from_str(format!("sqlite://{}/valve.db?mode=rw", db_dir).as_str())
            .unwrap();
    let pool = AnyPoolOptions::new().max_connections(5).connect_with(connection_options);
    let pool = block_on(pool).unwrap();
    block_on(sqlx_query("PRAGMA foreign_keys = ON").execute(&pool)).unwrap();

    let new_row_number = block_on(insert_new_row(&pool, table_name, &row)).unwrap();
    Ok(new_row_number)
}

#[pymodule]
fn valve(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_configure_and_or_load, m)?)?;
    m.add_function(wrap_pyfunction!(py_get_matching_values, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_row, m)?)?;
    m.add_function(wrap_pyfunction!(py_update_row, m)?)?;
    m.add_function(wrap_pyfunction!(py_insert_new_row, m)?)?;
    Ok(())
}
