use itertools::Chunk;
use serde_json::{
    json,
    // SerdeMap by default backed by BTreeMap (see https://docs.serde.rs/serde_json/map/index.html)
    Map as SerdeMap,
    Value as SerdeValue,
};
use sqlx::sqlite::SqlitePool;
use std::collections::HashMap;

use crate::ast::Expression;
use crate::cmi_pb_grammar::StartParser;

type RowMap = SerdeMap<String, SerdeValue>;

pub fn validate_rows_intra(
    config: &RowMap,
    pool: &SqlitePool,
    parser: &StartParser,
    parsed_conditions: &HashMap<String, Expression>,
    compiled_conditions: &HashMap<String, Box<dyn Fn(&str) -> bool>>,
    table_name: &String,
    headers: &csv::StringRecord,
    rows: &mut Chunk<csv::StringRecordsIter<std::fs::File>>,
    chunk_number: usize,
    results: &mut HashMap<usize, SerdeValue>,
    multiprocessing: bool,
) -> Vec<SerdeValue> {
    let mut result_rows: Vec<SerdeValue> = vec![];
    for row in rows {
        let row: RowMap = row.unwrap().deserialize(Some(headers)).unwrap();
        let mut result_row: RowMap = SerdeMap::new();
        for (column, value) in row {
            let result_cell = json!({
                "value": value,
                "valid": true,
                "messages": [],
            });
            result_row.insert(column, result_cell);
        }

        for (column_name, cell) in result_row.clone() {
            let cell = validate_cell_nulltype(
                config,
                pool,
                parser,
                parsed_conditions,
                compiled_conditions,
                table_name,
                &column_name,
                cell.as_object().unwrap(),
            );
            result_row.insert(column_name.to_string(), SerdeValue::Object(cell));
        }

        for (column_name, cell) in result_row.clone() {
            let mut cell = validate_cell_rules(
                config,
                pool,
                parser,
                parsed_conditions,
                compiled_conditions,
                table_name,
                &column_name,
                &result_row,
                cell.as_object().unwrap(),
            );
            if !cell.contains_key("nulltype") {
                cell = validate_cell_datatype(
                    config,
                    pool,
                    parser,
                    parsed_conditions,
                    compiled_conditions,
                    table_name,
                    &column_name,
                    &cell,
                );
            }
            result_row.insert(column_name.to_string(), SerdeValue::Object(cell));
        }
        result_rows.push(SerdeValue::Object(result_row));
    }

    if multiprocessing {
        results.insert(chunk_number, SerdeValue::Array(result_rows.clone()));
    }
    result_rows
}

fn validate_cell_nulltype(
    config: &RowMap,
    // Temporarily prefix these variables with an underscore to avoid compiler warnings about unused
    // variables (remove this later).
    _pool: &SqlitePool,
    _parser: &StartParser,
    _parsed_conditions: &HashMap<String, Expression>,
    compiled_conditions: &HashMap<String, Box<dyn Fn(&str) -> bool>>,
    table_name: &String,
    column_name: &String,
    cell: &RowMap,
) -> RowMap {
    let mut cell = cell.clone();

    let column = config
        .get("table")
        .and_then(|t| t.as_object())
        .and_then(|o| o.get(table_name))
        .and_then(|t| t.as_object())
        .and_then(|o| o.get("column"))
        .and_then(|c| c.as_object())
        .and_then(|o| o.get(column_name))
        .unwrap();
    if let Some(SerdeValue::String(nt_name)) = column.get("nulltype") {
        let nt_condition = compiled_conditions.get(nt_name).unwrap();
        let value = cell.get("value").and_then(|v| v.as_str()).unwrap();
        if nt_condition(value) {
            cell.insert("nulltype".to_string(), SerdeValue::String(nt_name.to_string()));
        }
    }

    cell
}

fn validate_cell_datatype(
    config: &RowMap,
    // Temporarily prefix these variables with an underscore to avoid compiler warnings about unused
    // variables (remove this later).
    _pool: &SqlitePool,
    _parser: &StartParser,
    _parsed_conditions: &HashMap<String, Expression>,
    compiled_conditions: &HashMap<String, Box<dyn Fn(&str) -> bool>>,
    table_name: &String,
    column_name: &String,
    cell: &RowMap,
) -> RowMap {
    fn get_datatypes_to_check(
        config: &RowMap,
        compiled_conditions: &HashMap<String, Box<dyn Fn(&str) -> bool>>,
        primary_dt_name: &str,
        dt_name: Option<String>,
    ) -> Vec<RowMap> {
        let mut datatypes = vec![];
        if let Some(dt_name) = dt_name {
            let datatype = config
                .get("datatype")
                .and_then(|d| d.as_object())
                .and_then(|o| o.get(&dt_name))
                .and_then(|d| d.as_object())
                .unwrap();
            let dt_name = datatype.get("datatype").and_then(|d| d.as_str()).unwrap();
            let dt_condition = compiled_conditions.get(dt_name);
            let dt_parent = match datatype.get("parent") {
                Some(SerdeValue::String(s)) => Some(s.clone()),
                _ => None,
            };
            if dt_name != primary_dt_name {
                if let Some(_) = dt_condition {
                    datatypes.push(datatype.clone());
                }
            }
            let mut more_datatypes =
                get_datatypes_to_check(config, compiled_conditions, primary_dt_name, dt_parent);
            datatypes.append(&mut more_datatypes);
        }
        datatypes
    }

    let cell_value = cell.get("value").and_then(|v| v.as_str()).unwrap();
    let mut cell = cell.clone();
    let column = config
        .get("table")
        .and_then(|t| t.as_object())
        .and_then(|o| o.get(table_name))
        .and_then(|t| t.as_object())
        .and_then(|o| o.get("column"))
        .and_then(|c| c.as_object())
        .and_then(|o| o.get(column_name))
        .unwrap();
    let primary_dt_name = column.get("datatype").and_then(|d| d.as_str()).unwrap();
    let primary_datatype = config
        .get("datatype")
        .and_then(|d| d.as_object())
        .and_then(|o| o.get(primary_dt_name))
        .unwrap();
    let primary_dt_description = primary_datatype.get("description").unwrap();
    if let Some(primary_dt_condition_func) = compiled_conditions.get(primary_dt_name) {
        if !primary_dt_condition_func(cell_value) {
            cell.insert("valid".to_string(), SerdeValue::Bool(false));
            let mut parent_datatypes = get_datatypes_to_check(
                config,
                compiled_conditions,
                primary_dt_name,
                Some(primary_dt_name.to_string()),
            );
            while !parent_datatypes.is_empty() {
                let datatype = parent_datatypes.pop().unwrap();
                let dt_name = datatype.get("datatype").and_then(|d| d.as_str()).unwrap();
                let dt_description = datatype.get("description").and_then(|d| d.as_str()).unwrap();
                let dt_condition = compiled_conditions.get(dt_name).unwrap();
                if !dt_condition(cell_value) {
                    let messages = cell.get_mut("messages").and_then(|m| m.as_array_mut()).unwrap();
                    let message = json!({
                        "rule": format!("datatype:{}", dt_name),
                        "level": "error",
                        "message": format!("{} should be {}", column_name, dt_description)
                    });
                    messages.push(message);
                }
            }
            if primary_dt_description != "" {
                let messages = cell.get_mut("messages").and_then(|m| m.as_array_mut()).unwrap();
                let message = json!({
                    "rule": format!("datatype:{}", primary_dt_name),
                    "level": "error",
                    "message": format!("{} should be {}", column_name, primary_dt_description)
                });
                messages.push(message);
            }
        }
    }

    cell
}

fn validate_cell_rules(
    // Temporarily prefix these variables with an underscore to avoid compiler warnings about unused
    // variables (remove this later).
    _config: &RowMap,
    _pool: &SqlitePool,
    _parser: &StartParser,
    _parsed_conditions: &HashMap<String, Expression>,
    _compiled_conditions: &HashMap<String, Box<dyn Fn(&str) -> bool>>,
    _table_name: &String,
    _column_name: &String,
    _result_row: &RowMap,
    cell: &RowMap,
) -> RowMap {
    cell.clone()
}
