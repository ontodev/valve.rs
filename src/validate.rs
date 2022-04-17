use serde_json::{
    json,
    //to_string,
    // SerdeMap by default backed by BTreeMap (see https://docs.serde.rs/serde_json/map/index.html)
    //Map as SerdeMap,
    Value as SerdeValue,
};
use sqlx::sqlite::SqlitePool;
use std::collections::HashMap;

use crate::ast::Expression;
use crate::cmi_pb_grammar::StartParser;
use crate::InputRowMap;

#[derive(Clone)]
pub struct ResultCell {
    nulltype: Option<String>,
    value: String,
    valid: bool,
    messages: Vec<SerdeValue>,
}

pub type ResultRow = HashMap<String, ResultCell>;

pub fn validate_rows_intra(
    config: &InputRowMap,
    pool: &SqlitePool,
    parser: &StartParser,
    parsed_conditions: &HashMap<String, Expression>,
    compiled_conditions: &HashMap<String, Box<dyn Fn(&str) -> bool + Sync + Send>>,
    table_name: &String,
    headers: &csv::StringRecord,
    rows: &mut Vec<Result<csv::StringRecord, csv::Error>>,
) -> Vec<ResultRow> {
    let mut result_rows: Vec<ResultRow> = vec![];
    for row in rows {
        if let Ok(row) = row {
            let mut result_row: ResultRow = ResultRow::new();
            for (i, value) in row.iter().enumerate() {
                let result_cell = ResultCell {
                    nulltype: None,
                    value: String::from(value),
                    valid: true,
                    messages: vec![],
                };
                let column = headers.get(i).unwrap();
                result_row.insert(column.to_string(), result_cell);
            }

            // We check all the cells for nulltype first, since the rules validation requires that we
            // have this information for all cells.
            let column_names: Vec<String> = result_row.keys().cloned().collect();
            for column_name in &column_names {
                let cell: &mut ResultCell = result_row.get_mut(column_name).unwrap();
                validate_cell_nulltype(
                    config,
                    pool,
                    parser,
                    parsed_conditions,
                    compiled_conditions,
                    table_name,
                    &column_name,
                    cell,
                );
            }

            for column_name in &column_names {
                let context = result_row.clone();
                let cell: &mut ResultCell = result_row.get_mut(column_name).unwrap();
                validate_cell_rules(
                    config,
                    pool,
                    parser,
                    parsed_conditions,
                    compiled_conditions,
                    table_name,
                    &column_name,
                    &context,
                    cell,
                );

                if cell.nulltype == None {
                    validate_cell_datatype(
                        config,
                        pool,
                        parser,
                        parsed_conditions,
                        compiled_conditions,
                        table_name,
                        &column_name,
                        cell,
                    );
                }
            }
            result_rows.push(result_row);
        }
    }

    /*
    I am using this as a quick ad hoc unit test but eventually we should re-implemt the unit tests
    in jamesaoverton/cmi-pb-terminology.git (on the `next` branch).
    for row in &result_rows {
        for (column_name, cell) in row {
            println!("{}: {}: messages: {:?}", table_name, column_name, cell.messages);
            if let Some(nulltype) = &cell.nulltype {
                println!("{}: {}: nulltype: {}", table_name, column_name, nulltype);
            }
            println!("{}: {}: valid: {}", table_name, column_name, cell.valid);
            println!("{}: {}: value: {}", table_name, column_name, cell.value);
        }
        //println!("{}: {}", table_name, to_string(row).unwrap());
    }
    */
    result_rows
}

fn validate_cell_nulltype(
    config: &InputRowMap,
    // Temporarily prefix these variables with an underscore to avoid compiler warnings about unused
    // variables (remove this later).
    _pool: &SqlitePool,
    _parser: &StartParser,
    _parsed_conditions: &HashMap<String, Expression>,
    compiled_conditions: &HashMap<String, Box<dyn Fn(&str) -> bool + Sync + Send>>,
    table_name: &String,
    column_name: &String,
    cell: &mut ResultCell,
) {
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
        let value = &cell.value;
        if nt_condition(&value) {
            cell.nulltype = Some(nt_name.to_string());
        }
    }
}

fn validate_cell_datatype(
    config: &InputRowMap,
    // Temporarily prefix these variables with an underscore to avoid compiler warnings about unused
    // variables (remove this later).
    _pool: &SqlitePool,
    _parser: &StartParser,
    _parsed_conditions: &HashMap<String, Expression>,
    compiled_conditions: &HashMap<String, Box<dyn Fn(&str) -> bool + Sync + Send>>,
    table_name: &String,
    column_name: &String,
    cell: &mut ResultCell,
) {
    fn get_datatypes_to_check(
        config: &InputRowMap,
        compiled_conditions: &HashMap<String, Box<dyn Fn(&str) -> bool + Sync + Send>>,
        primary_dt_name: &str,
        dt_name: Option<String>,
    ) -> Vec<InputRowMap> {
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
        if !primary_dt_condition_func(&cell.value) {
            cell.valid = false;
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
                if !dt_condition(&cell.value) {
                    let message = json!({
                        "rule": format!("datatype:{}", dt_name),
                        "level": "error",
                        "message": format!("{} should be {}", column_name, dt_description)
                    });
                    cell.messages.push(message);
                }
            }
            if primary_dt_description != "" {
                let primary_dt_description = primary_dt_description.as_str().unwrap();
                let message = json!({
                    "rule": format!("datatype:{}", primary_dt_name),
                    "level": "error",
                    "message": format!("{} should be {}", column_name, primary_dt_description)
                });
                cell.messages.push(message);
            }
        }
    }
}

fn validate_cell_rules(
    // Temporarily prefix these variables with an underscore to avoid compiler warnings about unused
    // variables (remove this later).
    _config: &InputRowMap,
    _pool: &SqlitePool,
    _parser: &StartParser,
    _parsed_conditions: &HashMap<String, Expression>,
    _compiled_conditions: &HashMap<String, Box<dyn Fn(&str) -> bool + Sync + Send>>,
    _table_name: &String,
    _column_name: &String,
    _context: &ResultRow,
    _cell: &mut ResultCell,
) {
    // To be implemented
}
