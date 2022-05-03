use serde_json::{
    json,
    //to_string,
    // SerdeMap by default backed by BTreeMap (see https://docs.serde.rs/serde_json/map/index.html)
    //Map as SerdeMap,
    Value as SerdeValue,
};
use sqlx::sqlite::SqlitePool;
use std::collections::HashMap;

use crate::cmi_pb_grammar::StartParser;
use crate::{ColumnRule, CompiledCondition, ConfigMap, ParsedStructure};

#[derive(Clone, Debug)]
pub struct ResultCell {
    nulltype: Option<String>,
    value: String,
    valid: bool,
    messages: Vec<SerdeValue>,
}

pub type ResultRow = HashMap<String, ResultCell>;

pub fn validate_rows_intra(
    config: &ConfigMap,
    pool: &SqlitePool,
    parser: &StartParser,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    parsed_structure_conditions: &HashMap<String, ParsedStructure>,
    table_name: &String,
    headers: &csv::StringRecord,
    rows: &mut Vec<Result<csv::StringRecord, csv::Error>>,
) -> Vec<ResultRow> {
    let mut result_rows: Vec<ResultRow> = vec![];
    for row in rows {
        if let Ok(row) = row {
            let mut result_row: ResultRow = ResultRow::new();
            let mut column_names: Vec<String> = vec![];
            for (i, value) in row.iter().enumerate() {
                let result_cell = ResultCell {
                    nulltype: None,
                    value: String::from(value),
                    valid: true,
                    messages: vec![],
                };
                let column = headers.get(i).unwrap();
                result_row.insert(column.to_string(), result_cell);
                column_names.push(column.to_string());
            }

            // We check all the cells for nulltype first, since the rules validation requires that we
            // have this information for all cells.
            for column_name in &column_names {
                let cell: &mut ResultCell = result_row.get_mut(column_name).unwrap();
                validate_cell_nulltype(
                    config,
                    compiled_datatype_conditions,
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
                    compiled_rule_conditions,
                    table_name,
                    &column_name,
                    &context,
                    cell,
                );

                if cell.nulltype == None {
                    validate_cell_datatype(
                        config,
                        compiled_datatype_conditions,
                        table_name,
                        &column_name,
                        cell,
                    );
                }
            }
            result_rows.push(result_row);
        }
    }

    // TODO: Remove this ad hoc test.
    //I am using this as a quick ad hoc unit test but eventually we should re-implemt the unit tests
    //in jamesaoverton/cmi-pb-terminology.git (on the `next` branch).
    for row in &result_rows {
        for (column_name, cell) in row {
            println!(
                "{}: {}: nulltype: {}, value: {}, valid: {}, messages: {:?}",
                table_name,
                column_name,
                match &cell.nulltype {
                    Some(nulltype) => nulltype.as_str(),
                    _ => "None",
                },
                cell.value,
                cell.valid,
                cell.messages
            );
        }
        //println!("{}: {}", table_name, to_string(row).unwrap());
    }

    // Finally return the result rows:
    result_rows
}

fn validate_cell_nulltype(
    config: &ConfigMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
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
        let nt_condition = &compiled_datatype_conditions.get(nt_name).unwrap().compiled;
        let value = &cell.value;
        if nt_condition(&value) {
            cell.nulltype = Some(nt_name.to_string());
        }
    }
}

fn validate_cell_datatype(
    config: &ConfigMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    table_name: &String,
    column_name: &String,
    cell: &mut ResultCell,
) {
    fn get_datatypes_to_check(
        config: &ConfigMap,
        compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
        primary_dt_name: &str,
        dt_name: Option<String>,
    ) -> Vec<ConfigMap> {
        let mut datatypes = vec![];
        if let Some(dt_name) = dt_name {
            let datatype = config
                .get("datatype")
                .and_then(|d| d.as_object())
                .and_then(|o| o.get(&dt_name))
                .and_then(|d| d.as_object())
                .unwrap();
            let dt_name = datatype.get("datatype").and_then(|d| d.as_str()).unwrap();
            let dt_condition = compiled_datatype_conditions.get(dt_name);
            let dt_parent = match datatype.get("parent") {
                Some(SerdeValue::String(s)) => Some(s.clone()),
                _ => None,
            };
            if dt_name != primary_dt_name {
                if let Some(_) = dt_condition {
                    datatypes.push(datatype.clone());
                }
            }
            let mut more_datatypes = get_datatypes_to_check(
                config,
                compiled_datatype_conditions,
                primary_dt_name,
                dt_parent,
            );
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
    if let Some(primary_dt_condition_func) = compiled_datatype_conditions.get(primary_dt_name) {
        let primary_dt_condition_func = &primary_dt_condition_func.compiled;
        if !primary_dt_condition_func(&cell.value) {
            cell.valid = false;
            let mut parent_datatypes = get_datatypes_to_check(
                config,
                compiled_datatype_conditions,
                primary_dt_name,
                Some(primary_dt_name.to_string()),
            );
            while !parent_datatypes.is_empty() {
                let datatype = parent_datatypes.pop().unwrap();
                let dt_name = datatype.get("datatype").and_then(|d| d.as_str()).unwrap();
                let dt_description = datatype.get("description").and_then(|d| d.as_str()).unwrap();
                let dt_condition = &compiled_datatype_conditions.get(dt_name).unwrap().compiled;
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
    config: &ConfigMap,
    compiled_rules: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    table_name: &String,
    column_name: &String,
    context: &ResultRow,
    cell: &mut ResultCell,
) {
    fn check_condition(
        condition_type: &str,
        cell: &ResultCell,
        rule: &ConfigMap,
        table_name: &String,
        column_name: &String,
        compiled_rules: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    ) -> bool {
        let condition = rule
            .get(format!("{} condition", condition_type).as_str())
            .and_then(|c| c.as_str())
            .unwrap();
        if vec!["null", "not null"].contains(&condition) {
            return (condition == "null" && cell.nulltype != None)
                || (condition == "not null" && cell.nulltype == None);
        } else {
            let compiled_condition = compiled_rules
                .get(table_name)
                .and_then(|t| t.get(column_name))
                .and_then(|v| {
                    v.iter().find(|c| {
                        if condition_type == "when" {
                            c.when.original == condition
                        } else {
                            c.then.original == condition
                        }
                    })
                })
                .and_then(|c| {
                    if condition_type == "when" {
                        Some(c.when.compiled.clone())
                    } else {
                        Some(c.then.compiled.clone())
                    }
                })
                .unwrap();
            return compiled_condition(&cell.value);
        }
    }

    let rules_config = config.get("rule").and_then(|r| r.as_object()).unwrap();
    let applicable_rules;
    match rules_config.get(table_name) {
        Some(SerdeValue::Object(table_rules)) => {
            match table_rules.get(column_name) {
                Some(SerdeValue::Array(column_rules)) => {
                    applicable_rules = column_rules;
                }
                _ => return,
            };
        }
        _ => return,
    };

    for (rule_number, rule) in applicable_rules.iter().enumerate() {
        // enumerate() begins at 0 by default but we need to begin with 1:
        let rule_number = rule_number + 1;
        let rule = rule.as_object().unwrap();
        if check_condition("when", cell, rule, table_name, column_name, compiled_rules) {
            let then_column = rule.get("then column").and_then(|c| c.as_str()).unwrap();
            let then_cell = context.get(then_column).unwrap();
            if !check_condition("then", then_cell, rule, table_name, column_name, compiled_rules) {
                cell.valid = false;
                cell.messages.push(json!({
                    "rule": format!("rule:{}-{}", column_name, rule_number),
                    "level": rule.get("level").unwrap(),
                    "message": rule.get("description").unwrap(),
                }));
            }
        }
    }
}
