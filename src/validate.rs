use serde_json::{
    json,
    //to_string,
    // SerdeMap by default backed by BTreeMap (see https://docs.serde.rs/serde_json/map/index.html)
    //Map as SerdeMap,
    Value as SerdeValue,
};
use sqlx::sqlite::SqlitePool;
use sqlx::{query, Row};
use std::collections::HashMap;

// provides `try_next` for sqlx:
use futures::TryStreamExt;

use crate::cmi_pb_grammar::StartParser;
use crate::{ColumnRule, CompiledCondition, ConfigMap, ParsedStructure};

#[derive(Clone, Debug)]
pub struct ResultCell {
    // TODO: these pubs are only needed for the ad hoc unit test that will be removed later, so
    // also remove these pubs at that time.
    pub nulltype: Option<String>,
    // NOTE: Unlike in the python version, a result cell will *always* have a value, so we need
    // to look at the `valid` field to determine whether to use it or not.
    pub value: String,
    pub valid: bool,
    pub messages: Vec<SerdeValue>,
}

#[derive(Clone, Debug)]
pub struct ResultRow {
    // TODO: make sure we really need to declare these as pub.
    pub row_number: Option<usize>,
    pub contents: HashMap<String, ResultCell>,
}

pub fn validate_rows_intra(
    config: &ConfigMap,
    // TODO: Remove these underscores later.
    _pool: &SqlitePool,
    _parser: &StartParser,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    // TODO: Remove this underscore later
    _parsed_structure_conditions: &HashMap<String, ParsedStructure>,
    table_name: &String,
    headers: &csv::StringRecord,
    rows: &mut Vec<Result<csv::StringRecord, csv::Error>>,
) -> Vec<ResultRow> {
    let mut result_rows: Vec<ResultRow> = vec![];
    for row in rows {
        if let Ok(row) = row {
            let mut result_row = ResultRow { row_number: None, contents: HashMap::new() };
            let mut column_names: Vec<String> = vec![];
            for (i, value) in row.iter().enumerate() {
                let result_cell = ResultCell {
                    nulltype: None,
                    value: String::from(value),
                    valid: true,
                    messages: vec![],
                };
                let column = headers.get(i).unwrap();
                result_row.contents.insert(column.to_string(), result_cell);
                column_names.push(column.to_string());
            }

            // We check all the cells for nulltype first, since the rules validation requires that we
            // have this information for all cells.
            for column_name in &column_names {
                let cell: &mut ResultCell = result_row.contents.get_mut(column_name).unwrap();
                validate_cell_nulltype(
                    config,
                    compiled_datatype_conditions,
                    table_name,
                    &column_name,
                    cell,
                );
            }

            for column_name in &column_names {
                //let context = result_row.clone();
                let context = ResultRow { row_number: result_row.row_number,
                                          contents: result_row.contents.clone() };
                let cell: &mut ResultCell = result_row.contents.get_mut(column_name).unwrap();
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

    // Finally return the result rows:
    result_rows
}

pub async fn validate_rows_trees(
    config: &ConfigMap,
    pool: &SqlitePool,
    parser: &StartParser,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    parsed_structure_conditions: &HashMap<String, ParsedStructure>,
    table_name: &String,
    headers: &csv::StringRecord,
    rows: &mut Vec<ResultRow>,
) -> Result<(), sqlx::Error> {
    let mut result_rows = vec![];
    for row in rows {
        let mut result_row = ResultRow { row_number: None, contents: HashMap::new() };
        for column_name in row.contents.keys().cloned().collect::<Vec<_>>() {
            //let context = row.clone();
            let context = ResultRow { row_number: row.row_number, contents: row.contents.clone() };
            let cell: &mut ResultCell = row.contents.get_mut(&column_name).unwrap();
            if cell.nulltype == None {
                validate_cell_trees(
                    config,
                    pool,
                    parser,
                    compiled_datatype_conditions,
                    compiled_rule_conditions,
                    parsed_structure_conditions,
                    table_name,
                    headers,
                    &column_name,
                    cell,
                    &context,
                    &result_rows,
                )
                .await?;
            }
            result_row.contents.insert(column_name.to_string(), cell.clone());
        }
        // Note that in this implementation, the result rows are never actually returned, but we
        // still need them because the validate_cell_trees() function needs a list of previous
        // results, and this then requires that we generate the result rows to play that role. The
        // call to cell.clone() above is required to make rust's borrow checker happy.
        result_rows.push(result_row);
    }

    Ok(())
}

pub async fn validate_rows_constraints(
    config: &ConfigMap,
    pool: &SqlitePool,
    parser: &StartParser,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    parsed_structure_conditions: &HashMap<String, ParsedStructure>,
    table_name: &String,
    headers: &csv::StringRecord,
    rows: &mut Vec<ResultRow>,
) -> Result<(), sqlx::Error> {

    Ok(())
}

fn with_tree_sql(
    tree: &ConfigMap,
    table_name: &String,
    root: Option<String>,
    extra_clause: Option<String>,
    // TODO (maybe): Instead of a tuple, define a new struct called SafeSql with two properties:
    // text and bind_params. Then we could use this elsewhere as well.
) -> (String, Vec<String>) {
    let extra_clause = extra_clause.unwrap_or(String::new());
    let child_col = tree.get("child").and_then(|c| c.as_str()).unwrap();
    let parent_col = tree.get("parent").and_then(|c| c.as_str()).unwrap();

    let mut params = vec![];
    let under_sql;
    if let Some(root) = root {
        under_sql = format!(r#"WHERE "{}" = ?"#, child_col);
        params.push(root.clone());
    } else {
        under_sql = String::new();
    }

    let sql = format!(
        r#"WITH RECURSIVE "tree" AS (
           {}
               SELECT "{}", "{}" 
                   FROM "{}" 
                   {} 
                   UNION ALL 
               SELECT "t1"."{}", "t1"."{}" 
                   FROM "{}" AS "t1" 
                   JOIN "tree" AS "t2" ON "t2"."{}" = "t1"."{}"
           )"#,
        extra_clause,
        child_col,
        parent_col,
        table_name,
        under_sql,
        child_col,
        parent_col,
        table_name,
        parent_col,
        child_col
    );

    (sql, params)
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

async fn validate_cell_trees(
    config: &ConfigMap,
    pool: &SqlitePool,
    // TODO: Remove these underscores later
    _parser: &StartParser,
    _compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    _compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    _parsed_structure_conditions: &HashMap<String, ParsedStructure>,
    table_name: &String,
    // TODO: Remove this underscore later
    _headers: &csv::StringRecord,
    column_name: &String,
    cell: &mut ResultCell,
    context: &ResultRow,
    prev_results: &Vec<ResultRow>,
) -> Result<(), sqlx::Error> {
    let tkeys = config
        .get("constraints")
        .and_then(|c| c.as_object())
        .and_then(|o| o.get("tree"))
        .and_then(|t| t.as_object())
        .and_then(|o| o.get(table_name))
        .and_then(|t| t.as_array())
        .unwrap()
        .iter()
        .filter(|t| {
            t.as_object()
                .and_then(|o| o.get("parent"))
                .and_then(|p| Some(p == column_name))
                .unwrap()
        })
        .map(|v| v.as_object().unwrap())
        .collect::<Vec<_>>();
    for tkey in tkeys {
        let parent_col = column_name;
        let child_col = tkey.get("child").and_then(|c| c.as_str()).unwrap();
        let parent_val = cell.value.clone();
        let child_val = context.contents.get(child_col).and_then(|c| Some(c.value.clone())).unwrap();

        let mut params = vec![];
        // It would have been nice to use query_builder to build up the query dynamically
        // (https://docs.rs/sqlx/latest/sqlx/query_builder/index.html), but unfortunately either
        // the documentation is out of date or else there is something wrong in the crate, because
        // when we try to: use sqlx::query_builder::QueryBuilder it tells us that this is not found
        // in the crate.
        let prev_selects = prev_results
            .iter()
            .filter(|p| p.contents.get(child_col).unwrap().valid && p.contents.get(parent_col).unwrap().valid)
            .map(|p| {
                params.push(p.contents.get(child_col).unwrap().value.clone());
                params.push(p.contents.get(parent_col).unwrap().value.clone());
                format!(r#"SELECT ? AS "{}", ? AS "{}""#, child_col, parent_col)
            })
            .collect::<Vec<_>>();
        let prev_selects = prev_selects.join(" UNION ");

        let table_name_ext;
        let extra_clause;
        if prev_selects.is_empty() {
            table_name_ext = table_name.clone();
            extra_clause = String::from("");
        } else {
            table_name_ext = format!("{}_ext", table_name);
            extra_clause = format!(
                r#"WITH "{}" AS (
                       SELECT "{}", "{}"
                           FROM "{}"
                           UNION
                       {}
                   )"#,
                table_name_ext, child_col, parent_col, table_name, prev_selects
            );
        }

        let (tree_sql, mut tree_sql_params) =
            with_tree_sql(&tkey, &table_name_ext, Some(parent_val.clone()), Some(extra_clause));
        params.append(&mut tree_sql_params);
        let sql = format!(r#"{} SELECT * FROM "tree""#, tree_sql,);
        let mut my_query = query(&sql);
        for param in &params {
            my_query = my_query.bind(param);
        }
        let rows = my_query.fetch_all(pool).await?;

        let cycle_detected = {
            let cycle_row = rows.iter().find(|row| {
                let parent: Result<&str, sqlx::Error> = row.try_get(parent_col.as_str());
                if let Ok(parent) = parent {
                    parent == child_val
                } else {
                    false
                }
            });
            match cycle_row {
                None => false,
                _ => true,
            }
        };

        if cycle_detected {
            let mut cycle_legs = vec![];
            for row in &rows {
                let child: &str = row.try_get(child_col).unwrap();
                let parent: &str = row.try_get(parent_col.as_str()).unwrap();
                cycle_legs.push((child, parent));
            }
            cycle_legs.push((&child_val, &parent_val));

            let mut cycle_msg = vec![];
            for cycle in &cycle_legs {
                cycle_msg
                    .push(format!("({}: {}, {}: {})", child_col, cycle.0, parent_col, cycle.1));
            }
            let cycle_msg = cycle_msg.join(", ");
            cell.valid = false;
            cell.messages.push(json!({
                "rule": "tree:cycle",
                "level": "error",
                "message": format!("Cyclic dependency: {} for tree({}) of {}",
                                   cycle_msg, parent_col, child_col),
            }));
        }
    }

    Ok(())
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
            let then_cell = context.contents.get(then_column).unwrap();
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
