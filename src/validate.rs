use enquote::unquote;
use serde_json::{json, Value as SerdeValue};
use sqlx::any::AnyPool;
use sqlx::ValueRef;
use sqlx::{query as sqlx_query, Row};
use std::collections::HashMap;

use crate::{
    ast::Expression, get_sql_type_from_global_config, local_sql_syntax, ColumnRule,
    CompiledCondition, ConfigMap, ParsedStructure, SQL_PARAM,
};

/// Represents a particular cell in a particular row of data with vaildation results.
#[derive(Clone, Debug)]
pub struct ResultCell {
    pub nulltype: Option<String>,
    pub value: String,
    pub valid: bool,
    pub messages: Vec<SerdeValue>,
}

/// Represents a particular row of data with validation results.
#[derive(Clone, Debug)]
pub struct ResultRow {
    pub row_number: Option<u32>,
    pub contents: HashMap<String, ResultCell>,
}

/// Given a result row, convert it to a ConfigMap (an alias for serde_json::Value) and return it.
/// Note that if the incoming result row has an associated row_number, this is ignored.
fn result_row_to_config_map(incoming: &ResultRow) -> ConfigMap {
    let mut outgoing = ConfigMap::new();
    for (column, cell) in incoming.contents.iter() {
        let mut cell_map = ConfigMap::new();
        if let Some(nulltype) = &cell.nulltype {
            cell_map.insert("nulltype".to_string(), SerdeValue::String(nulltype.to_string()));
        }
        cell_map.insert("value".to_string(), SerdeValue::String(cell.value.to_string()));
        cell_map.insert("valid".to_string(), SerdeValue::Bool(cell.valid));
        cell_map.insert("messages".to_string(), SerdeValue::Array(cell.messages.clone()));
        outgoing.insert(column.to_string(), SerdeValue::Object(cell_map));
    }
    outgoing
}

/// Given a config map, maps of compiled datatype and rule conditions, a database connection
/// pool, a table name, a row to validate, and a row number in case the row already exists,
/// perform both intra- and inter-row validation and return the validated row.
pub async fn validate_row(
    config: &ConfigMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
    table_name: &str,
    row: &ConfigMap,
    existing_row: bool,
    row_number: Option<u32>,
) -> Result<ConfigMap, sqlx::Error> {
    // If existing_row is false, then override any row number provided with None:
    let mut row_number = row_number.clone();
    if !existing_row {
        row_number = None;
    }

    // Initialize the result row with the values from the given row:
    let mut result_row = ResultRow { row_number: row_number, contents: HashMap::new() };
    for (column, cell) in row.iter() {
        let result_cell = ResultCell {
            nulltype: cell
                .get("nulltype")
                .and_then(|n| Some(n.as_str().unwrap()))
                .and_then(|n| Some(n.to_string())),
            value: match cell.get("value") {
                Some(SerdeValue::String(s)) => s.to_string(),
                Some(SerdeValue::Number(n)) => format!("{}", n),
                _ => panic!("Cell value is neither a number nor a string."),
            },
            valid: cell.get("valid").and_then(|v| v.as_bool()).unwrap(),
            messages: cell.get("messages").and_then(|m| m.as_array()).unwrap().to_vec(),
        };
        result_row.contents.insert(column.to_string(), result_cell);
    }

    // We check all the cells for nulltype first, since the rules validation requires that we
    // have this information for all cells.
    for (column_name, cell) in result_row.contents.iter_mut() {
        validate_cell_nulltype(
            config,
            compiled_datatype_conditions,
            &table_name.to_string(),
            column_name,
            cell,
        );
    }

    let context = result_row.clone();
    for (column_name, cell) in result_row.contents.iter_mut() {
        validate_cell_rules(
            config,
            compiled_rule_conditions,
            &table_name.to_string(),
            column_name,
            &context,
            cell,
        );

        if cell.nulltype == None {
            validate_cell_datatype(
                config,
                compiled_datatype_conditions,
                &table_name.to_string(),
                column_name,
                cell,
            );
            validate_cell_trees(
                config,
                pool,
                &table_name.to_string(),
                column_name,
                cell,
                &context,
                &vec![],
            )
            .await?;
            validate_cell_foreign_constraints(
                config,
                pool,
                &table_name.to_string(),
                column_name,
                cell,
            )
            .await?;
            validate_cell_unique_constraints(
                config,
                pool,
                &table_name.to_string(),
                column_name,
                cell,
                &vec![],
                existing_row,
                row_number,
            )
            .await?;
        }
    }

    let mut violations =
        validate_tree_foreign_keys(config, pool, &table_name.to_string(), Some(context.clone()))
            .await?;
    violations.append(
        &mut validate_under(config, pool, &table_name.to_string(), Some(context.clone())).await?,
    );

    for violation in violations.iter_mut() {
        let vrow_number = violation.get("row_number").unwrap().as_i64().unwrap() as u32;
        if Some(vrow_number) == row_number || (row_number == None && Some(vrow_number) == Some(0)) {
            let column = violation.get("column").and_then(|c| c.as_str()).unwrap().to_string();
            let column_meta = violation.get_mut("meta").unwrap();
            // If there were any previous error messages for this cell, add them to the meta info
            // that we just received:
            let result_cell = &mut result_row.contents.get_mut(&column).unwrap();
            let column_meta_messages =
                column_meta.get_mut("messages").and_then(|m| m.as_array_mut()).unwrap();
            result_cell.messages.append(column_meta_messages);
            if result_cell.messages.len() > 0 {
                result_cell.valid = false;
            }
        }
    }

    let result_row = result_row_to_config_map(&result_row);

    Ok(result_row)
}

/// Given a config map, a map of compiled datatype conditions, a database connection pool, a table
/// name, a column name, and (optionally) a string to match, return a JSON array of possible valid
/// values for the given column which contain the matching string as a substring (or all of them if
/// no matching string is given). The JSON array returned is formatted for Typeahead, i.e., it takes
/// the form: `[{"id": id, "label": label, "order": order}, ...]`.
pub async fn get_matching_values(
    config: &ConfigMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    parsed_structure_conditions: &HashMap<String, ParsedStructure>,
    pool: &AnyPool,
    table_name: &str,
    column_name: &str,
    matching_string: Option<&str>,
) -> Result<SerdeValue, sqlx::Error> {
    let dt_name = config
        .get("table")
        .and_then(|t| t.as_object())
        .and_then(|t| t.get(table_name))
        .and_then(|t| t.as_object())
        .and_then(|t| t.get("column"))
        .and_then(|c| c.as_object())
        .and_then(|c| c.get(column_name))
        .and_then(|c| c.as_object())
        .and_then(|c| c.get("datatype"))
        .and_then(|d| d.as_str())
        .unwrap();

    let dt_condition =
        compiled_datatype_conditions.get(dt_name).and_then(|d| Some(d.parsed.clone()));

    let mut values = vec![];
    match dt_condition {
        Some(Expression::Function(name, args)) if name == "in" => {
            for arg in args {
                if let Expression::Label(arg) = *arg {
                    // Remove the enclosing quotes from the values being returned:
                    let label = unquote(&arg).unwrap_or(arg);
                    if let Some(s) = matching_string {
                        if label.contains(s) {
                            values.push(label);
                        }
                    }
                }
            }
        }
        _ => {
            // If the datatype for the column does not correspond to an `in(...)` function, then we
            // check the column's structure constraints. If they include a
            // `from(foreign_table.foreign_column)` condition, then the values are taken from the
            // foreign column. Otherwise if the structure includes an
            // `under(tree_table.tree_column, value)` condition, then get the values from the tree
            // column that are under `value`.
            let structure = parsed_structure_conditions.get(
                config
                    .get("table")
                    .and_then(|t| t.as_object())
                    .and_then(|t| t.get(table_name))
                    .and_then(|t| t.as_object())
                    .and_then(|t| t.get("column"))
                    .and_then(|c| c.as_object())
                    .and_then(|c| c.get(column_name))
                    .and_then(|c| c.as_object())
                    .and_then(|c| c.get("structure"))
                    .and_then(|d| d.as_str())
                    .unwrap(),
            );

            let sql_type =
                get_sql_type_from_global_config(&config, table_name, &column_name).unwrap();

            match structure {
                Some(ParsedStructure { original, parsed }) => {
                    let matching_string = {
                        match matching_string {
                            None => "%".to_string(),
                            Some(s) => format!("%{}%", s),
                        }
                    };

                    match parsed {
                        Expression::Function(name, args) if name == "from" => {
                            let foreign_key = &args[0];
                            if let Expression::Field(ftable, fcolumn) = &**foreign_key {
                                let fcolumn_text;
                                if sql_type.to_lowercase() == "integer" {
                                    fcolumn_text = format!(r#"CAST("{}" AS TEXT)"#, fcolumn);
                                } else {
                                    fcolumn_text = format!(r#""{}""#, fcolumn);
                                }
                                let sql = local_sql_syntax(
                                    &pool,
                                    &format!(
                                        r#"SELECT "{}" FROM "{}" WHERE {} LIKE {}"#,
                                        fcolumn, ftable, fcolumn_text, SQL_PARAM
                                    ),
                                );
                                let rows =
                                    sqlx_query(&sql).bind(&matching_string).fetch_all(pool).await?;
                                for row in rows.iter() {
                                    if sql_type.to_lowercase() == "integer" {
                                        let foo: i32 = row.get(format!(r#"{}"#, fcolumn).as_str());
                                        let foo: u32 = foo as u32;
                                        values.push(foo.to_string());
                                    } else {
                                        let value: &str = row.get(&**fcolumn);
                                        values.push(value.to_string());
                                    }
                                }
                            }
                        }
                        Expression::Function(name, args) if name == "under" || name == "tree" => {
                            let mut tree_col = "not set";
                            let mut under_val = Some("not set".to_string());
                            if name == "under" {
                                if let Expression::Field(_, column) = &**&args[0] {
                                    tree_col = column;
                                }
                                if let Expression::Label(label) = &**&args[1] {
                                    under_val = Some(label.to_string());
                                }
                            } else {
                                let tree_key = &args[0];
                                if let Expression::Label(label) = &**tree_key {
                                    tree_col = label;
                                    under_val = None;
                                }
                            }

                            let tree = config
                                .get("constraints")
                                .and_then(|c| c.as_object())
                                .and_then(|c| c.get("tree"))
                                .and_then(|t| t.as_object())
                                .and_then(|t| t.get(table_name))
                                .and_then(|t| t.as_array())
                                .and_then(|t| {
                                    t.iter().find(|o| o.get("child").unwrap() == tree_col)
                                })
                                .expect(
                                    format!("No tree: '{}.{}' found", table_name, tree_col)
                                        .as_str(),
                                )
                                .as_object()
                                .unwrap();
                            let child_column = tree.get("child").and_then(|c| c.as_str()).unwrap();

                            let (tree_sql, mut params) = with_tree_sql(
                                &config,
                                tree,
                                &table_name.to_string(),
                                &table_name.to_string(),
                                under_val,
                                None,
                            );
                            let child_column_text;
                            if sql_type.to_lowercase() == "integer" {
                                child_column_text = format!(r#"CAST("{}" AS TEXT)"#, child_column);
                            } else {
                                child_column_text = format!(r#""{}""#, child_column);
                            }
                            let sql = local_sql_syntax(
                                &pool,
                                &format!(
                                    r#"{} SELECT "{}" FROM "tree" WHERE {} LIKE {}"#,
                                    tree_sql, child_column, child_column_text, SQL_PARAM
                                ),
                            );
                            params.push(matching_string);

                            let mut query = sqlx_query(&sql);
                            for param in &params {
                                query = query.bind(param);
                            }

                            let rows = query.fetch_all(pool).await?;
                            for row in rows.iter() {
                                if sql_type.to_lowercase() == "integer" {
                                    let foo: i32 = row.get(format!(r#"{}"#, child_column).as_str());
                                    let foo: u32 = foo as u32;
                                    values.push(foo.to_string());
                                } else {
                                    let value: &str = row.get(child_column);
                                    values.push(value.to_string());
                                }
                            }
                        }
                        _ => panic!("Unrecognised structure: {}", original),
                    };
                }
                None => (),
            };
        }
    };

    let mut typeahead_values = vec![];
    for (i, v) in values.iter().enumerate() {
        // enumerate() begins at 0 but we need to begin at 1:
        let i = i + 1;
        typeahead_values.push(json!({
            "id": v,
            "label": v,
            "order": i,
        }));
    }

    Ok(json!(typeahead_values))
}

/// Given a config map, compiled datatype and rule conditions, a table name, the headers for the
/// table, and a number of rows to validate, validate all of the rows and return the validated
/// versions.
pub fn validate_rows_intra(
    config: &ConfigMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    table_name: &String,
    headers: &csv::StringRecord,
    rows: &Vec<Result<csv::StringRecord, csv::Error>>,
) -> Vec<ResultRow> {
    let mut result_rows = vec![];
    for row in rows {
        match row {
            Err(err) => eprintln!("Error while processing row for '{}': {}", table_name, err),
            Ok(row) => {
                let mut result_row = ResultRow { row_number: None, contents: HashMap::new() };
                for (i, value) in row.iter().enumerate() {
                    let result_cell = ResultCell {
                        nulltype: None,
                        value: String::from(value),
                        valid: true,
                        messages: vec![],
                    };
                    let column = headers.get(i).unwrap();
                    result_row.contents.insert(column.to_string(), result_cell);
                }

                let column_names = config
                    .get("table")
                    .and_then(|t| t.get(table_name))
                    .and_then(|t| t.get("column_order"))
                    .and_then(|c| c.as_array())
                    .unwrap()
                    .iter()
                    .map(|v| v.as_str().unwrap().to_string())
                    .collect::<Vec<_>>();

                // We begin by determining the nulltype of all of the cells, since the rules
                // validation step requires that all cells have this information.
                for column_name in &column_names {
                    let cell = result_row.contents.get_mut(column_name).unwrap();
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
                    let cell = result_row.contents.get_mut(column_name).unwrap();
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
        };
    }

    // Finally return the result rows:
    result_rows
}

/// Given a config map, a database connection pool, a table name, and a number of rows to validate,
/// perform tree validation on the rows and return the validated results.
pub async fn validate_rows_trees(
    config: &ConfigMap,
    pool: &AnyPool,
    table_name: &String,
    rows: &mut Vec<ResultRow>,
) -> Result<(), sqlx::Error> {
    let column_names = config
        .get("table")
        .and_then(|t| t.get(table_name))
        .and_then(|t| t.get("column_order"))
        .and_then(|c| c.as_array())
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect::<Vec<_>>();

    let mut result_rows = vec![];
    for row in rows {
        let mut result_row = ResultRow { row_number: None, contents: HashMap::new() };
        for column_name in &column_names {
            let context = row.clone();
            let cell = row.contents.get_mut(column_name).unwrap();
            if cell.nulltype == None {
                validate_cell_trees(
                    config,
                    pool,
                    table_name,
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

/// Given a config map, a database connection pool, a table name, and a number of rows to validate,
/// validate foreign and unique constraints, where the latter include primary and "tree child" keys
/// (which imply unique constraints) and return the validated results.
pub async fn validate_rows_constraints(
    config: &ConfigMap,
    pool: &AnyPool,
    table_name: &String,
    rows: &mut Vec<ResultRow>,
) -> Result<(), sqlx::Error> {
    let column_names = config
        .get("table")
        .and_then(|t| t.get(table_name))
        .and_then(|t| t.get("column_order"))
        .and_then(|c| c.as_array())
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect::<Vec<_>>();

    let mut result_rows = vec![];
    for row in rows.iter_mut() {
        let mut result_row = ResultRow { row_number: None, contents: HashMap::new() };
        for column_name in &column_names {
            let cell = row.contents.get_mut(column_name).unwrap();
            if cell.nulltype == None {
                validate_cell_foreign_constraints(config, pool, table_name, &column_name, cell)
                    .await?;

                validate_cell_unique_constraints(
                    config,
                    pool,
                    table_name,
                    &column_name,
                    cell,
                    &result_rows,
                    false,
                    None,
                )
                .await?;
            }
            result_row.contents.insert(column_name.to_string(), cell.clone());
        }
        // Note that in this implementation, the result rows are never actually returned, but we
        // still need them because the validate_cell_unique_constraints() function needs a list of
        // previous results, and this then requires that we generate the result rows to play that
        // role. The call to cell.clone() above is required to make rust's borrow checker happy.
        result_rows.push(result_row);
    }

    Ok(())
}

/// Given a map representing a tree constraint, a table name, a root from which to generate a
/// sub-tree of the tree, and an extra SQL clause, generate the SQL for a WITH clause representing
/// the sub-tree.
fn with_tree_sql(
    config: &ConfigMap,
    tree: &ConfigMap,
    table_name: &str,
    effective_table_name: &str,
    root: Option<String>,
    extra_clause: Option<String>,
) -> (String, Vec<String>) {
    let extra_clause = extra_clause.unwrap_or(String::new());
    let child_col = tree.get("child").and_then(|c| c.as_str()).unwrap();
    let parent_col = tree.get("parent").and_then(|c| c.as_str()).unwrap();

    let mut params = vec![];
    let under_sql;
    if let Some(root) = root {
        let sql_type = get_sql_type_from_global_config(&config, table_name, &child_col).unwrap();
        let sql_param;
        if sql_type.to_lowercase() == "integer" {
            sql_param = format!("CAST({} AS INTEGER)", SQL_PARAM);
        } else {
            sql_param = String::from(SQL_PARAM);
        }
        under_sql = format!(r#"WHERE "{}" = {}"#, child_col, sql_param);
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
        effective_table_name,
        under_sql,
        child_col,
        parent_col,
        effective_table_name,
        parent_col,
        child_col
    );

    (sql, params)
}

/// Given a config map, compiled datatype conditions, a table name, a column name, and a cell to
/// validate, validate the cell's nulltype condition. If the cell's value is one of the allowable
/// nulltype values for this column, then fill in the cell's nulltype value before returning the
/// cell.
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

/// Given a config map, a db connection pool, a table name, a column name, and a cell to validate,
/// check the cell value against any foreign keys that have been defined for the column. If there is
/// a violation, indicate it with an error message attached to the cell.
async fn validate_cell_foreign_constraints(
    config: &ConfigMap,
    pool: &AnyPool,
    table_name: &String,
    column_name: &String,
    cell: &mut ResultCell,
) -> Result<(), sqlx::Error> {
    let fkeys = config
        .get("constraints")
        .and_then(|c| c.as_object())
        .and_then(|o| o.get("foreign"))
        .and_then(|t| t.as_object())
        .and_then(|o| o.get(table_name))
        .and_then(|t| t.as_array())
        .unwrap()
        .iter()
        .filter(|t| {
            t.as_object()
                .and_then(|o| o.get("column"))
                .and_then(|p| Some(p == column_name))
                .unwrap()
        })
        .map(|v| v.as_object().unwrap())
        .collect::<Vec<_>>();

    for fkey in fkeys {
        let ftable = fkey.get("ftable").and_then(|t| t.as_str()).unwrap();
        let fcolumn = fkey.get("fcolumn").and_then(|c| c.as_str()).unwrap();
        let sql_type = get_sql_type_from_global_config(&config, &ftable, &fcolumn).unwrap();
        let sql_param;
        if sql_type.to_lowercase() == "integer" {
            sql_param = format!("CAST({} AS INTEGER)", SQL_PARAM);
        } else {
            sql_param = String::from(SQL_PARAM);
        }
        let fsql = local_sql_syntax(
            &pool,
            &format!(r#"SELECT 1 FROM "{}" WHERE "{}" = {} LIMIT 1"#, ftable, fcolumn, sql_param),
        );
        let frows = sqlx_query(&fsql).bind(&cell.value).fetch_all(pool).await?;

        if frows.is_empty() {
            cell.valid = false;
            let mut message = json!({
                "rule": "key:foreign",
                "level": "error",
            });

            let fsql = local_sql_syntax(
                &pool,
                &format!(
                    r#"SELECT 1 FROM "{}_conflict" WHERE "{}" = {} LIMIT 1"#,
                    ftable, fcolumn, sql_param
                ),
            );
            let frows = sqlx_query(&fsql).bind(cell.value.clone()).fetch_all(pool).await?;

            if frows.is_empty() {
                message.as_object_mut().and_then(|m| {
                    m.insert(
                        "message".to_string(),
                        SerdeValue::String(format!(
                            "Value {} of column {} is not in {}.{}",
                            cell.value, column_name, ftable, fcolumn
                        )),
                    )
                });
            } else {
                message.as_object_mut().and_then(|m| {
                    m.insert(
                        "message".to_string(),
                        SerdeValue::String(format!(
                            "Value {} of column {} exists only in {}_conflict.{}",
                            cell.value, column_name, ftable, fcolumn
                        )),
                    )
                });
            }
            cell.messages.push(message);
        }
    }

    Ok(())
}

/// Given a config map, a db connection pool, a table name, a column name, a cell to validate,
/// the row, `context`, to which the cell belongs, and a list of previously validated rows,
/// validate that none of the "tree" constraints on the column are violated, and indicate any
/// violations by attaching error messages to the cell.
async fn validate_cell_trees(
    config: &ConfigMap,
    pool: &AnyPool,
    table_name: &String,
    column_name: &String,
    cell: &mut ResultCell,
    context: &ResultRow,
    prev_results: &Vec<ResultRow>,
) -> Result<(), sqlx::Error> {
    // If the current column is the parent column of a tree, validate that adding the current value
    // will not result in a cycle between this and the parent column:
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
        let child_val =
            context.contents.get(child_col).and_then(|c| Some(c.value.clone())).unwrap();

        // In order to check if the current row will cause a dependency cycle, we need to query
        // against all previously validated rows. Since previously validated rows belonging to the
        // current batch will not have been inserted to the db yet, we explicitly add them in:
        let mut params = vec![];
        let prev_selects = prev_results
            .iter()
            .filter(|p| {
                p.contents.get(child_col).unwrap().valid
                    && p.contents.get(parent_col).unwrap().valid
            })
            .map(|p| {
                params.push(p.contents.get(child_col).unwrap().value.clone());
                params.push(p.contents.get(parent_col).unwrap().value.clone());
                let sql_type =
                    get_sql_type_from_global_config(&config, &table_name, &column_name).unwrap();
                let sql_param;
                if sql_type.to_lowercase() == "integer" {
                    sql_param = format!("CAST({} AS INTEGER)", SQL_PARAM);
                } else {
                    sql_param = String::from(SQL_PARAM);
                }
                format!(
                    r#"SELECT {} AS "{}", {} AS "{}""#,
                    sql_param, child_col, sql_param, parent_col
                )
            })
            .collect::<Vec<_>>();
        let prev_selects = prev_selects.join(" UNION ALL ");

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
                           UNION ALL
                       {}
                   )"#,
                table_name_ext, child_col, parent_col, table_name, prev_selects
            );
        }

        let (tree_sql, mut tree_sql_params) = with_tree_sql(
            &config,
            &tkey,
            &table_name,
            &table_name_ext,
            Some(parent_val.clone()),
            Some(extra_clause),
        );
        params.append(&mut tree_sql_params);
        let sql = local_sql_syntax(
            &pool,
            &format!(r#"{} SELECT "{}", "{}" FROM "tree""#, tree_sql, child_col, parent_col),
        );

        let mut query = sqlx_query(&sql);
        for param in &params {
            query = query.bind(param);
        }
        let rows = query.fetch_all(pool).await?;

        // If there is a row in the tree whose parent is the to-be-inserted child, then inserting
        // the new row would result in a cycle.
        let cycle_detected = {
            let cycle_row = rows.iter().find(|row| {
                let sql_type =
                    get_sql_type_from_global_config(&config, &table_name, &parent_col).unwrap();
                if sql_type.to_lowercase() == "integer" {
                    let raw_foo = row.try_get_raw(format!(r#"{}"#, parent_col).as_str()).unwrap();
                    if raw_foo.is_null() {
                        false
                    } else {
                        let foo: i32 = row.get(format!(r#"{}"#, parent_col).as_str());
                        let foo: u32 = foo as u32;
                        foo.to_string() == child_val
                    }
                } else {
                    let parent: Result<&str, sqlx::Error> = row.try_get(parent_col.as_str());
                    if let Ok(parent) = parent {
                        parent == child_val
                    } else {
                        false
                    }
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
                let child: &str = row.get(child_col);
                let parent: &str = row.get(parent_col.as_str());
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

/// Given a config map, compiled datatype conditions, a table name, a column name, and a cell to
/// validate, validate the cell's datatype and return the validated cell.
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
            // If this datatype has any parents, check them beginning from the most general to the
            // most specific. We use while and pop instead of a for loop so as to check the
            // conditions in LIFO order.
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

/// Given a config map, compiled rule conditions, a table name, a column name, the row context,
/// and the cell to validate, look in the rule table (if it exists) and validate the cell according
/// to any applicable rules.
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
        // Check the then condition only if the when condition is satisfied:
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

/// Given a config map, a db connection pool, a table name, a column name, a cell to validate,
/// the row, `context`, to which the cell belongs, and a list of previously validated rows,
/// check the cell value against any unique-type keys that have been defined for the column.
/// If there is a violation, indicate it with an error message attached to the cell. If
/// the `existing_row` flag is set to True, then checks will be made as if the given `row_number`
/// does not exist in the table.
async fn validate_cell_unique_constraints(
    config: &ConfigMap,
    pool: &AnyPool,
    table_name: &String,
    column_name: &String,
    cell: &mut ResultCell,
    prev_results: &Vec<ResultRow>,
    existing_row: bool,
    row_number: Option<u32>,
) -> Result<(), sqlx::Error> {
    // If existing_row is false, then override any row number provided with None:
    let mut row_number = row_number.clone();
    if !existing_row {
        row_number = None;
    }
    // If the column has a primary or unique key constraint, or if it is the child associated with
    // a tree, then if the value of the cell is a duplicate either of one of the previously
    // validated rows in the batch, or a duplicate of a validated row that has already been inserted
    // into the table, mark it with the corresponding error:
    let primaries = config
        .get("constraints")
        .and_then(|c| c.as_object())
        .and_then(|o| o.get("primary"))
        .and_then(|t| t.as_object())
        .and_then(|o| o.get(table_name))
        .and_then(|t| t.as_array())
        .unwrap();
    let uniques = config
        .get("constraints")
        .and_then(|c| c.as_object())
        .and_then(|o| o.get("unique"))
        .and_then(|t| t.as_object())
        .and_then(|o| o.get(table_name))
        .and_then(|t| t.as_array())
        .unwrap();
    let trees = config
        .get("constraints")
        .and_then(|c| c.as_object())
        .and_then(|o| o.get("tree"))
        .and_then(|t| t.as_object())
        .and_then(|o| o.get(table_name))
        .and_then(|t| t.as_array())
        .and_then(|a| Some(a.iter().map(|o| o.as_object().and_then(|o| o.get("child")).unwrap())))
        .unwrap()
        .collect::<Vec<_>>();

    let is_primary = primaries.contains(&SerdeValue::String(column_name.to_string()));
    let is_unique = !is_primary && uniques.contains(&SerdeValue::String(column_name.to_string()));
    let is_tree_child = trees.contains(&&SerdeValue::String(column_name.to_string()));

    fn make_error(rule: &str, column_name: &String) -> SerdeValue {
        json!({
            "rule": rule.to_string(),
            "level": "error",
            "message": format!("Values of {} must be unique", column_name.to_string()),
        })
    }

    if is_primary || is_unique || is_tree_child {
        let mut with_sql = String::new();
        let except_table = format!("{}_exc", table_name);
        if existing_row {
            with_sql = format!(
                r#"WITH "{}" AS (
                       SELECT * FROM "{}"
                       WHERE "row_number" != {}
                   ) "#,
                except_table,
                table_name,
                row_number.unwrap()
            );
        }

        let query_table;
        if !with_sql.is_empty() {
            query_table = except_table;
        } else {
            query_table = table_name.to_string();
        }

        let sql_type = get_sql_type_from_global_config(&config, &table_name, &column_name).unwrap();
        let sql_param;
        if sql_type.to_lowercase() == "integer" {
            sql_param = format!("CAST({} AS INTEGER)", SQL_PARAM);
        } else {
            sql_param = String::from(SQL_PARAM);
        }
        let sql = local_sql_syntax(
            &pool,
            &format!(
                r#"{} SELECT 1 FROM "{}" WHERE "{}" = {} LIMIT 1"#,
                with_sql, query_table, column_name, sql_param
            ),
        );
        let query = sqlx_query(&sql).bind(&cell.value);

        let contained_in_prev_results = !prev_results
            .iter()
            .filter(|p| {
                p.contents.get(column_name).unwrap().value == cell.value
                    && p.contents.get(column_name).unwrap().valid
            })
            .collect::<Vec<_>>()
            .is_empty();

        if contained_in_prev_results || !query.fetch_all(pool).await?.is_empty() {
            cell.valid = false;
            if is_primary || is_unique {
                let error_message;
                if is_primary {
                    error_message = make_error("key:primary", column_name);
                } else {
                    error_message = make_error("key:unique", column_name);
                }
                cell.messages.push(error_message);
            }
            if is_tree_child {
                let error_message = make_error("tree:child-unique", column_name);
                cell.messages.push(error_message);
            }
        }
    }
    Ok(())
}

/// Generate a SQL Select clause that is a union of: (a) the literal values of the given extra row,
/// and (b) a Select statement over `table_name` of all the fields in the extra row.
fn select_with_extra_row(
    config: &ConfigMap,
    extra_row: &ResultRow,
    table_name: &str,
) -> (String, Vec<String>) {
    let extra_row_len = extra_row.contents.keys().len();
    let mut params = vec![];
    let mut first_select;
    match extra_row.row_number {
        Some(rn) => first_select = format!(r#"SELECT {} AS "row_number", "#, rn),
        _ => first_select = String::from(r#"SELECT NULL AS "row_number", "#),
    };

    let mut second_select = String::from(r#"SELECT "row_number", "#);
    for (i, (key, content)) in extra_row.contents.iter().enumerate() {
        let sql_type = get_sql_type_from_global_config(&config, &table_name, &key).unwrap();
        let sql_param;
        if sql_type.to_lowercase() == "integer" {
            sql_param = format!("CAST({} AS INTEGER)", SQL_PARAM);
        } else {
            sql_param = String::from(SQL_PARAM);
        }
        // enumerate() begins from 0 but we need to begin at 1:
        let i = i + 1;
        first_select.push_str(format!(r#"{} AS "{}", "#, sql_param, key).as_str());
        params.push(content.value.to_string());
        first_select.push_str(format!(r#"NULL AS "{}_meta""#, key).as_str());
        second_select.push_str(format!(r#""{}", "{}_meta""#, key, key).as_str());
        if i < extra_row_len {
            first_select.push_str(", ");
            second_select.push_str(", ");
        } else {
            second_select.push_str(format!(r#" FROM "{}""#, table_name).as_str());
        }
    }

    (
        format!(r#"WITH "{}_ext" AS ({} UNION ALL {})"#, table_name, first_select, second_select),
        params,
    )
}

/// Given a config map, a db connection pool, a table name, and an optional extra row, validate
/// any associated under constraints for the current column.
pub async fn validate_under(
    config: &ConfigMap,
    pool: &AnyPool,
    table_name: &String,
    extra_row: Option<ResultRow>,
) -> Result<Vec<SerdeValue>, sqlx::Error> {
    let mut results = vec![];
    let ukeys = config
        .get("constraints")
        .and_then(|c| c.as_object())
        .and_then(|o| o.get("under"))
        .and_then(|t| t.as_object())
        .and_then(|o| o.get(table_name))
        .and_then(|t| t.as_array())
        .unwrap();

    for ukey in ukeys {
        let ukey = ukey.as_object().unwrap();
        let tree_table = ukey.get("ttable").and_then(|tt| tt.as_str()).unwrap();
        let tree_child = ukey.get("tcolumn").and_then(|tc| tc.as_str()).unwrap();
        let column = ukey.get("column").and_then(|c| c.as_str()).unwrap();
        let tree = config
            .get("constraints")
            .and_then(|c| c.as_object())
            .and_then(|o| o.get("tree"))
            .and_then(|t| t.as_object())
            .and_then(|o| o.get(tree_table))
            .and_then(|t| t.as_array())
            .unwrap()
            .iter()
            .find(|tkey| {
                tkey.as_object()
                    .and_then(|o| o.get("child"))
                    .and_then(|c| Some(c == tree_child))
                    .unwrap()
            })
            .and_then(|tree| Some(tree.as_object().unwrap()))
            .unwrap();
        let tree_parent = tree.get("parent").and_then(|p| p.as_str()).unwrap();
        let mut extra_clause;
        let mut params;
        if let Some(ref extra_row) = extra_row {
            (extra_clause, params) = select_with_extra_row(&config, extra_row, table_name);
        } else {
            extra_clause = String::new();
            params = vec![];
        }

        // For each value of the column to be checked:
        // (1) Determine whether it is in the tree's child column.
        // (2) Create a sub-tree of the given tree whose root is the given "under value"
        //     (i.e., ukey["value"]). Now on the one hand, if the value to be checked is in the
        //     parent column of that sub-tree, then it follows that that value is _not_ under the
        //     under value, but above it. On the other hand, if the value to be checked is not in
        //     the parent column of the sub-tree, then if condition (1) is also satisfied it follows
        //     that it _is_ under the under_value.
        //     Note that "under" is interpreted in the inclusive sense; i.e., values are trivially
        //     understood to be under themselves.
        let effective_table;
        if !extra_clause.is_empty() {
            effective_table = format!("{}_ext", table_name);
        } else {
            effective_table = table_name.clone();
        }

        let effective_tree;
        if tree_table == table_name {
            effective_tree = effective_table.to_string();
        } else {
            effective_tree = tree_table.to_string();
        }

        let uval = ukey.get("value").and_then(|v| v.as_str()).unwrap().to_string();
        let (tree_sql, mut tree_params) =
            with_tree_sql(&config, tree, &table_name, &effective_tree, Some(uval.clone()), None);
        // Add the tree params to the beginning of the parameter list:
        tree_params.append(&mut params);
        params = tree_params;

        // Remove the 'WITH' part of the extra clause since it is redundant given the tree sql and
        // will therefore result in a syntax error:
        if !extra_clause.is_empty() {
            extra_clause = format!(", {}", &extra_clause[5..]);
        }
        let sql = local_sql_syntax(
            &pool,
            &format!(
                r#"{} {}
                   SELECT
                    "row_number",
                    "{}"."{}",
                    CASE
                      WHEN "{}"."{}_meta" IS NOT NULL
                        THEN JSON("{}"."{}_meta")
                       ELSE JSON('{{"valid": true, "messages": []}}')
                    END AS "{}_meta",
                    CASE
                      WHEN "{}"."{}" IN (
                        SELECT "{}" FROM "{}"
                      )
                      THEN 1 ELSE 0
                    END AS "is_in_tree",
                    CASE
                      WHEN "{}"."{}" IN (
                        SELECT "{}" FROM "tree"
                      )
                      THEN 0 ELSE 1
                    END AS "is_under"
                  FROM "{}""#,
                tree_sql,
                extra_clause,
                effective_table,
                column,
                effective_table,
                column,
                effective_table,
                column,
                column,
                effective_table,
                column,
                tree_child,
                effective_tree,
                effective_table,
                column,
                tree_parent,
                effective_table,
            ),
        );

        let mut query = sqlx_query(&sql);
        for param in &params {
            query = query.bind(param);
        }
        let rows = query.fetch_all(pool).await?;
        for row in rows {
            let meta: &str = row.get_unchecked(format!(r#"{}_meta"#, column).as_str());
            let meta: SerdeValue = serde_json::from_str(meta).unwrap();
            let meta = meta.as_object().unwrap();
            // If the value in the parent column is legitimately empty, then just skip this row:
            if meta.contains_key("nulltype") {
                continue;
            }

            // If the value in the column already contains a different error, its value will be null
            // and it will be returned by the above query regardless of whether it is valid or
            // invalid. So we need to check the value from the meta column instead.
            let raw_column_val = row.try_get_raw(format!(r#"{}"#, column).as_str()).unwrap();
            let column_val: String;
            if raw_column_val.is_null() {
                column_val = meta.get("value").and_then(|v| v.as_str()).unwrap().to_string();
            } else {
                let sql_type =
                    get_sql_type_from_global_config(&config, &table_name, &column).unwrap();
                if sql_type.to_lowercase() == "integer" {
                    let foo: i32 = row.get(format!(r#"{}"#, column).as_str());
                    let foo: u32 = foo as u32;
                    column_val = foo.to_string();
                } else {
                    column_val = row.get(format!(r#"{}"#, column).as_str());
                }
            }

            // We use i32 instead of i64 (which we use for row_number) here because, unlike row_number,
            // which is a BIGINT, 0 and 1 are being interpreted as normal sized ints.
            let is_in_tree: i32 = row.get("is_in_tree");
            let is_in_tree: u32 = is_in_tree as u32;
            let is_under: i32 = row.get("is_under");
            let is_under: u32 = is_under as u32;
            if is_in_tree == 0 {
                let mut meta = meta.clone();
                meta.insert("valid".to_string(), SerdeValue::Bool(false));
                meta.insert("value".to_string(), SerdeValue::String(column_val.to_string()));
                let message = json!({
                    "rule": "under:not-in-tree",
                    "level": "error",
                    "message": format!("Value {} of column {} is not in {}.{}",
                                       column_val, column, tree_table, tree_child).as_str(),
                });
                meta.get_mut("messages")
                    .and_then(|m| m.as_array_mut())
                    .and_then(|a| Some(a.push(message)));

                let raw_row_number = row.try_get_raw("row_number").unwrap();
                let row_number: i64;
                if raw_row_number.is_null() {
                    row_number = 0;
                } else {
                    row_number = row.get("row_number");
                }

                let row_number = row_number as u32;
                let result = json!({
                    "row_number": row_number,
                    "column": column,
                    "meta": meta,
                });
                results.push(result);
            } else if is_under == 0 {
                let mut meta = meta.clone();
                meta.insert("valid".to_string(), SerdeValue::Bool(false));
                meta.insert("value".to_string(), SerdeValue::String(column_val.to_string()));
                let message = json!({
                    "rule": "under:not-under",
                    "level": "error",
                    "message": format!("Value '{}' of column {} is not under '{}'",
                                       column_val, column, uval.clone()).as_str(),
                });
                meta.get_mut("messages")
                    .and_then(|m| m.as_array_mut())
                    .and_then(|a| Some(a.push(message)));
                let row_number: i64 = row.get("row_number");
                let row_number = row_number as u32;
                let result = json!({
                    "row_number": row_number,
                    "column": column,
                    "meta": meta,
                });
                results.push(result);
            }
        }
    }

    Ok(results)
}

/// Given a config map, a db connection pool, and a table name, validate whether there is a
/// 'foreign key' violation for any of the table's trees; i.e., for a given tree: tree(child) which
/// has a given parent column, validate that all of the values in the parent column are in the child
/// column.
pub async fn validate_tree_foreign_keys(
    config: &ConfigMap,
    pool: &AnyPool,
    table_name: &String,
    extra_row: Option<ResultRow>,
) -> Result<Vec<SerdeValue>, sqlx::Error> {
    let tkeys = config
        .get("constraints")
        .and_then(|c| c.as_object())
        .and_then(|o| o.get("tree"))
        .and_then(|t| t.as_object())
        .and_then(|o| o.get(table_name))
        .and_then(|t| t.as_array())
        .unwrap();

    let mut results = vec![];
    for tkey in tkeys {
        let tkey = tkey.as_object().unwrap();
        let child_col = tkey.get("child").and_then(|c| c.as_str()).unwrap();
        let parent_col = tkey.get("parent").and_then(|p| p.as_str()).unwrap();
        let with_clause;
        let params;
        if let Some(ref extra_row) = extra_row {
            (with_clause, params) = select_with_extra_row(&config, extra_row, table_name);
        } else {
            with_clause = String::new();
            params = vec![];
        }

        let effective_table_name;
        if !with_clause.is_empty() {
            effective_table_name = format!("{}_ext", table_name);
        } else {
            effective_table_name = table_name.clone();
        }

        let sql = local_sql_syntax(
            &pool,
            &format!(
                r#"{}
                   SELECT
                     t1."row_number", t1."{}",
                     CASE
                       WHEN t1."{}_meta" IS NOT NULL
                         THEN JSON(t1."{}_meta")
                       ELSE JSON('{{"valid": true, "messages": []}}')
                     END AS "{}_meta"
                   FROM "{}" t1
                   WHERE NOT EXISTS (
                     SELECT 1
                     FROM "{}" t2
                     WHERE t2."{}" = t1."{}"
                   )"#,
                with_clause,
                parent_col,
                parent_col,
                parent_col,
                parent_col,
                effective_table_name,
                effective_table_name,
                child_col,
                parent_col
            ),
        );

        let mut query = sqlx_query(&sql);
        for param in &params {
            query = query.bind(param);
        }
        let rows = query.fetch_all(pool).await?;
        for row in rows {
            let meta: &str = row.get_unchecked(format!(r#"{}_meta"#, parent_col).as_str());
            let meta: SerdeValue = serde_json::from_str(meta).unwrap();
            let meta = meta.as_object().unwrap();
            // If the value in the parent column is legitimately empty, then just skip this row:
            if meta.contains_key("nulltype") {
                continue;
            }

            // If the parent column already contains a different error, its value will be null and
            // it will be returned by the above query regardless of whether it actually violates the
            // tree's foreign constraint. So we check the value from the meta column instead.
            let raw_parent_val = row.try_get_raw(format!(r#"{}"#, parent_col).as_str()).unwrap();
            let parent_val;
            if !raw_parent_val.is_null() {
                parent_val = row.get(format!(r#"{}"#, parent_col).as_str());
            } else {
                parent_val = meta.get("value").and_then(|v| v.as_str()).unwrap();
                let sql = local_sql_syntax(
                    &pool,
                    &format!(
                        r#"SELECT 1 FROM "{}" WHERE "{}" = {} LIMIT 1"#,
                        table_name, child_col, SQL_PARAM
                    ),
                );
                let query = sqlx_query(&sql).bind(parent_val);
                let rows = query.fetch_all(pool).await?;
                if rows.len() > 0 {
                    continue;
                }
            }

            let mut meta = meta.clone();
            meta.insert("valid".to_string(), SerdeValue::Bool(false));
            meta.insert("value".to_string(), SerdeValue::String(parent_val.to_string()));
            let message = json!({
                "rule": "tree:foreign",
                "level": "error",
                "message": format!("Value {} of column {} is not in column {}",
                                   parent_val, parent_col, child_col).as_str(),
            });
            meta.get_mut("messages")
                .and_then(|m| m.as_array_mut())
                .and_then(|a| Some(a.push(message)));

            let raw_row_number = row.try_get_raw("row_number").unwrap();
            let row_number: i64;
            if raw_row_number.is_null() {
                row_number = 0;
            } else {
                row_number = row.get("row_number");
            }

            let row_number = row_number as u32;
            let result = json!({
                "row_number": row_number,
                "column": parent_col,
                "meta": meta,
            });
            results.push(result);
        }
    }

    Ok(results)
}
