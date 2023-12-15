use chrono::Utc;
use indexmap::IndexMap;
use serde_json::{json, Value as SerdeValue};
use sqlx::{
    any::AnyPool, query as sqlx_query, Acquire, Error::Configuration as SqlxCErr, Row, Transaction,
    ValueRef,
};
use std::collections::HashMap;

use crate::{
    cast_sql_param_from_text, get_column_value, get_sql_type_from_global_config, is_sql_type_error,
    local_sql_syntax, valve_log, ColumnRule, CompiledCondition, SerdeMap, ValveRow,
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
    pub contents: IndexMap<String, ResultCell>,
}

/// The sense in which a [QueryAsIf] struct should be interpreted.
#[derive(Clone, Debug, PartialEq)]
pub enum QueryAsIfKind {
    Add,
    Remove,
    Replace,
}

/// Used for counterfactual validation.
#[derive(Clone, Debug)]
pub struct QueryAsIf {
    pub kind: QueryAsIfKind,
    pub table: String,
    // Although PostgreSQL allows it, SQLite does not allow a CTE named 'foo' to refer to a table
    // named 'foo' so we need to use an alias:
    pub alias: String,
    pub row_number: u32,
    pub row: Option<ValveRow>,
}

/// Given a config map, maps of compiled datatype and rule conditions, a database connection
/// pool, a table name, a row to validate and a row number in the case where the row already
/// exists, perform both intra- and inter-row validation and return the validated row.
/// Optionally, if a transaction is given, use that instead of the pool for database access.
/// Optionally, if query_as_if is given, validate the row counterfactually according to that
/// parameter. Note that this function is idempotent.
pub async fn validate_row_tx(
    config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
    tx: Option<&mut Transaction<'_, sqlx::Any>>,
    table_name: &str,
    row: &ValveRow,
    row_number: Option<u32>,
    query_as_if: Option<&QueryAsIf>,
) -> Result<ValveRow, sqlx::Error> {
    // Fallback to a default transaction if it is not given. Since we do not commit before it falls
    // out of scope the transaction will be rolled back at the end of this function. And since this
    // function is read-only the rollback is trivial and therefore inconsequential.
    let default_tx = &mut pool.begin().await?;
    let tx = match tx {
        Some(tx) => tx,
        None => default_tx,
    };

    // Initialize the result row with the values from the given row:
    let mut result_row = ResultRow {
        row_number: row_number,
        contents: IndexMap::new(),
    };

    for (column, cell) in row.iter() {
        let nulltype = match cell.get("nulltype") {
            None => None,
            Some(SerdeValue::String(s)) => Some(s.to_string()),
            _ => {
                return Err(SqlxCErr(
                    format!("No string 'nulltype' in cell: {:?}.", cell).into(),
                ))
            }
        };
        let value = match cell.get("value") {
            Some(SerdeValue::String(s)) => s.to_string(),
            Some(SerdeValue::Number(n)) => format!("{}", n),
            _ => {
                return Err(SqlxCErr(
                    format!("No string/number 'value' in cell: {:#?}.", cell).into(),
                ))
            }
        };
        let valid = match cell.get("valid").and_then(|v| v.as_bool()) {
            Some(b) => b,
            None => {
                return Err(SqlxCErr(
                    format!("No bool 'valid' in cell: {:?}.", cell).into(),
                ))
            }
        };
        let messages = match cell.get("messages").and_then(|m| m.as_array()) {
            Some(a) => a.to_vec(),
            None => {
                return Err(SqlxCErr(
                    format!("No array 'messages' in cell: {:?}.", cell).into(),
                ))
            }
        };
        let result_cell = ResultCell {
            nulltype: nulltype,
            value: value,
            valid: valid,
            messages: messages,
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

            // We don't do any further validation on cells that have SQL type violations because
            // they can result in database errors when, for instance, we compare a numeric with a
            // non-numeric type.
            let sql_type =
                get_sql_type_from_global_config(&config, table_name, &column_name, pool).unwrap();
            if !is_sql_type_error(&sql_type, &cell.value) {
                // TODO: Pass the query_as_if parameter to validate_cell_trees.
                validate_cell_trees(
                    config,
                    pool,
                    Some(tx),
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
                    Some(tx),
                    &table_name.to_string(),
                    column_name,
                    cell,
                    query_as_if,
                )
                .await?;
                validate_cell_unique_constraints(
                    config,
                    pool,
                    Some(tx),
                    &table_name.to_string(),
                    column_name,
                    cell,
                    &vec![],
                    row_number,
                )
                .await?;
            }
        }
    }

    // TODO: Possibly propagate `query_as_if` down into this function:
    let mut violations = validate_tree_foreign_keys(
        config,
        pool,
        Some(tx),
        &table_name.to_string(),
        Some(&context.clone()),
    )
    .await?;
    violations.append(
        // TODO: Possibly propagate `query_as_if` down into this function:
        &mut validate_under(
            config,
            pool,
            Some(tx),
            &table_name.to_string(),
            Some(&context.clone()),
        )
        .await?,
    );

    for violation in violations.iter_mut() {
        let vrow_number = violation.get("row_number").unwrap().as_i64().unwrap() as u32;
        if Some(vrow_number) == row_number || (row_number == None && Some(vrow_number) == Some(0)) {
            let column = violation.get("column").and_then(|s| s.as_str()).unwrap();
            let level = violation.get("level").and_then(|s| s.as_str()).unwrap();
            let rule = violation.get("rule").and_then(|s| s.as_str()).unwrap();
            let message = violation.get("message").and_then(|s| s.as_str()).unwrap();
            let result_cell = &mut result_row.contents.get_mut(column).unwrap();
            result_cell.messages.push(json!({
                "level": level,
                "rule": rule,
                "message": message,
            }));
            if result_cell.valid {
                result_cell.valid = false;
            }
        }
    }

    let result_row = remove_duplicate_messages(&result_row_to_config_map(&result_row))?;
    Ok(result_row)
}

/// Given a config map, a db connection pool, a table name, and an optional extra row, validate
/// any associated under constraints for the current column. Optionally, if a transaction is
/// given, use that instead of the pool for database access.
pub async fn validate_under(
    config: &SerdeMap,
    pool: &AnyPool,
    mut tx: Option<&mut Transaction<'_, sqlx::Any>>,
    table_name: &String,
    extra_row: Option<&ResultRow>,
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

    let view_name = format!("{}_view", table_name);
    for ukey in ukeys {
        let ukey = ukey.as_object().unwrap();
        let tree_table = ukey.get("ttable").and_then(|tt| tt.as_str()).unwrap();
        let tree_child = ukey.get("tcolumn").and_then(|tc| tc.as_str()).unwrap();
        let column = ukey.get("column").and_then(|c| c.as_str()).unwrap();
        let sql_type =
            get_sql_type_from_global_config(&config, &table_name, &column, pool).unwrap();
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
            (extra_clause, params) =
                select_with_extra_row(&config, extra_row, table_name, &view_name, pool);
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
            effective_table = format!("{}_ext", view_name);
        } else {
            effective_table = view_name.clone();
        }

        let effective_tree;
        if tree_table == table_name {
            effective_tree = effective_table.to_string();
        } else {
            effective_tree = tree_table.to_string();
        }

        let uval = ukey
            .get("value")
            .and_then(|v| v.as_str())
            .unwrap()
            .to_string();
        let (tree_sql, mut tree_params) = with_tree_sql(
            &config,
            tree,
            &table_name,
            &effective_tree,
            Some(&uval.clone()),
            None,
            pool,
        );
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
                r#"{tree_sql} {extra_clause}
                   SELECT
                    "row_number",
                    "{effective_table}"."{column}",
                    CASE
                      WHEN "{effective_table}"."{column}" IN (
                        SELECT "{tree_child}" FROM "{effective_tree}"
                      )
                      THEN 1 ELSE 0
                    END AS "is_in_tree",
                    CASE
                      WHEN "{effective_table}"."{column}" IN (
                        SELECT "{tree_parent}" FROM "tree"
                      )
                      THEN 0 ELSE 1
                    END AS "is_under"
                  FROM "{effective_table}""#,
                tree_sql = tree_sql,
                extra_clause = extra_clause,
                effective_table = effective_table,
                column = column,
                tree_child = tree_child,
                effective_tree = effective_tree,
                tree_parent = tree_parent,
            ),
        );

        let mut query = sqlx_query(&sql);
        for param in &params {
            query = query.bind(param);
        }
        let rows = {
            if let None = tx {
                query.fetch_all(pool).await?
            } else {
                query
                    .fetch_all(tx.as_mut().unwrap().acquire().await?)
                    .await?
            }
        };

        for row in rows {
            let raw_row_number = row.try_get_raw("row_number").unwrap();
            let row_number: i64;
            if raw_row_number.is_null() {
                row_number = 0;
            } else {
                row_number = row.get("row_number");
            }

            let raw_column_val = row.try_get_raw(format!(r#"{}"#, column).as_str()).unwrap();
            if !raw_column_val.is_null() {
                let column_val = get_column_value(&row, &column, &sql_type);
                // We use i32 instead of i64 (which we use for row_number) here because, unlike
                // row_number, which is a BIGINT, 0 and 1 are being interpreted as normal sized ints.
                let is_in_tree: i32 = row.get("is_in_tree");
                let is_under: i32 = row.get("is_under");
                if is_in_tree == 0 {
                    results.push(json!({
                        "row_number": row_number as u32,
                        "column": column,
                        "value": column_val,
                        "level": "error",
                        "rule": "under:not-in-tree",
                        "message": format!("Value '{}' of column {} is not in {}.{}",
                                           column_val, column, tree_table, tree_child).as_str(),
                    }));
                } else if is_under == 0 {
                    results.push(json!({
                        "row_number": row_number as u32,
                        "column": column,
                        "value": column_val,
                        "level": "error",
                        "rule": "under:not-under",
                        "message": format!("Value '{}' of column {} is not under '{}'",
                                           column_val, column, uval.clone()).as_str(),
                    }));
                }
            }
        }
    }

    Ok(results)
}

/// Given a config map, a db connection pool, and a table name, validate whether there is a
/// 'foreign key' violation for any of the table's trees; i.e., for a given tree: tree(child) which
/// has a given parent column, validate that all of the values in the parent column are in the child
/// column. Optionally, if a transaction is given, use that instead of the pool for database access.
pub async fn validate_tree_foreign_keys(
    config: &SerdeMap,
    pool: &AnyPool,
    mut tx: Option<&mut Transaction<'_, sqlx::Any>>,
    table_name: &String,
    extra_row: Option<&ResultRow>,
) -> Result<Vec<SerdeValue>, sqlx::Error> {
    let tkeys = config
        .get("constraints")
        .and_then(|c| c.as_object())
        .and_then(|o| o.get("tree"))
        .and_then(|t| t.as_object())
        .and_then(|o| o.get(table_name))
        .and_then(|t| t.as_array())
        .unwrap();

    let view_name = format!("{}_view", table_name);
    let mut results = vec![];
    for tkey in tkeys {
        let tkey = tkey.as_object().unwrap();
        let child_col = tkey.get("child").and_then(|c| c.as_str()).unwrap();
        let parent_col = tkey.get("parent").and_then(|p| p.as_str()).unwrap();
        let parent_sql_type =
            get_sql_type_from_global_config(&config, &table_name, &parent_col, pool).unwrap();
        let with_clause;
        let params;
        if let Some(ref extra_row) = extra_row {
            (with_clause, params) =
                select_with_extra_row(&config, extra_row, table_name, &view_name, pool);
        } else {
            with_clause = String::new();
            params = vec![];
        }

        let effective_table_name;
        if !with_clause.is_empty() {
            effective_table_name = format!("{}_ext", view_name);
        } else {
            effective_table_name = view_name.clone();
        }

        let sql = local_sql_syntax(
            &pool,
            &format!(
                r#"{with_clause}
                   SELECT
                     t1."row_number", t1."{parent_col}"
                   FROM "{effective_table_name}" t1
                   WHERE NOT EXISTS (
                     SELECT 1
                     FROM "{effective_table_name}" t2
                     WHERE t2."{child_col}" = t1."{parent_col}"
                   )"#,
                with_clause = with_clause,
                parent_col = parent_col,
                effective_table_name = effective_table_name,
                child_col = child_col,
            ),
        );

        let mut query = sqlx_query(&sql);
        for param in &params {
            query = query.bind(param);
        }
        let rows = {
            if let None = tx {
                query.fetch_all(pool).await?
            } else {
                query
                    .fetch_all(tx.as_mut().unwrap().acquire().await?)
                    .await?
            }
        };
        for row in rows {
            let raw_row_number = row.try_get_raw("row_number").unwrap();
            let row_number: i64;
            if raw_row_number.is_null() {
                row_number = 0;
            } else {
                row_number = row.get("row_number");
            }
            let raw_parent_val = row
                .try_get_raw(format!(r#"{}"#, parent_col).as_str())
                .unwrap();
            if !raw_parent_val.is_null() {
                let parent_val = get_column_value(&row, &parent_col, &parent_sql_type);
                results.push(json!({
                    "row_number": row_number as u32,
                    "column": parent_col,
                    "value": parent_val,
                    "level": "error",
                    "rule": "tree:foreign",
                    "message": format!("Value '{}' of column {} is not in column {}",
                                       parent_val, parent_col, child_col).as_str(),
                }));
            }
        }
    }

    Ok(results)
}

/// Given a config map, a database connection pool, a table name, and a number of rows to validate,
/// perform tree validation on the rows and return the validated results.
pub async fn validate_rows_trees(
    config: &SerdeMap,
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
        let mut result_row = ResultRow {
            row_number: None,
            contents: IndexMap::new(),
        };
        for column_name in &column_names {
            let context = row.clone();
            let cell = row.contents.get_mut(column_name).unwrap();
            // We don't do any further validation on cells that are legitimately empty, or on cells
            // that have SQL type violations. We exclude the latter because they can result in
            // database errors when, for instance, we compare a numeric with a non-numeric type.
            let sql_type =
                get_sql_type_from_global_config(&config, table_name, &column_name, pool).unwrap();
            if cell.nulltype == None && !is_sql_type_error(&sql_type, &cell.value) {
                validate_cell_trees(
                    config,
                    pool,
                    None,
                    table_name,
                    &column_name,
                    cell,
                    &context,
                    &result_rows,
                )
                .await?;
            }
            result_row
                .contents
                .insert(column_name.to_string(), cell.clone());
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
    config: &SerdeMap,
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
        let mut result_row = ResultRow {
            row_number: None,
            contents: IndexMap::new(),
        };
        for column_name in &column_names {
            let cell = row.contents.get_mut(column_name).unwrap();
            // We don't do any further validation on cells that are legitimately empty, or on cells
            // that have SQL type violations. We exclude the latter because they can result in
            // database errors when, for instance, we compare a numeric with a non-numeric type.
            let sql_type =
                get_sql_type_from_global_config(&config, table_name, &column_name, pool).unwrap();
            if cell.nulltype == None && !is_sql_type_error(&sql_type, &cell.value) {
                validate_cell_foreign_constraints(
                    config,
                    pool,
                    None,
                    table_name,
                    &column_name,
                    cell,
                    None,
                )
                .await?;

                validate_cell_unique_constraints(
                    config,
                    pool,
                    None,
                    table_name,
                    &column_name,
                    cell,
                    &result_rows,
                    None,
                )
                .await?;
            }
            result_row
                .contents
                .insert(column_name.to_string(), cell.clone());
        }
        // Note that in this implementation, the result rows are never actually returned, but we
        // still need them because the validate_cell_unique_constraints() function needs a list of
        // previous results, and this then requires that we generate the result rows to play that
        // role. The call to cell.clone() above is required to make rust's borrow checker happy.
        result_rows.push(result_row);
    }

    Ok(())
}

/// Given a config map, compiled datatype and rule conditions, a table name, the headers for the
/// table, and a number of rows to validate, validate all of the rows and return the validated
/// versions.
pub fn validate_rows_intra(
    config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    table_name: &String,
    headers: &csv::StringRecord,
    rows: &Vec<Result<csv::StringRecord, csv::Error>>,
) -> Vec<ResultRow> {
    let mut result_rows = vec![];
    for row in rows {
        match row {
            Err(err) => valve_log!("ERROR while processing row for '{}': {}", table_name, err),
            Ok(row) => {
                let mut result_row = ResultRow {
                    row_number: None,
                    contents: IndexMap::new(),
                };
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

/// Given a row represented as a ValveRow, remove any duplicate messages from the row's cells, so
/// that no cell has messages with the same level, rule, and message text.
fn remove_duplicate_messages(row: &ValveRow) -> Result<ValveRow, sqlx::Error> {
    let mut deduped_row = ValveRow::new();
    for (column_name, cell) in row.iter() {
        let mut messages = cell
            .get("messages")
            .and_then(|m| m.as_array())
            .unwrap_or(&vec![])
            .clone();
        messages.sort_by(|a, b| {
            let a = format!(
                "{}{}{}",
                a.get("level").unwrap(),
                a.get("rule").unwrap(),
                a.get("message").unwrap()
            );
            let b = format!(
                "{}{}{}",
                b.get("level").unwrap(),
                b.get("rule").unwrap(),
                b.get("message").unwrap()
            );
            a.partial_cmp(&b).unwrap()
        });
        messages.dedup_by(|a, b| {
            a.get("level").unwrap() == b.get("level").unwrap()
                && a.get("rule").unwrap() == b.get("rule").unwrap()
                && a.get("message").unwrap() == b.get("message").unwrap()
        });
        let mut cell = cell.as_object().unwrap().clone();
        cell.insert("messages".to_string(), json!(messages));
        deduped_row.insert(column_name.to_string(), json!(cell));
    }
    Ok(deduped_row)
}

/// Given a result row, convert it to a ValveRow and return it.
/// Note that if the incoming result row has an associated row_number, this is ignored.
fn result_row_to_config_map(incoming: &ResultRow) -> ValveRow {
    let mut outgoing = ValveRow::new();
    for (column, cell) in incoming.contents.iter() {
        let mut cell_map = ValveRow::new();
        if let Some(nulltype) = &cell.nulltype {
            cell_map.insert(
                "nulltype".to_string(),
                SerdeValue::String(nulltype.to_string()),
            );
        }
        cell_map.insert(
            "value".to_string(),
            SerdeValue::String(cell.value.to_string()),
        );
        cell_map.insert("valid".to_string(), SerdeValue::Bool(cell.valid));
        cell_map.insert(
            "messages".to_string(),
            SerdeValue::Array(cell.messages.clone()),
        );
        outgoing.insert(column.to_string(), SerdeValue::Object(cell_map));
    }
    outgoing
}

/// Generate a SQL Select clause that is a union of: (a) the literal values of the given extra row,
/// and (b) a Select statement over `table_name` of all the fields in the extra row.
fn select_with_extra_row(
    config: &SerdeMap,
    extra_row: &ResultRow,
    table: &str,
    effective_table: &str,
    pool: &AnyPool,
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
        let sql_type = get_sql_type_from_global_config(&config, &table, &key, pool).unwrap();
        let sql_param = cast_sql_param_from_text(&sql_type);
        // enumerate() begins from 0 but we need to begin at 1:
        let i = i + 1;
        first_select.push_str(format!(r#"{} AS "{}""#, sql_param, key).as_str());
        params.push(content.value.to_string());
        second_select.push_str(format!(r#""{}""#, key).as_str());
        if i < extra_row_len {
            first_select.push_str(", ");
            second_select.push_str(", ");
        } else {
            second_select.push_str(format!(r#" FROM "{}""#, effective_table).as_str());
        }
    }

    if let Some(rn) = extra_row.row_number {
        second_select.push_str(format!(r#" WHERE "row_number" <> {}"#, rn).as_str());
    }

    (
        format!(
            r#"WITH "{}_ext" AS ({} UNION ALL {})"#,
            effective_table, first_select, second_select
        ),
        params,
    )
}

/// Given a map representing a tree constraint, a table name, a root from which to generate a
/// sub-tree of the tree, and an extra SQL clause, generate the SQL for a WITH clause representing
/// the sub-tree.
pub fn with_tree_sql(
    config: &SerdeMap,
    tree: &SerdeMap,
    table_name: &str,
    effective_table_name: &str,
    root: Option<&String>,
    extra_clause: Option<&String>,
    pool: &AnyPool,
) -> (String, Vec<String>) {
    let empty_string = String::new();
    let extra_clause = extra_clause.unwrap_or_else(|| &empty_string);
    let child_col = tree.get("child").and_then(|c| c.as_str()).unwrap();
    let parent_col = tree.get("parent").and_then(|c| c.as_str()).unwrap();

    let mut params = vec![];
    let under_sql;
    if let Some(root) = root {
        let sql_type =
            get_sql_type_from_global_config(&config, table_name, &child_col, pool).unwrap();
        under_sql = format!(
            r#"WHERE "{}" = {}"#,
            child_col,
            cast_sql_param_from_text(&sql_type)
        );
        params.push(root.clone());
    } else {
        under_sql = String::new();
    }

    let sql = format!(
        r#"WITH RECURSIVE "tree" AS (
           {extra_clause}
               SELECT "{child_col}", "{parent_col}" 
                   FROM "{effective_table_name}" 
                   {under_sql} 
                   UNION ALL 
               SELECT "t1"."{child_col}", "t1"."{parent_col}" 
                   FROM "{effective_table_name}" AS "t1" 
                   JOIN "tree" AS "t2" ON "t2"."{parent_col}" = "t1"."{child_col}"
           )"#,
        extra_clause = extra_clause,
        child_col = child_col,
        parent_col = parent_col,
        effective_table_name = effective_table_name,
        under_sql = under_sql,
    );

    (sql, params)
}

/// Given a config map, compiled datatype conditions, a table name, a column name, and a cell to
/// validate, validate the cell's nulltype condition. If the cell's value is one of the allowable
/// nulltype values for this column, then fill in the cell's nulltype value before returning the
/// cell.
fn validate_cell_nulltype(
    config: &SerdeMap,
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

/// Given a config map, compiled datatype conditions, a table name, a column name, and a cell to
/// validate, validate the cell's datatype and return the validated cell.
fn validate_cell_datatype(
    config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    table_name: &String,
    column_name: &String,
    cell: &mut ResultCell,
) {
    fn get_datatypes_to_check(
        config: &SerdeMap,
        compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
        primary_dt_name: &str,
        dt_name: Option<&String>,
    ) -> Vec<SerdeMap> {
        let mut datatypes = vec![];
        if let Some(dt_name) = dt_name {
            let datatype = config
                .get("datatype")
                .and_then(|d| d.as_object())
                .and_then(|o| o.get(dt_name))
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
                dt_parent.as_ref(),
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
    let primary_dt_description = primary_datatype
        .get("description")
        .and_then(|d| d.as_str())
        .unwrap();
    if let Some(primary_dt_condition_func) = compiled_datatype_conditions.get(primary_dt_name) {
        let primary_dt_condition_func = &primary_dt_condition_func.compiled;
        if !primary_dt_condition_func(&cell.value) {
            cell.valid = false;
            let mut parent_datatypes = get_datatypes_to_check(
                config,
                compiled_datatype_conditions,
                primary_dt_name,
                Some(&primary_dt_name.to_string()),
            );
            // If this datatype has any parents, check them beginning from the most general to the
            // most specific. We use while and pop instead of a for loop so as to check the
            // conditions in LIFO order.
            while !parent_datatypes.is_empty() {
                let datatype = parent_datatypes.pop().unwrap();
                let dt_name = datatype.get("datatype").and_then(|d| d.as_str()).unwrap();
                let dt_description = datatype
                    .get("description")
                    .and_then(|d| d.as_str())
                    .unwrap();
                let dt_condition = &compiled_datatype_conditions.get(dt_name).unwrap().compiled;
                if !dt_condition(&cell.value) {
                    let message = if dt_description == "" {
                        format!("{} should be of datatype {}", column_name, dt_name)
                    } else {
                        format!("{} should be {}", column_name, dt_description)
                    };
                    let message_info = json!({
                        "rule": format!("datatype:{}", dt_name),
                        "level": "error",
                        "message": message,
                    });
                    cell.messages.push(message_info);
                }
            }

            let message = if primary_dt_description == "" {
                format!("{} should be of datatype {}", column_name, primary_dt_name)
            } else {
                format!("{} should be {}", column_name, primary_dt_description)
            };
            let message_info = json!({
                "rule": format!("datatype:{}", primary_dt_name),
                "level": "error",
                "message": message,
            });
            cell.messages.push(message_info);
        }
    }
}

/// Given a config map, compiled rule conditions, a table name, a column name, the row context,
/// and the cell to validate, look in the rule table (if it exists) and validate the cell according
/// to any applicable rules.
fn validate_cell_rules(
    config: &SerdeMap,
    compiled_rules: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    table_name: &String,
    column_name: &String,
    context: &ResultRow,
    cell: &mut ResultCell,
) {
    fn check_condition(
        condition_type: &str,
        cell: &ResultCell,
        rule: &SerdeMap,
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
            if !check_condition(
                "then",
                then_cell,
                rule,
                table_name,
                column_name,
                compiled_rules,
            ) {
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

/// Generates an SQL fragment representing the "as if" portion of a query that will be used for
/// counterfactual validation.
fn as_if_to_sql(
    global_config: &SerdeMap,
    pool: &AnyPool,
    as_if: &QueryAsIf,
    conflict_table: bool,
) -> String {
    let sql = {
        let suffix = {
            if conflict_table {
                "_conflict"
            } else {
                ""
            }
        };

        match as_if.kind {
            QueryAsIfKind::Remove => {
                format!(
                    r#""{table_alias}{suffix}" AS (
                       SELECT * FROM "{table_name}{suffix}" WHERE "row_number" <> {row_number}
                    )"#,
                    table_alias = as_if.alias,
                    suffix = suffix,
                    table_name = as_if.table,
                    row_number = as_if.row_number,
                )
            }
            QueryAsIfKind::Add | QueryAsIfKind::Replace => {
                let row = as_if.row.as_ref().unwrap();
                let columns = row.keys().cloned().collect::<Vec<_>>();
                let values = {
                    let mut values = vec![];
                    for column in &columns {
                        let valid = row
                            .get(column)
                            .and_then(|c| c.get("valid"))
                            .and_then(|v| v.as_bool())
                            .unwrap();

                        let value = {
                            if valid == true {
                                let value = match row.get(column).and_then(|c| c.get("value")) {
                                    Some(SerdeValue::String(s)) => Ok(format!("{}", s)),
                                    Some(SerdeValue::Number(n)) => Ok(format!("{}", n)),
                                    Some(SerdeValue::Bool(b)) => Ok(format!("{}", b)),
                                    _ => Err(format!(
                                        "Value missing or of unknown type in column {} of row to \
                                         update: {:?}",
                                        column, row
                                    )),
                                }
                                .unwrap();
                                if value == "" {
                                    "NULL".to_string()
                                } else {
                                    value
                                }
                            } else {
                                "NULL".to_string()
                            }
                        };

                        let sql_type = get_sql_type_from_global_config(
                            &global_config,
                            &as_if.table,
                            &column,
                            pool,
                        )
                        .unwrap();

                        if sql_type.to_lowercase() == "text"
                            || sql_type.to_lowercase().starts_with("varchar(")
                        {
                            values.push(format!("'{}'", value));
                        } else {
                            values.push(value.to_string());
                        }
                    }
                    values.join(", ")
                };
                let columns = columns
                    .iter()
                    .map(|c| format!("\"{}\"", c))
                    .collect::<Vec<_>>()
                    .join(", ");
                let where_clause = {
                    if as_if.kind == QueryAsIfKind::Replace {
                        format!(r#"WHERE "row_number" != {}"#, as_if.row_number)
                    } else {
                        "".to_string()
                    }
                };
                format!(
                    r#""{table_alias}{suffix}" AS (
                   SELECT "row_number", {columns}
                   FROM "{table_name}{suffix}"
                   {where_clause}
                   UNION ALL
                   SELECT {row_number}, {values}
                )"#,
                    columns = columns,
                    table_alias = as_if.alias,
                    suffix = suffix,
                    table_name = as_if.table,
                    row_number = as_if.row_number,
                    values = values,
                    where_clause = where_clause,
                )
            }
        }
    };

    sql
}

/// Given a config map, a db connection pool, a table name, a column name, and a cell to validate,
/// check the cell value against any foreign keys that have been defined for the column. If there is
/// a violation, indicate it with an error message attached to the cell. Optionally, if a
/// transaction is given, use that instead of the pool for database access.
async fn validate_cell_foreign_constraints(
    config: &SerdeMap,
    pool: &AnyPool,
    mut tx: Option<&mut Transaction<'_, sqlx::Any>>,
    table_name: &String,
    column_name: &String,
    cell: &mut ResultCell,
    query_as_if: Option<&QueryAsIf>,
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

    let as_if_clause = match query_as_if {
        Some(query_as_if) => {
            format!("WITH {} ", as_if_to_sql(config, pool, &query_as_if, false))
        }
        None => "".to_string(),
    };
    let as_if_clause_for_conflict = match query_as_if {
        Some(query_as_if) => {
            format!("WITH {} ", as_if_to_sql(config, pool, &query_as_if, true))
        }
        None => "".to_string(),
    };

    for fkey in fkeys {
        let ftable = fkey.get("ftable").and_then(|t| t.as_str()).unwrap();
        let (as_if_clause, ftable_alias) = match query_as_if {
            Some(query_as_if) if ftable == query_as_if.table => {
                (as_if_clause.to_string(), query_as_if.alias.to_string())
            }
            _ => ("".to_string(), ftable.to_string()),
        };
        let fcolumn = fkey.get("fcolumn").and_then(|c| c.as_str()).unwrap();
        let sql_type = get_sql_type_from_global_config(&config, &ftable, &fcolumn, pool).unwrap();
        let sql_param = cast_sql_param_from_text(&sql_type);
        let fsql = local_sql_syntax(
            &pool,
            &format!(
                r#"{}SELECT 1 FROM "{}" WHERE "{}" = {} LIMIT 1"#,
                as_if_clause, ftable_alias, fcolumn, sql_param
            ),
        );

        let frows = {
            if let None = tx {
                sqlx_query(&fsql).bind(&cell.value).fetch_all(pool).await?
            } else {
                sqlx_query(&fsql)
                    .bind(&cell.value)
                    .fetch_all(tx.as_mut().unwrap().acquire().await?)
                    .await?
            }
        };
        if frows.is_empty() {
            cell.valid = false;
            let mut message = json!({
                "rule": "key:foreign",
                "level": "error",
            });

            let (as_if_clause_for_conflict, ftable_alias) = match query_as_if {
                Some(query_as_if) if ftable == query_as_if.table => (
                    as_if_clause_for_conflict.to_string(),
                    query_as_if.alias.to_string(),
                ),
                _ => ("".to_string(), ftable.to_string()),
            };

            let fsql = local_sql_syntax(
                &pool,
                &format!(
                    r#"{}SELECT 1 FROM "{}_conflict" WHERE "{}" = {} LIMIT 1"#,
                    as_if_clause_for_conflict, ftable_alias, fcolumn, sql_param
                ),
            );
            let frows = {
                if let None = tx {
                    sqlx_query(&fsql)
                        .bind(cell.value.clone())
                        .fetch_all(pool)
                        .await?
                } else {
                    sqlx_query(&fsql)
                        .bind(cell.value.clone())
                        .fetch_all(tx.as_mut().unwrap().acquire().await?)
                        .await?
                }
            };

            if frows.is_empty() {
                message.as_object_mut().and_then(|m| {
                    m.insert(
                        "message".to_string(),
                        SerdeValue::String(format!(
                            "Value '{}' of column {} is not in {}.{}",
                            cell.value, column_name, ftable, fcolumn
                        )),
                    )
                });
            } else {
                message.as_object_mut().and_then(|m| {
                    m.insert(
                        "message".to_string(),
                        SerdeValue::String(format!(
                            "Value '{}' of column {} exists only in {}_conflict.{}",
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
/// violations by attaching error messages to the cell. Optionally, if a transaction is
/// given, use that instead of the pool for database access.
async fn validate_cell_trees(
    config: &SerdeMap,
    pool: &AnyPool,
    mut tx: Option<&mut Transaction<'_, sqlx::Any>>,
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

    // If there are no tree keys, just silently return so as to save the cost of finding the
    // parent sql type etc, which will add up if we have to do it for every cell of every row.
    if tkeys.is_empty() {
        return Ok(());
    }

    let parent_col = column_name;
    let parent_sql_type =
        get_sql_type_from_global_config(&config, &table_name, &parent_col, pool).unwrap();
    let parent_sql_param = cast_sql_param_from_text(&parent_sql_type);
    let parent_val = cell.value.clone();
    let view_name = format!("{}_view", table_name);
    for tkey in tkeys {
        let child_col = tkey.get("child").and_then(|c| c.as_str()).unwrap();
        let child_sql_type =
            get_sql_type_from_global_config(&config, &table_name, &child_col, pool).unwrap();
        let child_sql_param = cast_sql_param_from_text(&child_sql_type);
        let child_val = context
            .contents
            .get(child_col)
            .and_then(|c| Some(c.value.clone()))
            .unwrap();

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
                format!(
                    r#"SELECT {} AS "{}", {} AS "{}""#,
                    child_sql_param, child_col, parent_sql_param, parent_col
                )
            })
            .collect::<Vec<_>>();
        let prev_selects = prev_selects.join(" UNION ALL ");

        let table_name_ext;
        let extra_clause;
        if prev_selects.is_empty() {
            table_name_ext = view_name.clone();
            extra_clause = String::from("");
        } else {
            table_name_ext = format!("{}_ext", view_name);
            extra_clause = format!(
                r#"WITH "{}" AS (
                       SELECT "{}", "{}"
                           FROM "{}"
                           UNION ALL
                       {}
                   )"#,
                table_name_ext, child_col, parent_col, view_name, prev_selects
            );
        }

        let (tree_sql, mut tree_sql_params) = with_tree_sql(
            &config,
            &tkey,
            &table_name,
            &table_name_ext,
            Some(&parent_val.clone()),
            Some(&extra_clause),
            pool,
        );
        params.append(&mut tree_sql_params);
        let sql = local_sql_syntax(
            &pool,
            &format!(
                r#"{} SELECT "{}", "{}" FROM "tree""#,
                tree_sql, child_col, parent_col
            ),
        );

        let mut query = sqlx_query(&sql);
        for param in &params {
            query = query.bind(param);
        }

        let rows = {
            if let None = tx {
                query.fetch_all(pool).await?
            } else {
                query
                    .fetch_all(tx.as_mut().unwrap().acquire().await?)
                    .await?
            }
        };

        // If there is a row in the tree whose parent is the to-be-inserted child, then inserting
        // the new row would result in a cycle.
        let cycle_detected = {
            let cycle_row = rows.iter().find(|row| {
                let raw_foo = row
                    .try_get_raw(format!(r#"{}"#, parent_col).as_str())
                    .unwrap();
                if raw_foo.is_null() {
                    false
                } else {
                    let parent = get_column_value(&row, &parent_col, &parent_sql_type);
                    parent == child_val
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
                let child = get_column_value(&row, &child_col, &child_sql_type);
                let parent = get_column_value(&row, &parent_col, &parent_sql_type);
                cycle_legs.push((child, parent));
            }
            cycle_legs.push((child_val, parent_val.clone()));

            let mut cycle_msg = vec![];
            for cycle in &cycle_legs {
                cycle_msg.push(format!(
                    "({}: {}, {}: {})",
                    child_col, cycle.0, parent_col, cycle.1
                ));
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

/// Given a config map, a db connection pool, a table name, a column name, a cell to validate,
/// the row, `context`, to which the cell belongs, and a list of previously validated rows,
/// check the cell value against any unique-type keys that have been defined for the column.
/// If there is a violation, indicate it with an error message attached to the cell. If
/// `row_number` is set to None, then no row corresponding to the given cell is assumed to exist
/// in the table. Optionally, if a transaction is given, use that instead of the pool for database
/// access.
async fn validate_cell_unique_constraints(
    config: &SerdeMap,
    pool: &AnyPool,
    mut tx: Option<&mut Transaction<'_, sqlx::Any>>,
    table_name: &String,
    column_name: &String,
    cell: &mut ResultCell,
    prev_results: &Vec<ResultRow>,
    row_number: Option<u32>,
) -> Result<(), sqlx::Error> {
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
        .and_then(|a| {
            Some(
                a.iter()
                    .map(|o| o.as_object().and_then(|o| o.get("child")).unwrap()),
            )
        })
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
        let view_name = format!("{}_view", table_name);
        let mut with_sql = String::new();
        let except_table = format!("{}_exc", view_name);
        if let Some(row_number) = row_number {
            with_sql = format!(
                r#"WITH "{}" AS (
                       SELECT * FROM "{}"
                       WHERE "row_number" != {}
                   ) "#,
                except_table, view_name, row_number
            );
        }

        let query_table;
        if !with_sql.is_empty() {
            query_table = except_table;
        } else {
            query_table = view_name.to_string();
        }

        let sql_type =
            get_sql_type_from_global_config(&config, &table_name, &column_name, pool).unwrap();
        let sql_param = cast_sql_param_from_text(&sql_type);
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

        if contained_in_prev_results
            || !{
                if let None = tx {
                    query.fetch_all(pool).await?.is_empty()
                } else {
                    query
                        .fetch_all(tx.as_mut().unwrap().acquire().await?)
                        .await?
                        .is_empty()
                }
            }
        {
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
