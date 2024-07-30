//! Low-level validation functions

use crate::{
    ast::Expression,
    toolkit::{
        cast_sql_param_from_text, get_column_value_as_string, get_datatype_ancestors,
        get_sql_type_from_global_config, get_table_options, is_sql_type_error, local_sql_syntax,
        ColumnRule, CompiledCondition, QueryAsIf, QueryAsIfKind, ValueType,
    },
    valve::{
        ValveCell, ValveCellMessage, ValveConfig, ValveRow, ValveRuleConfig, ValveTreeConstraint,
    },
    DT_CACHE_SIZE, FKEY_CACHE_SIZE,
};
use anyhow::Result;
use indexmap::IndexMap;
use lfu_cache::LfuCache;
use serde_json::{json, Value as SerdeValue};
use sqlx::{any::AnyPool, query as sqlx_query, Acquire, Row, Transaction, ValueRef};
use std::collections::HashMap;

/// Given a config struct, maps of compiled datatype and rule conditions, a database connection
/// pool, a table name, a row to validate represented as a [ValveRow], and a row number in the case
/// where the row already exists, perform both intra- and inter-row validation and return the
/// validated row. Optionally, if a transaction is given, use that instead of the pool for database
/// access. Optionally, if query_as_if is given, validate the row counterfactually according to that
/// parameter. Note that this function is idempotent.
pub async fn validate_row_tx(
    config: &ValveConfig,
    datatype_conditions: &HashMap<String, CompiledCondition>,
    rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
    tx: Option<&mut Transaction<'_, sqlx::Any>>,
    table_name: &str,
    row: &ValveRow,
    query_as_if: Option<&QueryAsIf>,
) -> Result<ValveRow> {
    // Fallback to a default transaction if it is not given. Since we do not commit before it falls
    // out of scope the transaction will be rolled back at the end of this function. And since this
    // function is read-only the rollback is trivial and therefore inconsequential.
    let default_tx = &mut pool.begin().await?;
    let tx = match tx {
        Some(tx) => tx,
        None => default_tx,
    };

    // Store the row number in a separate local variable for convenience:
    let row_number = row.row_number;

    // Initialize the result row with the values from the given row:
    let mut valve_row = row.clone();

    // We check all the cells for nulltype first, since the rules validation requires that we
    // have this information for all cells.
    for (column_name, cell) in valve_row.contents.iter_mut() {
        validate_cell_nulltype(
            config,
            datatype_conditions,
            &table_name.to_string(),
            column_name,
            cell,
        );
    }

    let context = valve_row.clone();
    for (column_name, cell) in valve_row.contents.iter_mut() {
        validate_cell_rules(
            config,
            rule_conditions,
            &table_name.to_string(),
            column_name,
            &context,
            cell,
        );

        if cell.nulltype == None {
            validate_cell_datatype(
                config,
                datatype_conditions,
                &table_name.to_string(),
                column_name,
                cell,
            );

            // We don't do any further validation on cells that have SQL type violations because
            // they can result in database errors when, for instance, we compare a numeric with a
            // non-numeric type.
            let sql_type = get_sql_type_from_global_config(&config, table_name, &column_name, pool);
            if !is_sql_type_error(&sql_type, &cell.strvalue()) {
                validate_cell_foreign_constraints(
                    config,
                    pool,
                    Some(tx),
                    datatype_conditions,
                    &table_name.to_string(),
                    column_name,
                    cell,
                    query_as_if,
                    None,
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
    for violation in violations.iter_mut() {
        let vrow_number = violation.get("row_number").unwrap().as_i64().unwrap() as u32;
        if Some(vrow_number) == row_number || (row_number == None && Some(vrow_number) == Some(0)) {
            let column = violation.get("column").and_then(|s| s.as_str()).unwrap();
            let level = violation.get("level").and_then(|s| s.as_str()).unwrap();
            let rule = violation.get("rule").and_then(|s| s.as_str()).unwrap();
            let message = violation.get("message").and_then(|s| s.as_str()).unwrap();
            let result_cell = &mut valve_row.contents.get_mut(column).unwrap();
            result_cell.messages.push(ValveCellMessage {
                level: level.to_string(),
                rule: rule.to_string(),
                message: message.to_string(),
            });
            if result_cell.valid {
                result_cell.valid = false;
            }
        }
    }

    remove_duplicate_messages(&mut valve_row)?;
    Ok(valve_row)
}

/// Given a config map, a db connection pool, a table name, and an optional extra row, validate
/// any associated under constraints for the current column. Optionally, if a transaction is
/// given, use that instead of the pool for database access.
pub async fn validate_under(
    config: &ValveConfig,
    pool: &AnyPool,
    mut tx: Option<&mut Transaction<'_, sqlx::Any>>,
    table_name: &String,
    extra_row: Option<&ValveRow>,
) -> Result<Vec<SerdeValue>> {
    let mut results = vec![];
    let ukeys = config
        .constraint
        .under
        .get(table_name)
        .expect(&format!("Undefined table '{}'", table_name));

    let table_options = get_table_options(config, table_name)?;
    let query_table = {
        if !table_options.contains("conflict") {
            table_name.to_string()
        } else {
            format!("{}_view", table_name)
        }
    };
    for ukey in ukeys {
        let tree_table = &ukey.ttable;
        let tree_child = &ukey.tcolumn;
        let column = &ukey.column;
        let sql_type = get_sql_type_from_global_config(&config, &table_name, &column, pool);
        let tree = config
            .constraint
            .tree
            .get(tree_table)
            .unwrap()
            .iter()
            .find(|tkey| tkey.child == *tree_child)
            .unwrap();
        let tree_parent = &tree.parent;
        let mut extra_clause;
        let mut params;
        if let Some(ref extra_row) = extra_row {
            (extra_clause, params) =
                select_with_extra_row(config, extra_row, table_name, &query_table, pool);
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
            effective_table = format!("{}_ext", query_table);
        } else {
            effective_table = query_table.clone();
        }

        let effective_tree;
        if tree_table == table_name {
            effective_tree = effective_table.to_string();
        } else {
            effective_tree = tree_table.to_string();
        }

        let uval = ukey
            .value
            .as_str()
            .and_then(|v| Some(v.to_string()))
            .unwrap();
        let (tree_sql, mut tree_params) = with_tree_sql(
            config,
            &tree,
            &table_name,
            &effective_tree,
            Some(&uval),
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
                let column_val = get_column_value_as_string(&row, &column, &sql_type);
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
    config: &ValveConfig,
    pool: &AnyPool,
    mut tx: Option<&mut Transaction<'_, sqlx::Any>>,
    table_name: &String,
    extra_row: Option<&ValveRow>,
) -> Result<Vec<SerdeValue>> {
    let tkeys = config
        .constraint
        .tree
        .get(table_name)
        .expect(&format!("Undefined table '{}'", table_name));

    let table_options = get_table_options(config, table_name)?;
    let query_table = {
        if !table_options.contains("conflict") {
            table_name.to_string()
        } else {
            format!("{}_view", table_name)
        }
    };
    let mut results = vec![];
    for tkey in tkeys {
        let child_col = &tkey.child;
        let parent_col = &tkey.parent;
        let parent_sql_type =
            get_sql_type_from_global_config(&config, &table_name, &parent_col, pool);
        let with_clause;
        let params;
        if let Some(ref extra_row) = extra_row {
            (with_clause, params) =
                select_with_extra_row(&config, extra_row, table_name, &query_table, pool);
        } else {
            with_clause = String::new();
            params = vec![];
        }

        let effective_table_name;
        if !with_clause.is_empty() {
            effective_table_name = format!("{}_ext", query_table);
        } else {
            effective_table_name = query_table.clone();
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
                let parent_val = get_column_value_as_string(&row, &parent_col, &parent_sql_type);
                let message = json!({
                    "row_number": row_number as u32,
                    "column": parent_col,
                    "value": parent_val,
                    "level": "error",
                    "rule": "tree:foreign",
                    "message": format!("Value '{}' of column {} is not in column {}",
                                       parent_val, parent_col, child_col).as_str(),
                });
                results.push(message);
            }
        }
    }

    Ok(results)
}

/// Given a config map, a database connection pool, a table name, and a number of rows to validate,
/// validate foreign and unique constraints, where the latter include primary and "tree child" keys
/// (which imply unique constraints) and return the validated results.
pub async fn validate_rows_constraints(
    config: &ValveConfig,
    pool: &AnyPool,
    datatype_conditions: &HashMap<String, CompiledCondition>,
    table_name: &String,
    rows: &mut Vec<ValveRow>,
) -> Result<()> {
    let column_names = &config
        .table
        .get(table_name)
        .expect(&format!("Undefined table '{}'", table_name))
        .column_order;

    let mut fkey_caches = match FKEY_CACHE_SIZE {
        0 => None,
        _ => {
            let mut cache = HashMap::new();
            for column in column_names {
                cache.insert(column.to_string(), LfuCache::with_capacity(FKEY_CACHE_SIZE));
            }
            Some(cache)
        }
    };

    let mut valve_rows = vec![];
    for row in rows.iter_mut() {
        let mut valve_row = ValveRow {
            row_number: None,
            contents: IndexMap::new(),
        };
        for column_name in column_names {
            let cell = row.contents.get_mut(column_name).unwrap();
            // We don't do any further validation on cells that are legitimately empty, or on cells
            // that have SQL type violations. We exclude the latter because they can result in
            // database errors when, for instance, we compare a numeric with a non-numeric type.
            let sql_type = get_sql_type_from_global_config(config, table_name, &column_name, pool);
            if cell.nulltype == None && !is_sql_type_error(&sql_type, &cell.strvalue()) {
                let fkey_cache = match &mut fkey_caches {
                    None => None,
                    Some(fkey_caches) => fkey_caches.get_mut(column_name),
                };
                validate_cell_foreign_constraints(
                    config,
                    pool,
                    None,
                    datatype_conditions,
                    table_name,
                    &column_name,
                    cell,
                    None,
                    fkey_cache,
                )
                .await
                .expect(&format!(
                    "Unable to validate foreign constraints for row number: {:?}, column '{}.{}'",
                    row.row_number, table_name, column_name
                ));
                validate_cell_unique_constraints(
                    config,
                    pool,
                    None,
                    table_name,
                    &column_name,
                    cell,
                    &valve_rows,
                    None,
                )
                .await
                .expect(&format!(
                    "Unable to validate unique constraints for row number: {:?}, column '{}.{}'",
                    row.row_number, table_name, column_name
                ));
            }
            valve_row
                .contents
                .insert(column_name.to_string(), cell.clone());
        }
        // Note that in this implementation, the result rows are never actually returned, but we
        // still need them because the validate_cell_unique_constraints() function needs a list of
        // previous results, and this then requires that we generate the result rows to play that
        // role. The call to cell.clone() above is required to make rust's borrow checker happy.
        valve_rows.push(valve_row);
    }

    Ok(())
}

/// Given a config map, compiled datatype and rule conditions, a table name, the headers for the
/// table, and a number of rows to validate, run intra-row validatation on all of the rows and
/// return the validated versions.
pub fn validate_rows_intra(
    config: &ValveConfig,
    datatype_conditions: &HashMap<String, CompiledCondition>,
    rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    table_name: &String,
    headers: &Vec<String>,
    rows: &Vec<Result<csv::StringRecord, csv::Error>>,
    only_nulltype: bool,
) -> Vec<ValveRow> {
    let mut dt_cache = match DT_CACHE_SIZE {
        0 => None,
        _ => Some(HashMap::new()),
    };
    let mut valve_rows = vec![];
    for row in rows {
        match row {
            Err(err) => log::error!(
                "While processing row for '{}', got error '{}'",
                table_name,
                err
            ),
            Ok(row) => {
                let mut valve_row = ValveRow {
                    row_number: None,
                    contents: IndexMap::new(),
                };
                for (i, value) in row.iter().enumerate() {
                    let result_cell = ValveCell {
                        nulltype: None,
                        value: json!(value),
                        valid: true,
                        messages: vec![],
                    };
                    let column = headers.get(i).unwrap();
                    valve_row.contents.insert(column.to_string(), result_cell);
                }

                let column_names = &config
                    .table
                    .get(table_name)
                    .expect(&format!("Undefined table '{}'", table_name))
                    .column_order;

                // We begin by determining the nulltype of all of the cells, since the rules
                // validation step requires that all cells have this information.
                for column_name in column_names {
                    let cell = valve_row.contents.get_mut(column_name).unwrap();
                    validate_cell_nulltype(
                        config,
                        datatype_conditions,
                        table_name,
                        &column_name,
                        cell,
                    );
                }

                if !only_nulltype {
                    for column_name in column_names {
                        let context = valve_row.clone();
                        let cell = valve_row.contents.get_mut(column_name).unwrap();
                        validate_cell_rules(
                            config,
                            rule_conditions,
                            table_name,
                            &column_name,
                            &context,
                            cell,
                        );

                        if cell.nulltype != None {
                            continue;
                        }

                        if let Some(dt_cache) = &mut dt_cache {
                            // Add a new map for the column to the dt_cache if one doesn't exist:
                            if !dt_cache.contains_key(column_name) {
                                dt_cache.insert(
                                    column_name.to_string(),
                                    LfuCache::with_capacity(DT_CACHE_SIZE),
                                );
                            }
                            let dt_col_cache = dt_cache.get_mut(column_name).unwrap();
                            let string_value = cell.value.to_string();

                            // We do not want to add cells to the datatype validation cache if they
                            // already contain other types of violations, since that will make it
                            // more difficult to construct a unique mapping from values to result
                            // cells; i.e., the same value could in principle map to either a valid
                            // or an invalid cell and this will need to be considered. Ignoring
                            // initially invalid cells (which will be comparitively fewer in number)
                            // thus simplifies the process.
                            if cell.valid {
                                let cached_cell = dt_col_cache.get_mut(&string_value);
                                match cached_cell {
                                    None => {
                                        validate_cell_datatype(
                                            config,
                                            datatype_conditions,
                                            table_name,
                                            &column_name,
                                            cell,
                                        );
                                        dt_col_cache.insert(string_value, cell.clone());
                                    }
                                    Some(cached_cell) => {
                                        cell.nulltype = cached_cell.nulltype.clone();
                                        cell.value = cached_cell.value.clone();
                                        cell.valid = cached_cell.valid;
                                        cell.messages = cached_cell.messages.clone();
                                    }
                                };
                            } else {
                                validate_cell_datatype(
                                    config,
                                    datatype_conditions,
                                    table_name,
                                    &column_name,
                                    cell,
                                );
                            }
                        } else {
                            validate_cell_datatype(
                                config,
                                datatype_conditions,
                                table_name,
                                &column_name,
                                cell,
                            );
                        }
                    }
                }
                valve_rows.push(valve_row);
            }
        };
    }

    // Finally return the result rows:
    valve_rows
}

/// Given a row represented as a [ValveRow], remove any duplicate messages from the row's cells, so
/// that no cell has messages with the same level, rule, and message text.
fn remove_duplicate_messages(row: &mut ValveRow) -> Result<()> {
    for (_column_name, cell) in row.contents.iter_mut() {
        let mut messages = cell.messages.clone();
        messages.sort_by(|a, b| {
            let a = format!("{}{}{}", a.level, a.rule, a.message);
            let b = format!("{}{}{}", b.level, b.rule, b.message,);
            a.partial_cmp(&b).unwrap()
        });
        messages.dedup_by(|a, b| a.level == b.level && a.rule == b.rule && a.message == b.message);
        cell.messages = messages;
    }
    Ok(())
}

/// Generate a SQL Select clause that is a union of: (a) the literal values of the given extra row,
/// and (b) a Select statement over `table_name` of all the fields in the extra row.
pub fn select_with_extra_row(
    config: &ValveConfig,
    extra_row: &ValveRow,
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
        let sql_type = get_sql_type_from_global_config(config, &table, &key, pool);
        let sql_param = cast_sql_param_from_text(&sql_type);
        // enumerate() begins from 0 but we need to begin at 1:
        let i = i + 1;
        first_select.push_str(format!(r#"{} AS "{}""#, sql_param, key).as_str());
        params.push(content.strvalue());
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
    config: &ValveConfig,
    tree: &ValveTreeConstraint,
    table_name: &str,
    effective_table_name: &str,
    root: Option<&String>,
    extra_clause: Option<&String>,
    pool: &AnyPool,
) -> (String, Vec<String>) {
    let empty_string = String::new();
    let extra_clause = extra_clause.unwrap_or_else(|| &empty_string);
    let child_col = &tree.child;
    let parent_col = &tree.parent;

    let mut params = vec![];
    let under_sql;
    if let Some(root) = root {
        let sql_type = get_sql_type_from_global_config(config, table_name, &child_col, pool);
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
pub fn validate_cell_nulltype(
    config: &ValveConfig,
    datatype_conditions: &HashMap<String, CompiledCondition>,
    table_name: &String,
    column_name: &String,
    cell: &mut ValveCell,
) {
    let column = config
        .table
        .get(table_name)
        .and_then(|t| t.column.get(column_name))
        .expect(&format!(
            "Undefined column '{}.{}'",
            table_name, column_name
        ));

    if column.nulltype != "" {
        let nt_name = &column.nulltype;
        let nt_condition = &datatype_conditions.get(nt_name).unwrap().compiled;
        let value = &cell.strvalue();
        if nt_condition(&value) {
            cell.nulltype = Some(nt_name.to_string());
        }
    }
}

/// Given a config map, compiled datatype conditions, a table name, a column name, and a cell to
/// validate, validate the cell's datatype and return the validated cell.
pub fn validate_cell_datatype(
    config: &ValveConfig,
    datatype_conditions: &HashMap<String, CompiledCondition>,
    table_name: &String,
    column_name: &String,
    cell: &mut ValveCell,
) {
    fn construct_message(
        value: &str,
        condition: &CompiledCondition,
        column: &str,
        dt_name: &str,
        dt_description: &str,
    ) -> String {
        match &condition.value_type {
            ValueType::List(_) if dt_description == "" => {
                let concrete_dt = match &condition.parsed {
                    Expression::Function(name, args) if name == "list" => match &*args[0] {
                        Expression::Label(datatype) => datatype,
                        _ => unreachable!(
                            "ValueType::List for a function with an invalid datatype argument"
                        ),
                    },
                    _ => unreachable!("ValueType::List for a non-function condition"),
                };
                format!(
                    "Value '{}' of column {} should be one of a list of tokens whose datatype is {}",
                    value, column, concrete_dt
                )
            }
            ValueType::List(_) => {
                format!(
                    "Value '{}' of column {} should be one of {}",
                    value, column, dt_description
                )
            }
            ValueType::Single if dt_description == "" => {
                format!("{} should be of datatype {}", column, dt_name)
            }
            ValueType::Single => {
                format!("{} should be {}", column, dt_description)
            }
        }
    }

    let column = config
        .table
        .get(table_name)
        .and_then(|t| t.column.get(column_name))
        .expect(&format!(
            "Undefined column '{}.{}'",
            table_name, column_name
        ));
    let primary_dt_name = &column.datatype;
    let primary_dt = &config
        .datatype
        .get(primary_dt_name)
        .expect(&format!("Undefined datatype '{}'", primary_dt_name));
    let primary_dt_desc = &primary_dt.description;
    if let Some(primary_dt_cond) = datatype_conditions.get(primary_dt_name) {
        let strvalue = cell.strvalue();
        let values = match &primary_dt_cond.value_type {
            ValueType::Single => vec![strvalue.as_str()],
            ValueType::List(separator) => strvalue.split(separator).collect::<Vec<_>>(),
        };
        for value in &values {
            if (primary_dt_cond.compiled)(&value) {
                continue;
            }
            cell.valid = false;
            let mut datatypes_to_check =
                get_datatype_ancestors(config, datatype_conditions, primary_dt_name, true);
            // If this datatype has any parents, check them beginning from the most general to
            // the most specific. We use while and pop instead of a for loop so as to check the
            // conditions in LIFO order.
            while !datatypes_to_check.is_empty() {
                let datatype = datatypes_to_check.pop().unwrap();
                let dt_name = &datatype.datatype;
                let dt_description = &datatype.description;
                let dt_condition = &datatype_conditions.get(dt_name).unwrap().compiled;
                if !dt_condition(&value) {
                    let message = construct_message(
                        &value,
                        primary_dt_cond,
                        column_name,
                        dt_name,
                        dt_description,
                    );
                    let message_info = ValveCellMessage {
                        rule: format!("datatype:{}", dt_name),
                        level: "error".to_string(),
                        message: message,
                    };
                    cell.messages.push(message_info);
                }
            }

            let message = construct_message(
                &value,
                primary_dt_cond,
                column_name,
                primary_dt_name,
                primary_dt_desc,
            );
            let message_info = ValveCellMessage {
                rule: format!("datatype:{}", primary_dt_name),
                level: "error".to_string(),
                message: message,
            };
            cell.messages.push(message_info);
        }
    }
}

/// Given a config map, compiled rule conditions, a table name, a column name, the row context,
/// and the cell to validate, look in the rule table (if it exists) and validate the cell according
/// to any applicable rules.
pub fn validate_cell_rules(
    config: &ValveConfig,
    rules: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    table_name: &String,
    column_name: &String,
    context: &ValveRow,
    cell: &mut ValveCell,
) {
    fn check_condition(
        condition_type: &str,
        cell: &ValveCell,
        rule: &ValveRuleConfig,
        table_name: &String,
        column_name: &String,
        rules: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    ) -> bool {
        let condition = {
            if condition_type == "when" {
                rule.when_condition.as_str()
            } else if condition_type == "then" {
                rule.then_condition.as_str()
            } else {
                panic!("Invalid condition type: {}", condition_type);
            }
        };

        if vec!["null", "not null"].contains(&condition) {
            return (condition == "null" && cell.nulltype != None)
                || (condition == "not null" && cell.nulltype == None);
        } else {
            let compiled_condition = rules
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
            return compiled_condition(&cell.strvalue());
        }
    }

    let applicable_rules = match config.rule.get(table_name).and_then(|t| t.get(column_name)) {
        Some(rules) => rules,
        _ => return,
    };

    for (rule_number, rule) in applicable_rules.iter().enumerate() {
        // enumerate() begins at 0 by default but we need to begin with 1:
        let rule_number = rule_number + 1;
        // Check the then condition only if the when condition is satisfied:
        if check_condition("when", cell, rule, table_name, column_name, rules) {
            let then_cell = context.contents.get(&rule.then_column).unwrap();
            if !check_condition("then", then_cell, rule, table_name, column_name, rules) {
                cell.valid = false;
                cell.messages.push(ValveCellMessage {
                    rule: format!("rule:{}-{}", column_name, rule_number),
                    level: rule.level.to_string(),
                    message: rule.description.to_string(),
                });
            }
        }
    }
}

/// Generates an SQL fragment representing the "as if" portion of a query that will be used for
/// counterfactual validation.
pub fn as_if_to_sql(
    config: &ValveConfig,
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
                let columns = row.contents.keys().cloned().collect::<Vec<_>>();
                let values = {
                    let mut values = vec![];
                    for column in &columns {
                        let valid = row
                            .contents
                            .get(column)
                            .and_then(|c| Some(c.valid))
                            .unwrap();

                        let value = {
                            if valid == true {
                                let value = row
                                    .contents
                                    .get(column)
                                    .and_then(|c| Some(c.strvalue()))
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

                        let sql_type =
                            get_sql_type_from_global_config(&config, &as_if.table, &column, pool);

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
pub async fn validate_cell_foreign_constraints(
    config: &ValveConfig,
    pool: &AnyPool,
    mut tx: Option<&mut Transaction<'_, sqlx::Any>>,
    datatype_conditions: &HashMap<String, CompiledCondition>,
    table_name: &String,
    column_name: &String,
    cell: &mut ValveCell,
    query_as_if: Option<&QueryAsIf>,
    mut fkey_cache: Option<&mut LfuCache<String, bool>>,
) -> Result<()> {
    let fkeys = config
        .constraint
        .foreign
        .get(table_name)
        .expect(&format!("Undefined table '{}'", table_name))
        .iter()
        .filter(|t| t.column == *column_name)
        .collect::<Vec<_>>();

    // If there are no foreign keys, then just return:
    if fkeys.is_empty() {
        return Ok(());
    }

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

    /// Use the given SQL statement to determine whether the value associated with the given cell
    /// is in the database. Use the given transaction if it is set, otherwise use the pool.
    async fn fkey_in_db(
        pool: &AnyPool,
        tx: &mut Option<&mut Transaction<'_, sqlx::Any>>,
        cell: &mut ValveCell,
        fsql: &str,
    ) -> Result<bool> {
        let frows = {
            if let None = tx {
                sqlx_query(&fsql)
                    .bind(&cell.strvalue())
                    .fetch_all(pool)
                    .await?
            } else {
                sqlx_query(&fsql)
                    .bind(&cell.strvalue())
                    .fetch_all(tx.as_mut().unwrap().acquire().await?)
                    .await?
            }
        };
        Ok(!frows.is_empty())
    }

    let strvalue = cell.strvalue();
    let values = {
        let value_type = {
            let datatype = &config
                .table
                .get(table_name)
                .expect(&format!("No config found for table '{}'", table_name))
                .column
                .get(column_name)
                .expect(&format!(
                    "No config found for column '{}' of table '{}'",
                    column_name, table_name
                ))
                .datatype;
            match datatype_conditions.get(datatype) {
                None => ValueType::Single,
                Some(condition) => condition.value_type.clone(),
            }
        };
        match value_type {
            ValueType::Single => vec![strvalue.as_str()],
            ValueType::List(separator) => strvalue.split(&separator).collect::<Vec<_>>(),
        }
    };

    for value in &values {
        for fkey in &fkeys {
            let ftable = &fkey.ftable;
            let (as_if_clause, ftable_alias) = match query_as_if {
                Some(query_as_if) if *ftable == query_as_if.table => {
                    (as_if_clause.to_string(), query_as_if.alias.to_string())
                }
                _ => ("".to_string(), ftable.to_string()),
            };
            let fcolumn = &fkey.fcolumn;
            let sql_type =
                get_sql_type_from_global_config(&config, ftable, fcolumn, pool).to_lowercase();
            let sql_param = cast_sql_param_from_text(&sql_type);
            let fsql = local_sql_syntax(
                &pool,
                &format!(
                    r#"{}SELECT 1 FROM "{}" WHERE "{}" = {} LIMIT 1"#,
                    as_if_clause,
                    ftable_alias,
                    fcolumn,
                    {
                        if sql_type == "text" || sql_type.starts_with("varchar(") {
                            format!("'{}'", value)
                        } else {
                            format!("{}", value)
                        }
                    }
                ),
            );

            let fkey_satisfied = match fkey_cache {
                Some(ref mut fkey_cache) => match fkey_cache.get(&value.to_string()) {
                    Some(in_db) => *in_db,
                    None => {
                        let in_db = fkey_in_db(pool, &mut tx, cell, &fsql).await?;
                        let key = value;
                        fkey_cache.insert(key.to_string(), in_db);
                        in_db
                    }
                },
                None => fkey_in_db(pool, &mut tx, cell, &fsql).await?,
            };

            if !fkey_satisfied {
                cell.valid = false;
                let mut message = ValveCellMessage {
                    rule: "key:foreign".to_string(),
                    level: "error".to_string(),
                    message: format!(
                        "Value '{}' of column {} is not in {}.{}",
                        value, column_name, ftable, fcolumn
                    ),
                };
                let foptions = &config
                    .table
                    .get(ftable)
                    .expect(&format!(
                        "Foreign table: '{}' is not in table config",
                        ftable
                    ))
                    .options;

                if foptions.contains("conflict") {
                    let (as_if_clause_for_conflict, ftable_alias) = match query_as_if {
                        Some(query_as_if) if *ftable == query_as_if.table => (
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
                            sqlx_query(&fsql).bind(value).fetch_all(pool).await?
                        } else {
                            sqlx_query(&fsql)
                                .bind(value)
                                .fetch_all(tx.as_mut().unwrap().acquire().await?)
                                .await?
                        }
                    };
                    if !frows.is_empty() {
                        message.message = format!(
                            "Value '{}' of column {} exists only in {}_conflict.{}",
                            value, column_name, ftable, fcolumn
                        );
                    }
                }
                cell.messages.push(message);
            }
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
pub async fn validate_cell_unique_constraints(
    config: &ValveConfig,
    pool: &AnyPool,
    mut tx: Option<&mut Transaction<'_, sqlx::Any>>,
    table_name: &String,
    column_name: &String,
    cell: &mut ValveCell,
    prev_results: &Vec<ValveRow>,
    row_number: Option<u32>,
) -> Result<()> {
    // If the column has a primary or unique key constraint, or if it is the child associated with
    // a tree, then if the value of the cell is a duplicate either of one of the previously
    // validated rows in the batch, or a duplicate of a validated row that has already been inserted
    // into the table, mark it with the corresponding error:
    let primaries = config
        .constraint
        .primary
        .get(table_name)
        .expect(&format!("Undefined table '{}'", table_name));
    let uniques = config
        .constraint
        .unique
        .get(table_name)
        .expect(&format!("Undefined table '{}'", table_name));
    let trees = config
        .constraint
        .tree
        .get(table_name)
        .expect(&format!("Undefined table '{}'", table_name))
        .iter()
        .map(|t| &t.child)
        .collect::<Vec<_>>();

    let is_primary = primaries.contains(column_name);
    let is_unique = !is_primary && uniques.contains(column_name);
    let is_tree_child = trees.contains(&column_name);

    fn make_error(rule: &str, column_name: &String) -> ValveCellMessage {
        ValveCellMessage {
            rule: rule.to_string(),
            level: "error".to_string(),
            message: format!("Values of {} must be unique", column_name.to_string()),
        }
    }

    if is_primary || is_unique || is_tree_child {
        let table_options = get_table_options(config, table_name)?;
        let mut query_table = {
            if !table_options.contains("conflict") {
                table_name.to_string()
            } else {
                format!("{}_view", table_name)
            }
        };
        let mut with_sql = String::new();
        let except_table = format!("{}_exc", query_table);
        if let Some(row_number) = row_number {
            with_sql = format!(
                r#"WITH "{}" AS (
                       SELECT * FROM "{}"
                       WHERE "row_number" != {}
                   ) "#,
                except_table, query_table, row_number
            );
        }

        if !with_sql.is_empty() {
            query_table = except_table;
        }

        let sql_type = get_sql_type_from_global_config(&config, &table_name, &column_name, pool);
        let sql_param = cast_sql_param_from_text(&sql_type);
        let sql = local_sql_syntax(
            &pool,
            &format!(
                r#"{} SELECT 1 FROM "{}" WHERE "{}" = {} LIMIT 1"#,
                with_sql, query_table, column_name, sql_param
            ),
        );
        let strvalue = cell.strvalue();
        let query = sqlx_query(&sql).bind(&strvalue);

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
