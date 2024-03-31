//! The Valve Toolkit - low-level functions useful for working with Valve.

use crate::{
    ast::Expression,
    internal::{generate_internal_table_config, INTERNAL_TABLES},
    validate::{
        validate_row_tx, validate_rows_constraints, validate_rows_intra, validate_rows_trees,
    },
    valve::{
        ValveCell, ValveCellMessage, ValveChange, ValveColumnConfig, ValveConfig,
        ValveConstraintConfig, ValveDatatypeConfig, ValveError, ValveForeignConstraint, ValveRow,
        ValveRowChange, ValveRuleConfig, ValveSpecialConfig, ValveTableConfig, ValveTreeConstraint,
        ValveUnderConstraint,
    },
    valve_grammar::StartParser,
    CHUNK_SIZE, MAX_DB_CONNECTIONS, MULTI_THREADED, SQL_PARAM, SQL_TYPES,
};
use anyhow::Result;
use async_recursion::async_recursion;
use crossbeam;
use csv::{ReaderBuilder, StringRecord, StringRecordsIter};
use futures::executor::block_on;
use indexmap::IndexMap;
use indoc::indoc;
use is_executable::IsExecutable;
use itertools::{IntoChunks, Itertools};
use petgraph::{
    algo::{all_simple_paths, toposort},
    graphmap::DiGraphMap,
    Direction,
};
use regex::Regex;
use serde_json::{json, Value as SerdeValue};
use sqlx::{
    any::{AnyConnectOptions, AnyKind, AnyPool, AnyPoolOptions, AnyRow},
    query as sqlx_query, Acquire, Column, Row, Transaction, ValueRef,
};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::File,
    iter::FromIterator,
    path::Path,
    str::FromStr,
    sync::Arc,
};

/// Alias for [Map](serde_json::map)<[String], [Value](serde_json::value)>.
pub type SerdeMap = serde_json::Map<String, SerdeValue>;

/// Represents a structure such as those found in the `structure` column of the `column` table in
/// both its parsed format (i.e., as an [Expression](ast/enum.Expression.html)) as well as in its
/// original format (i.e., as a plain [String]).
#[derive(Clone)]
pub struct ParsedStructure {
    pub original: String,
    pub parsed: Expression,
}

// We use Debug here instead of Display because we have only implemented Debug for Expressions.
// See the comment about this in ast.rs.
impl std::fmt::Debug for ParsedStructure {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{{\"parsed_structure\": {{\"original\": \"{}\", \"parsed\": {:?}}}}}",
            &self.original, &self.parsed
        )
    }
}

/// Represents a condition in three different ways: (i) in String format, (ii) as a parsed
/// [Expression](ast/enum.Expression.html), and (iii) as a pre-compiled regular expression.
#[derive(Clone)]
pub struct CompiledCondition {
    pub original: String,
    pub parsed: Expression,
    pub compiled: Arc<dyn Fn(&str) -> bool + Sync + Send>,
}

// We use Debug here instead of Display because we have only implemented Debug for Expressions.
// See the comment about this in ast.rs.
impl std::fmt::Debug for CompiledCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{{\"compiled_condition\": {{\"original\": \"{}\", \"parsed\": {:?}}}}}",
            &self.original, &self.parsed
        )
    }
}

/// Represents a 'when-then' condition, as found in the `rule` table, as two
/// [CompiledCondition](struct.CompiledCondition.html) structs corresponding to the when and then
/// parts of the given rule.
#[derive(Clone)]
pub struct ColumnRule {
    pub when: CompiledCondition,
    pub then: CompiledCondition,
}

// We use Debug here instead of Display because we have only implemented Debug for Expressions.
// See the comment about this in ast.rs.
impl std::fmt::Debug for ColumnRule {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{{\"column_rule\": {{\"when\": {:?}, \"then\": {:?}}}}}",
            &self.when, &self.then
        )
    }
}

/// Used for counterfactual queries. Use this struct to specify that a query should be run as if
/// the row specified in the instance were (depending on `kind`) either added, removed or replaced.
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

/// The sense in which a [QueryAsIf] struct should be interpreted.
#[derive(Clone, Debug, PartialEq)]
pub enum QueryAsIfKind {
    Add,
    Remove,
    Replace,
}

/// Given a string representing the location of a database, return a database connection pool.
pub async fn get_pool_from_connection_string(database: &str) -> Result<AnyPool> {
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
        connection_options = AnyConnectOptions::from_str(connection_string.as_str())?;
    }

    let pool = AnyPoolOptions::new()
        .max_connections(MAX_DB_CONNECTIONS)
        .connect_with(connection_options)
        .await?;
    Ok(pool)
}

/// Given the path to a table table (either a table.tsv file or a database containing a
/// table named "table"), load and check the 'table', 'column', and 'datatype' tables, and return
/// the following items:
/// - Special table configuration information
/// - Table configuration information for all managed tables
/// - Table configuration information for all managed datatypes
/// - Rule configuration information for every column of every managed table
/// - Constraint configuration information
/// - The list of managed tables in dependency order
/// - A map from table names to the tables that depend on a given table
/// - A map from table names to the tables that a given table depends on
pub fn read_config_files(
    path: &str,
    parser: &StartParser,
    pool: &AnyPool,
) -> Result<(
    ValveSpecialConfig,
    HashMap<String, ValveTableConfig>,
    Vec<String>,
    HashMap<String, ValveDatatypeConfig>,
    HashMap<String, HashMap<String, Vec<ValveRuleConfig>>>,
    ValveConstraintConfig,
    Vec<String>,
    HashMap<String, Vec<String>>,
    HashMap<String, Vec<String>>,
)> {
    // Given a list of columns that are required for some table, and a subset of those columns
    // that are required to have values, check if both sets of requirements are met by the given
    // row. Returns a ValveError if both sets of requirements are not met, or if values_are_required
    // is not a subset of columns_are_required.
    fn check_table_requirements(
        columns_are_required: &Vec<&str>,
        values_are_required: &Vec<&str>,
        row: &SerdeMap,
    ) -> Result<()> {
        let columns_are_required: HashSet<&str> =
            HashSet::from_iter(columns_are_required.iter().cloned());
        let values_are_required: HashSet<&str> =
            HashSet::from_iter(values_are_required.iter().cloned());
        if !values_are_required.is_subset(&columns_are_required) {
            return Err(ValveError::InputError(format!(
                "{:?} is not a subset of {:?}",
                values_are_required, columns_are_required
            ))
            .into());
        }

        for &column in columns_are_required.iter() {
            match row.get(column).and_then(|c| c.as_str()) {
                None => {
                    return Err(ValveError::ConfigError(format!(
                        "Missing required column '{}'",
                        column
                    ))
                    .into());
                }
                Some(value) if value == "" && values_are_required.contains(&column) => {
                    return Err(ValveError::ConfigError(format!(
                        "Missing required value for '{}'",
                        column
                    ))
                    .into());
                }
                _ => (),
            }
        }
        Ok(())
    }

    // 1. Load the table config for the 'table' table from the given path, and determine the
    // table names to use for the other special config types: 'column', 'datatype', and 'rule', then
    // save those in specials_config. Also begin filling out the more general table configuration
    // information related to each of those tables, to which further info will be added later.
    let mut specials_config = ValveSpecialConfig::default();
    let mut tables_config = HashMap::new();
    let mut table_order = vec![];
    let rows = {
        // Read in the table table from either a file or the database table called "table".
        if path.to_lowercase().ends_with(".tsv") {
            read_tsv_into_vector(path)?
        } else {
            read_db_table_into_vector(pool, "table")?
        }
    };

    for row in rows {
        if let Err(e) = check_table_requirements(
            &vec!["table", "path", "type", "description"],
            &vec!["table"],
            &row,
        ) {
            return Err(ValveError::ConfigError(format!(
                "Error while reading '{}': {:?}",
                path, e
            ))
            .into());
        }

        let row_table = row.get("table").and_then(|t| t.as_str()).unwrap();
        table_order.push(row_table.into());
        let row_path = row.get("path").and_then(|t| t.as_str()).unwrap();
        let row_path_info = Path::new(&row_path);
        let row_type = row.get("type").and_then(|t| t.as_str()).unwrap();
        let row_type = row_type.to_lowercase();
        let row_type = row_type.as_str();
        let row_mode = row.get("mode").and_then(|t| t.as_str()).unwrap();
        let row_mode = row_mode.to_lowercase();
        let row_mode = row_mode.as_str();
        let row_desc = row.get("description").and_then(|t| t.as_str()).unwrap();

        if row_mode == "internal" {
            return Err(ValveError::ConfigError(format!(
                "The mode 'internal' is reserved for internal use and is not allowed to be \
                 specified as a table mode in '{}'",
                path
            ))
            .into());
        }

        // Here is a summary of the allowed table configurations for the various table modes:
        // - A table of mode 'view' is allowed to have an empty path. If the path is non-empty then
        //   it must either end (case insensitively) in '.sql' or be an executable file. It must not
        //   end (case insensitively) in '.tsv'.
        // - A 'readonly' table is allowed to have an empty path. If the path is non-empty then it
        //   can be a file ending (case insensitively) with '.tsv', a file ending (case
        //   insensitively) with '.sql', or an executable file.
        // - All other tables must have a path and it must end (case insensitively) in '.tsv'.
        if vec!["view", "readonly"].contains(&row_mode) {
            if row_mode == "view" && row_path.ends_with(".tsv") {
                return Err(ValveError::ConfigError(format!(
                    "Invalid path '{}' for view '{}'. TSV files are not supported for views.",
                    row_path, row_table,
                ))
                .into());
            }
            if row_path != "" && !row_path.ends_with(".tsv") && !row_path.ends_with(".sql") {
                if !row_path_info.is_executable() {
                    return Err(ValveError::ConfigError(format!(
                        "The generic program '{}' associated with the view or readonly table '{}' \
                         is not executable (assuming that it even exists at all)",
                        row_path, row_table
                    ))
                    .into());
                }
            }
        } else if row_mode != "internal" && !row_path.to_lowercase().ends_with(".tsv") {
            return Err(ValveError::ConfigError(format!(
                "Illegal path for table '{}'. Tables of mode '{}' require a path that ends in \
                 '.tsv'",
                row_table, row_mode
            ))
            .into());
        }

        // Check that the table table path is the same as the path that was input as an argument to
        // this function:
        if row_type == "table" {
            if path.to_lowercase().ends_with(".tsv") && row_path != path {
                return Err(ValveError::ConfigError(format!(
                    "The \"table\" table path '{}' is not the expected '{}'",
                    row_path, path
                ))
                .into());
            }
        }

        let duplicate_err_msg = format!(
            "Multiple tables with type '{}' declared in '{}'",
            row_type, path
        );
        match row_type {
            "" => (), // Tables with no type are ignored.
            "column" => {
                if specials_config.column != "" {
                    return Err(ValveError::ConfigError(duplicate_err_msg).into());
                }
                specials_config.column = row_table.to_string();
            }
            "datatype" => {
                if specials_config.datatype != "" {
                    return Err(ValveError::ConfigError(duplicate_err_msg).into());
                }
                specials_config.datatype = row_table.to_string();
            }
            "rule" => {
                if specials_config.rule != "" {
                    return Err(ValveError::ConfigError(duplicate_err_msg).into());
                }
                specials_config.rule = row_table.to_string();
            }
            "table" => {
                if specials_config.table != "" {
                    return Err(ValveError::ConfigError(duplicate_err_msg).into());
                }
                specials_config.table = row_table.to_string();
            }
            _ => {
                return Err(ValveError::ConfigError(format!(
                    "Unrecognized table type '{}' in '{}'",
                    row_type, path
                ))
                .into())
            }
        };

        tables_config.insert(
            row_table.to_string(),
            ValveTableConfig {
                table: row_table.to_string(),
                table_type: row_type.to_string(),
                mode: row_mode.to_string(),
                description: row_desc.to_string(),
                path: row_path.to_string(),
                ..Default::default()
            },
        );
    }

    // Check that all the required special tables are present
    if specials_config.column == "" {
        return Err(ValveError::ConfigError(format!(
            "Missing required 'column' table in '{}'",
            path
        ))
        .into());
    }
    if specials_config.datatype == "" {
        return Err(ValveError::ConfigError(format!(
            "Missing required 'datatype' table in '{}'",
            path
        ))
        .into());
    }
    if specials_config.table == "" {
        return Err(ValveError::ConfigError(format!(
            "Missing required 'table' table in '{}'",
            path
        ))
        .into());
    }

    // Helper function for extracting special configuration (other than the main 'table'
    // configuration) from either a file or a table in the database, depending on the value of
    // `path`. When `path` ends in '.tsv', the path of the config table corresponding to
    // `table_type` is looked up, the TSV is read, and the rows are returned. When `path` does not
    // end in '.tsv', the table name corresponding to `table_type` is looked up in the database
    // indicated by `path`, the table is read, and the rows are returned.
    fn get_special_config(
        table_type: &str,
        specials_config: &ValveSpecialConfig,
        tables_config: &HashMap<String, ValveTableConfig>,
        table_table: &str,
        pool: &AnyPool,
    ) -> Result<Vec<SerdeMap>> {
        if table_table.to_lowercase().ends_with(".tsv") {
            let table_name = match table_type {
                "column" => &specials_config.column,
                "datatype" => &specials_config.datatype,
                "rule" => {
                    let rule_table = &specials_config.rule;
                    if rule_table == "" {
                        return Err(ValveError::ConfigError(format!(
                            "Tried to get special config for rule table but it is undefined"
                        ))
                        .into());
                    }
                    rule_table
                }
                _ => {
                    return Err(ValveError::InputError(format!(
                        "In get_special_config(): Table type '{}' not supported for this function.",
                        table_type
                    ))
                    .into())
                }
            };
            let path = String::from(
                tables_config
                    .get(table_name)
                    .and_then(|t| Some(t.path.to_string()))
                    .ok_or(ValveError::ConfigError(format!(
                        "Table '{}', supposedly of type '{}', not found in tables config",
                        table_name, table_type
                    )))?,
            );
            read_tsv_into_vector(&path)
        } else {
            let mut db_table = None;
            for (table_name, table_config) in tables_config {
                let this_type = table_config.table_type.as_str();
                if this_type == table_type {
                    db_table = Some(table_name);
                    break;
                }
            }
            let db_table = match db_table {
                None => {
                    return Err(ValveError::ConfigError(format!(
                        "Could not determine special table name for type '{}'.",
                        table_type
                    ))
                    .into())
                }
                Some(table) => table,
            };
            read_db_table_into_vector(pool, db_table)
        }
    }

    // 2. Load the datatype table.
    let mut datatypes_config = HashMap::new();
    let rows = get_special_config("datatype", &specials_config, &tables_config, path, pool)?;
    for row in rows {
        if let Err(e) = check_table_requirements(
            &vec![
                "datatype",
                "HTML type",
                "SQL type",
                "condition",
                "description",
                "parent",
            ],
            &vec!["datatype"],
            &row,
        ) {
            return Err(ValveError::ConfigError(format!(
                "Error while reading from datatype table: {:?}",
                e
            ))
            .into());
        }

        let dt_name = row.get("datatype").and_then(|d| d.as_str()).unwrap();
        let html_type = row.get("HTML type").and_then(|s| s.as_str()).unwrap();
        let sql_type = row.get("SQL type").and_then(|s| s.as_str()).unwrap();
        let condition = row.get("condition").and_then(|s| s.as_str()).unwrap();
        let description = row.get("description").and_then(|s| s.as_str()).unwrap();
        let parent = row.get("parent").and_then(|s| s.as_str()).unwrap();
        datatypes_config.insert(
            dt_name.to_string(),
            ValveDatatypeConfig {
                html_type: html_type.to_string(),
                sql_type: sql_type.to_string(),
                condition: condition.to_string(),
                datatype: dt_name.to_string(),
                description: description.to_string(),
                parent: parent.to_string(),
            },
        );
    }

    // Check that all the essential datatypes have been configured:
    for dt in vec!["text", "empty", "line", "word"] {
        if !datatypes_config.contains_key(dt) {
            return Err(
                ValveError::ConfigError(format!("Missing required datatype: '{}'", dt)).into(),
            );
        }
    }

    // 3. Load the column table.
    let rows = get_special_config("column", &specials_config, &tables_config, path, pool)?;
    for row in rows {
        if let Err(e) = check_table_requirements(
            &vec![
                "table",
                "nulltype",
                "datatype",
                "column",
                "description",
                "label",
                "structure",
            ],
            &vec!["table", "column", "datatype"],
            &row,
        ) {
            return Err(ValveError::ConfigError(format!(
                "Error while reading from column table: {:?}",
                e
            ))
            .into());
        }

        let row_table = row.get("table").and_then(|t| t.as_str()).unwrap();
        if !tables_config.contains_key(row_table) {
            return Err(ValveError::ConfigError(format!("Undefined table '{}'", row_table)).into());
        }
        let nulltype = row.get("nulltype").and_then(|t| t.as_str()).unwrap();
        if nulltype != "" && !datatypes_config.contains_key(nulltype) {
            return Err(
                ValveError::ConfigError(format!("Undefined nulltype '{}'", nulltype)).into(),
            );
        }
        let datatype = row.get("datatype").and_then(|d| d.as_str()).unwrap();
        if !datatypes_config.contains_key(datatype) {
            return Err(
                ValveError::ConfigError(format!("Undefined datatype '{}'", datatype)).into(),
            );
        }
        let column_name = row.get("column").and_then(|c| c.as_str()).unwrap();
        let description = row.get("description").and_then(|c| c.as_str()).unwrap();
        let label = row.get("label").and_then(|c| c.as_str()).unwrap();
        let structure = row.get("structure").and_then(|c| c.as_str()).unwrap();
        tables_config.get_mut(row_table).and_then(|t| {
            Some(t.column.insert(
                column_name.to_string(),
                ValveColumnConfig {
                    table: row_table.to_string(),
                    column: column_name.to_string(),
                    datatype: datatype.to_string(),
                    description: description.to_string(),
                    label: label.to_string(),
                    structure: structure.to_string(),
                    nulltype: nulltype.to_string(),
                },
            ))
        });
    }

    // 4. Load rule table if it exists
    let mut rules_config = HashMap::new();
    if specials_config.rule != "" {
        let table_name = &specials_config.rule;
        let rows = get_special_config(table_name, &specials_config, &tables_config, path, pool)?;
        for row in rows {
            if let Err(e) = check_table_requirements(
                &vec![
                    "table",
                    "when column",
                    "when condition",
                    "then column",
                    "then condition",
                    "level",
                    "description",
                ],
                &vec![
                    "table",
                    "when column",
                    "when condition",
                    "then column",
                    "then condition",
                    "level",
                    "description",
                ],
                &row,
            ) {
                return Err(ValveError::ConfigError(format!(
                    "Error while reading from rule table: {:?}",
                    e
                ))
                .into());
            }

            let row_table = row.get("table").and_then(|t| t.as_str()).unwrap();
            if !tables_config.contains_key(row_table) {
                return Err(ValveError::ConfigError(format!(
                    "Undefined table '{}' while reading rule configuration",
                    row_table
                ))
                .into());
            }

            // Add the rule specified in the given row to the list of rules associated with the
            // value of the when column:
            if !rules_config.contains_key(row_table) {
                rules_config.insert(String::from(row_table), HashMap::new());
            }
            let table_rule_config = rules_config.get_mut(row_table).unwrap();

            let when_col = row.get("when column").and_then(|c| c.as_str()).unwrap();
            if !table_rule_config.contains_key(when_col) {
                table_rule_config.insert(String::from(when_col), vec![]);
            }

            let column_rule_config = table_rule_config.get_mut(&when_col.to_string()).unwrap();
            let desc = row.get("description").and_then(|c| c.as_str()).unwrap();
            let level = row.get("level").and_then(|c| c.as_str()).unwrap();
            let when_con = row.get("when condition").and_then(|c| c.as_str()).unwrap();
            let then_col = row.get("then column").and_then(|c| c.as_str()).unwrap();
            let then_con = row.get("then condition").and_then(|c| c.as_str()).unwrap();
            column_rule_config.push(ValveRuleConfig {
                description: desc.to_string(),
                level: level.to_string(),
                table: row_table.to_string(),
                then_column: then_col.to_string(),
                then_condition: then_con.to_string(),
                when_column: when_col.to_string(),
                when_condition: when_con.to_string(),
            });
        }
    }

    // 5. Initialize the constraints config:
    let mut constraints_config = ValveConstraintConfig::default();
    for table_name in tables_config.keys().cloned().collect::<Vec<_>>() {
        let this_table = tables_config
            .get(&table_name)
            .ok_or(ValveError::ConfigError(format!(
                "Table '{}' not found in tables config",
                table_name
            )))?;

        let mut path = None;
        if !Path::new(&this_table.path).is_file() {
            log::warn!("File does not exist {}", this_table.path);
        } else if Path::new(&this_table.path).canonicalize().is_err() {
            log::warn!("File path could not be made canonical {}", this_table.path);
        } else {
            path = Some(this_table.path.to_string())
        }

        // Constraints on internal tables do not need to be configured explicitly:
        if this_table.mode == "internal" {
            continue;
        }

        let this_column_config = &this_table.column;
        let defined_columns: Vec<String> = this_column_config.keys().cloned().collect::<Vec<_>>();

        // We use column_order to explicitly indicate the order in which the columns should appear
        // in the table, for later reference. The default is to preserve the order from the actual
        // table file. If that does not exist, we use the ordering in defined_columns.
        let mut column_order = vec![];
        match path {
            Some(path) if path.to_lowercase().ends_with(".tsv") => {
                // Get the actual columns from the data itself. Note that we set has_headers to
                // false (even though the files have header rows) in order to explicitly read the
                // header row.
                let mut rdr = ReaderBuilder::new()
                    .has_headers(false)
                    .delimiter(b'\t')
                    .from_reader(File::open(path.clone()).map_err(|err| {
                        ValveError::ConfigError(format!(
                            "Unable to open '{}': {}",
                            path.clone(),
                            err
                        ))
                    })?);
                let mut iter = rdr.records();
                if let Some(result) = iter.next() {
                    let actual_columns = result
                        .map_err(|e| {
                            ValveError::ConfigError(format!(
                                "Unable to read row from '{}': {}",
                                path, e
                            ))
                        })?
                        .iter()
                        .map(|c| c.to_string())
                        .collect::<Vec<_>>();
                    // Make sure that the actual columns found in the table file, and the columns
                    // defined in the column config, exactly match in terms of their content:
                    for column_name in &actual_columns {
                        column_order.push(column_name.to_string());
                        if !defined_columns.contains(&&column_name.to_string()) {
                            return Err(ValveError::ConfigError(format!(
                                "Column '{}.{}' not in column config",
                                table_name, column_name
                            ))
                            .into());
                        }
                    }
                    for column_name in &defined_columns {
                        if !actual_columns.contains(&column_name.to_string()) {
                            return Err(ValveError::ConfigError(format!(
                                "Defined column '{}.{}' not found in table",
                                table_name, column_name
                            ))
                            .into());
                        }
                    }
                } else {
                    return Err(ValveError::ConfigError(format!("'{}' is empty", path)).into());
                }
            }
            _ => (),
        };
        if column_order.is_empty() {
            column_order = defined_columns.clone();
        }
        tables_config
            .get_mut(&table_name)
            .and_then(|t| Some(t.column_order = column_order));

        // Populate the table constraints for this table:
        let (primaries, uniques, foreigns, trees, unders) = get_table_constraints(
            &tables_config,
            &datatypes_config,
            parser,
            &table_name,
            &pool,
        );
        constraints_config
            .primary
            .insert(table_name.to_string(), primaries);
        constraints_config
            .unique
            .insert(table_name.to_string(), uniques);
        constraints_config
            .foreign
            .insert(table_name.to_string(), foreigns);
        constraints_config
            .tree
            .insert(table_name.to_string(), trees);
        constraints_config
            .under
            .insert(table_name.to_string(), unders);
    }

    // 6. Add internal table configuration to the table config:
    for table in INTERNAL_TABLES.iter() {
        tables_config.insert(table.to_string(), generate_internal_table_config(table));
        table_order.push(table.to_string());
    }

    // 7. Sort the tables (other than internal tables) according to their foreign key
    // dependencies so that tables are always loaded after the tables they depend on.
    let (sorted_tables, table_dependencies_in, table_dependencies_out) = verify_table_deps_and_sort(
        &tables_config
            .keys()
            .cloned()
            // Internal tables will be taken account of within verify_table_deps_and_sort() and
            // manually added to the sorted table list that is returned there.
            .filter(|m| !INTERNAL_TABLES.contains(&m.to_string().as_str()))
            .collect(),
        &constraints_config,
    );

    // 8. Finally, return all the configs:
    Ok((
        specials_config,
        tables_config,
        table_order,
        datatypes_config,
        rules_config,
        constraints_config,
        sorted_tables,
        table_dependencies_in,
        table_dependencies_out,
    ))
}

/// Given the global configuration struct and a parser, compile all of the datatype conditions,
/// add them to a hash map whose keys are the text versions of the conditions and whose values
/// are the compiled conditions, and then finally return the hash map.
pub fn get_compiled_datatype_conditions(
    config: &ValveConfig,
    parser: &StartParser,
) -> Result<HashMap<String, CompiledCondition>> {
    let mut compiled_datatype_conditions: HashMap<String, CompiledCondition> = HashMap::new();
    for (dt_name, dt_config) in config.datatype.iter() {
        let condition = dt_config.condition.as_str();
        if condition != "" {
            let compiled_condition =
                compile_condition(condition, parser, &compiled_datatype_conditions)?;
            compiled_datatype_conditions.insert(dt_name.to_string(), compiled_condition);
        }
    }
    Ok(compiled_datatype_conditions)
}

/// Given the global config struct, a hash map of compiled datatype conditions (indexed by the text
/// versions of the conditions), and a parser, compile all of the rule conditions, add them to a
/// hash which has the following structure and return it:
/// ```
/// {
///      table_1: {
///          when_column_1: [rule_1, rule_2, ...],
///          ...
///      },
///      ...
/// }
/// ```
pub fn get_compiled_rule_conditions(
    config: &ValveConfig,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    parser: &StartParser,
) -> Result<HashMap<String, HashMap<String, Vec<ColumnRule>>>> {
    let mut compiled_rule_conditions = HashMap::new();
    let tables_config = &config.table;
    let rules_config = &config.rule;
    for (rules_table, table_rules) in rules_config.iter() {
        for (column_rule_key, column_rules) in table_rules.iter() {
            for rule in column_rules {
                let table_columns = tables_config
                    .get(rules_table)
                    .unwrap()
                    .column
                    .keys()
                    .collect::<Vec<_>>();
                for column in vec![&rule.when_column, &rule.then_column] {
                    if !table_columns.contains(&column) {
                        return Err(ValveError::DataError(format!(
                            "Undefined column '{}.{}' in rules table",
                            rules_table, column
                        ))
                        .into());
                    }
                }
                let when_compiled =
                    compile_condition(&rule.when_condition, parser, &compiled_datatype_conditions)?;
                let then_compiled =
                    compile_condition(&rule.then_condition, parser, &compiled_datatype_conditions)?;

                if !compiled_rule_conditions.contains_key(rules_table) {
                    let table_rules = HashMap::new();
                    compiled_rule_conditions.insert(rules_table.to_string(), table_rules);
                }
                let table_rules = compiled_rule_conditions.get_mut(rules_table).unwrap();
                if !table_rules.contains_key(column_rule_key) {
                    table_rules.insert(column_rule_key.to_string(), vec![]);
                }
                let column_rules = table_rules.get_mut(column_rule_key).unwrap();
                column_rules.push(ColumnRule {
                    when: when_compiled,
                    then: then_compiled,
                });
            }
        }
    }
    Ok(compiled_rule_conditions)
}

/// Given the global config struct and a parser, parse all of the structure conditions, add them to
/// a hash map whose keys are given by the text versions of the conditions and whose values are
/// given by the parsed versions, and finally return the hashmap.
pub fn get_parsed_structure_conditions(
    config: &ValveConfig,
    parser: &StartParser,
) -> Result<HashMap<String, ParsedStructure>> {
    let mut parsed_structure_conditions = HashMap::new();
    let tables_config = &config.table;
    for (table, table_config) in tables_config.iter() {
        let columns_config = &table_config.column;
        for (column, column_config) in columns_config.iter() {
            let structure = &column_config.structure;
            if structure != "" {
                let parsed_structure = parser.parse(structure);
                if let Err(e) = parsed_structure {
                    return Err(ValveError::ConfigError(format!(
                        "While parsing structure: '{}' for column: '{}.{}' got error:\n{}",
                        structure, table, column, e
                    ))
                    .into());
                }
                let parsed_structure = parsed_structure.unwrap();
                let parsed_structure = &parsed_structure[0];
                let parsed_structure = ParsedStructure {
                    original: structure.to_string(),
                    parsed: *parsed_structure.clone(),
                };
                parsed_structure_conditions.insert(structure.to_string(), parsed_structure);
            }
        }
    }
    Ok(parsed_structure_conditions)
}

/// Given the name of a table and a database connection pool, generate SQL for creating a view
/// based on the table that provides a unified representation of the normal and conflict versions
/// of the table, plus columns summarising the information associated with the given table that is
/// contained in the message and history tables.
pub fn get_sql_for_standard_view(table: &str, pool: &AnyPool) -> String {
    let message_t;
    if pool.any_kind() == AnyKind::Postgres {
        message_t = format!(
            indoc! {r#"
                (
                  SELECT JSON_AGG(m)::TEXT FROM (
                    SELECT "column", "value", "level", "rule", "message"
                    FROM "message"
                    WHERE "table" = '{t}'
                      AND "row" = union_t."row_number"
                    ORDER BY "column", "message_id"
                  ) m
                )
            "#},
            t = table,
        );
    } else {
        message_t = format!(
            indoc! {r#"
                (
                  SELECT NULLIF(
                    JSON_GROUP_ARRAY(
                      JSON_OBJECT(
                        'column', "column",
                        'value', "value",
                        'level', "level",
                        'rule', "rule",
                        'message', "message"
                      )
                    ),
                    '[]'
                  )
                  FROM "message"
                  WHERE "table" = '{t}'
                    AND "row" = union_t."row_number"
                  ORDER BY "column", "message_id"
                )
            "#},
            t = table,
        );
    }

    let history_t;
    if pool.any_kind() == AnyKind::Postgres {
        history_t = format!(
            indoc! {r#"
                (
                  SELECT '[' || STRING_AGG("summary", ',') || ']'
                  FROM (
                    SELECT "summary"
                    FROM "history"
                    WHERE "table" = '{t}'
                      AND "row" = union_t."row_number"
                      AND "summary" IS DISTINCT FROM NULL
                      AND "undone_by" IS NOT DISTINCT FROM NULL
                    ORDER BY "history_id"
                  ) h
                )
            "#},
            t = table,
        );
    } else {
        history_t = format!(
            indoc! {r#"
                (
                  SELECT '[' || GROUP_CONCAT("summary") || ']'
                  FROM (
                    SELECT "summary"
                    FROM "history"
                    WHERE "table" = '{t}'
                      AND "row" = union_t."row_number"
                      AND "summary" IS NOT NULL
                      AND "undone_by" IS NULL
                    ORDER BY "history_id"
                  ) h
                )
            "#},
            t = table,
        );
    }

    let create_view_sql = format!(
        indoc! {r#"
          CREATE VIEW "{t}_view" AS
            SELECT
              union_t.*,
              {message_t} AS "message",
              {history_t} AS "history"
            FROM (
              SELECT * FROM "{t}"
              UNION ALL
              SELECT * FROM "{t}_conflict"
            ) as union_t;
        "#},
        t = table,
        message_t = message_t,
        history_t = history_t,
    );

    create_view_sql
}

/// Given the tables configuration map, the name of a table and a database connection pool,
/// generate SQL for creating a more user-friendly version of the view than the one generated by
/// [get_sql_for_standard_view()]. Unlike the standard view generated by that function, the view
/// generated by this function (called my_table_text_view) always shows all of the values (which are
/// all rendered as text) of every column in the table, even when those values contain SQL datatype
/// errors.
pub fn get_sql_for_text_view(
    tables_config: &HashMap<String, ValveTableConfig>,
    table: &str,
    pool: &AnyPool,
) -> String {
    let is_clause = if pool.any_kind() == AnyKind::Sqlite {
        "IS"
    } else {
        "IS NOT DISTINCT FROM"
    };

    let real_columns = &tables_config
        .get(table)
        .and_then(|t| Some(t.column.keys().map(|k| k.to_string()).collect::<Vec<_>>()))
        .expect(&format!("Undefined table '{}'", table));

    // Add a second "text view" such that the datatypes of all values are TEXT and appear
    // directly in their corresponsing columns (rather than as NULLs) even when they have
    // SQL datatype errors.
    let mut inner_columns = real_columns
        .iter()
        .map(|c| {
            format!(
                r#"CASE
                     WHEN "{column}" {is_clause} NULL THEN (
                       SELECT value
                       FROM "message"
                       WHERE "row" = "row_number"
                         AND "column" = '{column}'
                         AND "table" = '{table}'
                       ORDER BY "message_id" DESC
                       LIMIT 1
                     )
                     ELSE {casted_column}
                   END AS "{column}""#,
                casted_column = if pool.any_kind() == AnyKind::Sqlite {
                    cast_column_sql_to_text(c, "non-text")
                } else {
                    format!("\"{}\"::TEXT", c)
                },
                column = c,
                table = table,
            )
        })
        .collect::<Vec<_>>();

    let mut outer_columns = real_columns
        .iter()
        .map(|c| format!("t.\"{}\"", c))
        .collect::<Vec<_>>();

    let inner_columns = {
        let mut v = vec![
            "row_number".to_string(),
            "message".to_string(),
            "history".to_string(),
        ];
        v.append(&mut inner_columns);
        v
    };

    let outer_columns = {
        let mut v = vec![
            "t.row_number".to_string(),
            "t.message".to_string(),
            "t.history".to_string(),
        ];
        v.append(&mut outer_columns);
        v
    };

    let create_view_sql = format!(
        r#"CREATE VIEW "{table}_text_view" AS
           SELECT {outer_columns}
           FROM (
               SELECT {inner_columns}
               FROM "{table}_view"
           ) t"#,
        outer_columns = outer_columns.join(", "),
        inner_columns = inner_columns.join(", "),
        table = table,
    );

    create_view_sql
}

/// Given a table name, a column name, and a database pool, construct an SQL string to extract the
/// value of the column, such that when the value of a given column is null, the query attempts to
/// extract it from the message table. Returns a String representing the SQL to retrieve the value
/// of the column.
pub fn query_column_with_message_value(table: &str, column: &str, pool: &AnyPool) -> String {
    let is_clause = if pool.any_kind() == AnyKind::Sqlite {
        "IS"
    } else {
        "IS NOT DISTINCT FROM"
    };

    format!(
        r#"CASE
             WHEN "{column}" {is_clause} NULL THEN (
               SELECT value
               FROM "message"
               WHERE "row" = "row_number"
                 AND "column" = '{column}'
                 AND "table" = '{table}'
               ORDER BY "message_id" DESC
               LIMIT 1
             )
             ELSE {casted_column}
           END AS "{column}""#,
        casted_column = if pool.any_kind() == AnyKind::Sqlite {
            cast_column_sql_to_text(column, "non-text")
        } else {
            format!("\"{}\"::TEXT", column)
        },
        column = column,
        table = table,
    )
}

/// Given a table name, a global configuration map, and a database connection pool, construct an
/// SQL query that one can use to get the logical contents of the table, such that when the value
/// of a given column is null, the query attempts to extract it from the message table. Returns a
/// String representing the query.
pub fn query_with_message_values(table: &str, config: &ValveConfig, pool: &AnyPool) -> String {
    let real_columns = config
        .table
        .get(table)
        .expect(&format!("Undefined table '{}'", table))
        .column
        .keys()
        .collect::<Vec<_>>();

    let mut inner_columns = real_columns
        .iter()
        .map(|column| query_column_with_message_value(table, column, pool))
        .collect::<Vec<_>>();

    let mut outer_columns = real_columns
        .iter()
        .map(|c| format!("t.\"{}\"", c))
        .collect::<Vec<_>>();

    let inner_columns = {
        let mut v = vec!["row_number".to_string(), "message".to_string()];
        v.append(&mut inner_columns);
        v
    };

    let outer_columns = {
        let mut v = vec!["t.row_number".to_string(), "t.message".to_string()];
        v.append(&mut outer_columns);
        v
    };

    format!(
        r#"SELECT {outer_columns}
                 FROM (
                   SELECT {inner_columns}
                   FROM "{table}_view"
                 ) t"#,
        outer_columns = outer_columns.join(", "),
        inner_columns = inner_columns.join(", "),
        table = table,
    )
}

/// Given a global config map, a database connection pool, a database transaction, a table name, a
/// column name, and a value for that column: get the rows, other than the one indicated by
/// `except`, that would need to be revalidated if the given value were to replace the actual
/// value of the column in that row.
pub async fn get_affected_rows(
    table: &str,
    column: &str,
    value: &str,
    except: Option<&u32>,
    config: &ValveConfig,
    pool: &AnyPool,
    tx: &mut Transaction<'_, sqlx::Any>,
) -> Result<Vec<ValveRow>> {
    // Since the consequence of an update could involve currently invalid rows
    // (in the conflict table) becoming valid or vice versa, we need to check rows for
    // which the value of the column is the same as `value`
    let sql = {
        format!(
            r#"{main_query} WHERE "{column}" = '{value}'{except}"#,
            main_query = query_with_message_values(table, config, pool),
            column = column,
            value = value,
            except = match except {
                None => "".to_string(),
                Some(row_number) => {
                    format!(" AND row_number != {}", row_number)
                }
            },
        )
    };

    let query = sqlx_query(&sql);
    let mut valve_rows = vec![];
    for row in query.fetch_all(tx.acquire().await?).await? {
        let mut contents = IndexMap::new();
        let mut row_number: Option<u32> = None;
        for column in row.columns() {
            let cname = column.name();
            if cname == "row_number" {
                row_number = Some(row.get::<i64, _>("row_number") as u32);
            } else if cname != "message" {
                let raw_value = row.try_get_raw(format!(r#"{}"#, cname).as_str()).unwrap();
                let value;
                if !raw_value.is_null() {
                    value = get_column_value(&row, &cname, "text");
                } else {
                    value = String::from("");
                }
                contents.insert(cname.to_string(), ValveCell::new(&json!(value)));
            }
        }
        let row_number =
            row_number.ok_or(ValveError::DataError("Row: has no row number".to_string()))?;
        valve_rows.push(ValveRow {
            row_number: Some(row_number),
            contents: contents,
        });
    }

    Ok(valve_rows)
}

/// Given a global configuration map, a database connection pool, a database transaction, a table
/// name and a row number, get the logical contents of that row (whether or not it is valid),
/// including any messages, from the database.
pub async fn get_row_from_db(
    config: &ValveConfig,
    pool: &AnyPool,
    tx: &mut Transaction<'_, sqlx::Any>,
    table: &str,
    row_number: &u32,
) -> Result<SerdeMap> {
    let sql = format!(
        "{} WHERE row_number = {}",
        query_with_message_values(table, config, pool),
        row_number
    );
    let query = sqlx_query(&sql);
    let rows = query.fetch_all(tx.acquire().await?).await?;
    if rows.len() == 0 {
        return Err(ValveError::DataError(
            format!(
                "In get_row_from_db(). No rows found for row_number: {}",
                row_number
            )
            .into(),
        )
        .into());
    }
    let sql_row = &rows[0];

    let messages = {
        let raw_messages = sql_row.try_get_raw("message")?;
        if raw_messages.is_null() {
            vec![]
        } else {
            let messages: &str = sql_row.get("message");
            match serde_json::from_str::<SerdeValue>(messages) {
                Err(e) => return Err(e.into()),
                Ok(SerdeValue::Array(m)) => m,
                _ => {
                    return Err(ValveError::DataError(
                        format!("{} is not an array.", messages).into(),
                    )
                    .into())
                }
            }
        }
    };

    let mut row = SerdeMap::new();
    for column in sql_row.columns() {
        let cname = column.name();
        if !vec!["row_number", "message"].contains(&cname) {
            let raw_value = sql_row.try_get_raw(format!(r#"{}"#, cname).as_str())?;
            let value;
            if !raw_value.is_null() {
                // The extended query returned by query_with_message_values() casts all column
                // values to text, so we pass "text" to get_column_value() for every column:
                value = get_column_value(&sql_row, &cname, "text");
            } else {
                value = String::from("");
            }
            let column_messages = messages
                .iter()
                .filter(|m| m.get("column").unwrap().as_str() == Some(cname))
                .collect::<Vec<_>>();
            let valid = column_messages
                .iter()
                .filter(|m| m.get("level").unwrap().as_str() == Some("error"))
                .collect::<Vec<_>>()
                .is_empty();
            let cell = json!({
                "value": value,
                "valid": valid,
                "messages": column_messages,
            });
            row.insert(cname.to_string(), json!(cell));
        }
    }
    Ok(row)
}

/// Given a database connection pool, a database transaction, a table name, a column name, and a row
/// number, get the current value of the given column in the database.
pub async fn get_db_value(
    table: &str,
    column: &str,
    row_number: &u32,
    pool: &AnyPool,
    tx: &mut Transaction<'_, sqlx::Any>,
) -> Result<String> {
    let is_clause = if pool.any_kind() == AnyKind::Sqlite {
        "IS"
    } else {
        "IS NOT DISTINCT FROM"
    };
    let sql = format!(
        r#"SELECT
                 CASE
                   WHEN "{column}" {is_clause} NULL THEN (
                     SELECT value
                     FROM "message"
                     WHERE "row" = "row_number"
                       AND "column" = '{column}'
                       AND "table" = '{table}'
                     ORDER BY "message_id" DESC
                     LIMIT 1
                   )
                   ELSE {casted_column}
                 END AS "{column}"
               FROM "{table}_view" WHERE "row_number" = {row_number}
            "#,
        column = column,
        is_clause = is_clause,
        table = table,
        row_number = row_number,
        casted_column = if pool.any_kind() == AnyKind::Sqlite {
            cast_column_sql_to_text(column, "non-text")
        } else {
            format!("\"{}\"::TEXT", column)
        },
    );

    let query = sqlx_query(&sql);
    let rows = query.fetch_all(tx.acquire().await?).await?;
    if rows.len() == 0 {
        return Err(ValveError::DataError(
            format!(
                "In get_db_value(). No rows found for row_number: {}",
                row_number
            )
            .into(),
        )
        .into());
    }
    let result_row = &rows[0];
    let value: &str = result_row.try_get(column).unwrap();
    Ok(value.to_string())
}

/// Given a global config map, a database connection pool, a database transaction, a table name,
/// and a [QueryAsIf] struct representing a custom modification to the query of the table, get
/// the rows that will potentially be affected by the database change to the row indicated in
/// query_as_if. These are divided into three: The rows that must be updated before the current
/// update, the rows that must be updated after the current update, and the rows from the same
/// table as the current update that need to be updated.
pub async fn get_rows_to_update(
    config: &ValveConfig,
    pool: &AnyPool,
    tx: &mut Transaction<'_, sqlx::Any>,
    table: &str,
    query_as_if: &QueryAsIf,
) -> Result<(
    IndexMap<String, Vec<ValveRow>>,
    IndexMap<String, Vec<ValveRow>>,
    IndexMap<String, Vec<ValveRow>>,
)> {
    fn get_cell_value(row: &ValveRow, column: &str) -> Result<String> {
        row.contents
            .get(column)
            .and_then(|cell| Some(cell.strvalue()))
            .ok_or(
                ValveError::InputError(format!(
                    "Value missing or of unknown type in column {} of row to update: {:?}",
                    column, row
                ))
                .into(),
            )
    }

    // Collect foreign key dependencies:
    let foreign_dependencies = {
        let mut foreign_dependencies = vec![];
        let global_fconstraints = &config.constraint.foreign;
        for (_, fconstraints) in global_fconstraints {
            for entry in fconstraints {
                if entry.ftable == *table {
                    foreign_dependencies.push(entry);
                }
            }
        }
        foreign_dependencies
    };

    let mut rows_to_update_before = IndexMap::new();
    let mut rows_to_update_after = IndexMap::new();
    for fdep in &foreign_dependencies {
        let dependent_table = &fdep.table;
        let dependent_column = &fdep.column;
        let target_column = &fdep.fcolumn;
        let target_table = &fdep.ftable;

        // Query the database using `row_number` to get the current value of the column for
        // the row.
        let updates_before = match query_as_if.kind {
            QueryAsIfKind::Add => {
                if let None = query_as_if.row {
                    log::warn!(
                        "No row in query_as_if: {:?} for {:?}",
                        query_as_if,
                        query_as_if.kind
                    );
                }
                vec![]
            }
            _ => {
                let current_value = get_db_value(
                    target_table,
                    target_column,
                    &query_as_if.row_number,
                    pool,
                    tx,
                )
                .await?;

                // Query dependent_table.dependent_column for the rows that will be affected by the
                // change from the current value:
                get_affected_rows(
                    dependent_table,
                    dependent_column,
                    &current_value,
                    None,
                    config,
                    pool,
                    tx,
                )
                .await?
            }
        };

        let updates_after = match &query_as_if.row {
            None => {
                if query_as_if.kind != QueryAsIfKind::Remove {
                    log::warn!(
                        "No row in query_as_if: {:?} for {:?}",
                        query_as_if,
                        query_as_if.kind
                    );
                }
                vec![]
            }
            Some(row) => {
                // Fetch the cell corresponding to `column` from `row`, and the value of that cell,
                // which is the new value for the row.
                let new_value = get_cell_value(&row, target_column)?;
                get_affected_rows(
                    dependent_table,
                    dependent_column,
                    &new_value,
                    None,
                    config,
                    pool,
                    tx,
                )
                .await?
            }
        };
        rows_to_update_before.insert(dependent_table.to_string(), updates_before);
        rows_to_update_after.insert(dependent_table.to_string(), updates_after);
    }

    // Collect the intra-table dependencies:
    // TODO: Consider also the tree intra-table dependencies.
    let primaries = config
        .constraint
        .primary
        .get(table)
        .expect(&format!("Undefined table '{}'", table));
    let uniques = config
        .constraint
        .unique
        .get(table)
        .expect(&format!("Undefined table '{}'", table));
    let columns = config
        .table
        .get(table)
        .expect(&format!("Undefined table '{}'", table))
        .column
        .keys()
        .map(|k| k.to_string())
        .collect::<Vec<_>>();

    let mut rows_to_update_intra = IndexMap::new();
    for column in &columns {
        if !uniques.contains(column) && !primaries.contains(column) {
            continue;
        }

        // Query the database using `row_number` to get the current value of the column for
        // the row. We only look for rows to update that match the current value of the column.
        // Rows matching the column's new value don't also need to be updated. Those will result
        // in a validation error for the new/modified row but that is fine.
        let updates = match query_as_if.kind {
            QueryAsIfKind::Add => {
                if let None = query_as_if.row {
                    log::warn!(
                        "No row in query_as_if: {:?} for {:?}",
                        query_as_if,
                        query_as_if.kind
                    );
                }
                vec![]
            }
            _ => {
                let current_value =
                    get_db_value(table, column, &query_as_if.row_number, pool, tx).await?;

                // Query table.column for the rows that will be affected by the change from the
                // current to the new value:
                get_affected_rows(
                    table,
                    column,
                    &current_value,
                    Some(&query_as_if.row_number),
                    config,
                    pool,
                    tx,
                )
                .await?
            }
        };
        rows_to_update_intra.insert(table.to_string(), updates);
    }

    // TODO: Collect the dependencies for under constraints similarly to the way we
    // collect foreign constraints (see just above).

    Ok((
        rows_to_update_before,
        rows_to_update_after,
        rows_to_update_intra,
    ))
}

/// Given a global config map, maps of datatype and rule conditions, a database connection pool,
/// a database transaction, some updates to process, a [QueryAsIf] struct indicating how
/// we should counterfactually modify the current state of the database, and a flag indicating
/// whether we should allow recursive updates, validate and then update each row indicated in
/// `updates`.
pub async fn process_updates(
    config: &ValveConfig,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
    tx: &mut Transaction<'_, sqlx::Any>,
    updates: &IndexMap<String, Vec<ValveRow>>,
    query_as_if: &QueryAsIf,
    do_not_recurse: bool,
) -> Result<()> {
    for (update_table, rows_to_update) in updates {
        for row in rows_to_update {
            let row_number = row
                .row_number
                .ok_or(ValveError::InputError("Row has no row number".to_string()))?;
            // Validate each row 'counterfactually':
            let vrow = validate_row_tx(
                config,
                compiled_datatype_conditions,
                compiled_rule_conditions,
                pool,
                Some(tx),
                update_table,
                row,
                Some(row_number),
                Some(&query_as_if),
            )
            .await?;

            // Update the row in the database:
            update_row_tx(
                config,
                compiled_datatype_conditions,
                compiled_rule_conditions,
                pool,
                tx,
                update_table,
                &vrow,
                &row_number,
                false,
                do_not_recurse,
            )
            .await?;
        }
    }
    Ok(())
}

/// Given a database transaction, a table name, a row number, optionally: the version of the row we
/// are going to change it from, optionally: the version of the row we are going to change it to,
/// and the name of the user making the change, record the change to the history table in the
/// database. Note that `from` and `to` cannot both be None: Either we are changing the row from
/// something to nothing (i.e., deleting), changing it from nothing to something (i.e., inserting),
/// or changing it from something to something else (i.e., updating).
/// Note that `from` and `to` must both be in the ('rich') format:
/// ```
/// {
///     "column_1": {
///         "valid": <true|false>,
///         "messages": [{"level": level, "rule": rule, "message": message}, ...],
///         "value": value1
///     },
///     "column_2": {
///         "valid": <true|false>,
///         "messages": [{"level": level, "rule": rule, "message": message}, ...],
///         "value": value2
///     },
///     ...
/// },
/// ```
pub async fn record_row_change(
    tx: &mut Transaction<'_, sqlx::Any>,
    table: &str,
    row_number: &u32,
    from: Option<&SerdeMap>,
    to: Option<&SerdeMap>,
    user: &str,
) -> Result<()> {
    if let (None, None) = (from, to) {
        return Err(ValveError::InputError(
            "Arguments 'from' and 'to' to function record_row_change() cannot both be None".into(),
        )
        .into());
    }

    fn to_text(row: Option<&SerdeMap>, quoted: bool) -> String {
        match row {
            None => "NULL".to_string(),
            Some(r) => {
                let inner = format!("{}", json!(r)).replace("'", "''");
                if !quoted {
                    inner
                } else {
                    format!("'{}'", inner)
                }
            }
        }
    }

    fn format_value(value: &String, numeric_re: &Regex) -> String {
        if numeric_re.is_match(value) {
            value.to_string()
        } else {
            format!("'{}'", value)
        }
    }

    fn summarize(from: Option<&SerdeMap>, to: Option<&SerdeMap>) -> Result<String> {
        // Constructs a summary of the form:
        // {
        //   "column":"bar",
        //   "level":"update",
        //   "message":"Value changed from 'A' to 'B'",
        //   "old_value":"'A'",
        //   "value":"'B'"
        // }
        let mut summary = vec![];
        match (from, to) {
            (None, _) | (_, None) => Ok("NULL".to_string()),
            (Some(from), Some(to)) => {
                let numeric_re = Regex::new(r"^[0-9]*\.?[0-9]+$")?;
                for (column, cell) in from.iter() {
                    let old_value = cell
                        .get("value")
                        .and_then(|v| match v {
                            SerdeValue::String(s) => Some(format!("{}", s)),
                            SerdeValue::Number(n) => Some(format!("{}", n)),
                            SerdeValue::Bool(b) => Some(format!("{}", b)),
                            _ => None,
                        })
                        .ok_or(ValveError::DataError(
                            format!("No value in {}", cell).into(),
                        ))?;
                    let new_value = to
                        .get(column)
                        .and_then(|v| v.get("value"))
                        .and_then(|v| match v {
                            SerdeValue::String(s) => Some(format!("{}", s)),
                            SerdeValue::Number(n) => Some(format!("{}", n)),
                            SerdeValue::Bool(b) => Some(format!("{}", b)),
                            _ => None,
                        })
                        .ok_or(ValveError::DataError(
                            format!("No value for column: {} in {:?}", column, to).into(),
                        ))?;
                    if new_value != old_value {
                        let mut column_summary = SerdeMap::new();
                        column_summary.insert("column".to_string(), json!(column));
                        column_summary.insert("level".to_string(), json!("update"));
                        column_summary.insert("old_value".to_string(), json!(old_value));
                        column_summary.insert("value".to_string(), json!(new_value));
                        column_summary.insert(
                            "message".to_string(),
                            json!(format!(
                                "Value changed from {} to {}",
                                format_value(&old_value.to_string(), &numeric_re),
                                format_value(&new_value.to_string(), &numeric_re),
                            )),
                        );
                        let column_summary = to_text(Some(&column_summary), false);
                        summary.push(column_summary);
                    }
                }
                Ok(format!("'[{}]'", summary.join(",")))
            }
        }
    }

    let summary = summarize(from, to)?;
    let (from, to) = (to_text(from, true), to_text(to, true));
    let sql = format!(
        r#"INSERT INTO "history" ("table", "row", "from", "to", "summary", "user")
           VALUES ('{}', {}, {}, {}, {}, '{}')"#,
        table, row_number, from, to, summary, user
    );
    let query = sqlx_query(&sql);
    query.execute(tx.acquire().await?).await?;

    Ok(())
}

/// Given a database record representing either an undo or a redo from the history table,
/// convert it to a vector of [ValveChange] structs.
pub fn convert_undo_or_redo_record_to_change(record: &AnyRow) -> Result<Option<ValveRowChange>> {
    let table: &str = record.get("table");
    let row_number: i64 = record.get("row");
    let row_number = row_number as u32;
    let from = get_json_from_row(&record, "from");
    let to = get_json_from_row(&record, "to");
    let summary = {
        let summary = record.try_get_raw("summary")?;
        if !summary.is_null() {
            let summary: &str = record.get("summary");
            match serde_json::from_str::<SerdeValue>(summary) {
                Ok(SerdeValue::Array(v)) => Some(v),
                _ => panic!("{} is not an array.", summary),
            }
        } else {
            None
        }
    };

    // If the `summary` part of the record is present, then just use it to populate the fields
    // of the ValveChange structs representing the changes to each column in the table row.
    if let Some(summary) = summary {
        let mut column_changes = vec![];
        for entry in summary {
            let column = entry.get("column").and_then(|s| s.as_str()).unwrap();
            let level = entry.get("level").and_then(|s| s.as_str()).unwrap();
            let old_value = entry.get("old_value").and_then(|s| s.as_str()).unwrap();
            let value = entry.get("value").and_then(|s| s.as_str()).unwrap();
            let message = entry.get("message").and_then(|s| s.as_str()).unwrap();
            column_changes.push(ValveChange {
                column: column.to_string(),
                level: level.to_string(),
                old_value: old_value.to_string(),
                value: value.to_string(),
                message: message.to_string(),
            });
        }
        return Ok(Some(ValveRowChange {
            table: table.to_string(),
            row: row_number,
            message: format!("Update row {row_number} of '{table}'"),
            changes: column_changes,
        }));
    }

    // If the `summary` part of the record is not present, then either the `from` or the `to`
    // parts of the record will be null. If `from` is not null then this is a delete (i.e.,
    // the row has changed from something to nothing).
    if let Some(from) = from {
        let mut column_changes = vec![];
        for (column, change) in from {
            let old_value = change.get("value").and_then(|s| s.as_str()).unwrap();
            column_changes.push(ValveChange {
                column: column.to_string(),
                level: "delete".to_string(),
                old_value: old_value.to_string(),
                value: "".to_string(),
                message: "".to_string(),
            });
        }
        return Ok(Some(ValveRowChange {
            table: table.to_string(),
            row: row_number,
            message: format!("Delete row {} from '{}'", row_number, table),
            changes: column_changes,
        }));
    }

    // If `to` is not null then this is an insert (i.e., the row has changed from nothing to
    // something).
    if let Some(to) = to {
        let mut column_changes = vec![];
        for (column, change) in to {
            let value = change.get("value").and_then(|s| s.as_str()).unwrap();
            column_changes.push(ValveChange {
                column: column.to_string(),
                level: "insert".to_string(),
                old_value: "".to_string(),
                value: value.to_string(),
                message: "".to_string(),
            });
        }
        return Ok(Some(ValveRowChange {
            table: table.to_string(),
            row: row_number,
            message: format!("Add row {} to '{}'", row_number, table),
            changes: column_changes,
        }));
    }

    // If we get to here, then `summary`, `from`, and `to` are all null so we return an error.
    Err(ValveError::InputError(
        "All of summary, from, and to are NULL in undo/redo record".to_string(),
    )
    .into())
}

/// Return the next recorded change to the data that can be undone, or None if there isn't any.
pub async fn get_record_to_undo(pool: &AnyPool) -> Result<Option<AnyRow>> {
    // Look in the history table, get the row with the greatest ID, get the row number,
    // from, and to, and determine whether the last operation was a delete, insert, or update.
    let is_clause = if pool.any_kind() == AnyKind::Sqlite {
        "IS"
    } else {
        "IS NOT DISTINCT FROM"
    };
    let sql = format!(
        r#"SELECT * FROM "history"
               WHERE "undone_by" {} NULL
               ORDER BY "history_id" DESC LIMIT 1"#,
        is_clause
    );
    let query = sqlx_query(&sql);
    let result_row = query.fetch_optional(pool).await?;
    Ok(result_row)
}

/// Return the next recorded change to the data that can be redone, or None if there isn't any.
pub async fn get_record_to_redo(pool: &AnyPool) -> Result<Option<AnyRow>> {
    // Look in the history table, get the row with the greatest ID, get the row number,
    // from, and to, and determine whether the last operation was a delete, insert, or update.
    let is_not_clause = if pool.any_kind() == AnyKind::Sqlite {
        "IS NOT"
    } else {
        "IS DISTINCT FROM"
    };
    let sql = format!(
        r#"SELECT * FROM "history"
           WHERE "undone_by" {} NULL
           ORDER BY "timestamp" DESC LIMIT 1"#,
        is_not_clause
    );
    let query = sqlx_query(&sql);
    let result_row = query.fetch_optional(pool).await?;
    Ok(result_row)
}

/// Given a row and a column name, extract the contents of the row as a JSON object and return it.
pub fn get_json_from_row(row: &AnyRow, column: &str) -> Option<SerdeMap> {
    let raw_value = row
        .try_get_raw(column)
        .expect("Unable to get raw value from row");
    if !raw_value.is_null() {
        let value: &str = row.get(column);
        match serde_json::from_str::<SerdeValue>(value) {
            Err(e) => {
                log::warn!("{}", e);
                None
            }
            Ok(SerdeValue::Object(value)) => Some(value),
            _ => {
                log::warn!("{} is not an object.", value);
                None
            }
        }
    } else {
        None
    }
}

/// Given a user, a history_id, a database transaction, and an undone_state indicating whether to
/// set the associated history record as undone (if undone_state == true) or as not undone
/// (otherwise), do the following: When setting the record to undone, `user` is used for the
/// 'undone_by' field of the history table, otherwise undone_by is set to NULL and the user is
/// indicated as the one responsible for the change (instead of whoever made the change originally).
pub async fn switch_undone_state(
    user: &str,
    history_id: u16,
    undone_state: bool,
    tx: &mut Transaction<'_, sqlx::Any>,
    pool: &AnyPool,
) -> Result<()> {
    // Set the history record to undone:
    let timestamp = {
        if pool.any_kind() == AnyKind::Sqlite {
            "STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')"
        } else {
            "CURRENT_TIMESTAMP"
        }
    };
    let undone_by = if undone_state == true {
        format!(r#""undone_by" = '{}', "timestamp" = {}"#, user, timestamp)
    } else {
        format!(
            r#""undone_by" = NULL, "user" = '{}', "timestamp" = {}"#,
            user, timestamp
        )
    };
    let sql = format!(
        r#"UPDATE "history" SET {} WHERE "history_id" = {}"#,
        undone_by, history_id
    );
    let query = sqlx_query(&sql);
    query.execute(tx.acquire().await?).await?;
    Ok(())
}

/// Given a global config map and a table name, return a list of the columns from the table
/// that may potentially result in database conflicts.
pub fn get_conflict_columns(config: &ValveConfig, table_name: &str) -> Vec<String> {
    let mut conflict_columns = vec![];
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
    // We take tree-children because these imply a unique database constraint on the corresponding
    // column.
    let tree_children = config
        .constraint
        .tree
        .get(table_name)
        .expect(&format!("Undefined table '{}'", table_name))
        .iter()
        .map(|t| t.child.to_string())
        .collect::<Vec<_>>();
    let foreign_sources = config
        .constraint
        .foreign
        .get(table_name)
        .expect(&format!("Undefined table '{}'", table_name))
        .iter()
        .map(|t| t.column.to_string())
        .collect::<Vec<_>>();
    let foreign_targets = config
        .constraint
        .foreign
        .get(table_name)
        .expect(&format!("Undefined table '{}'", table_name))
        .iter()
        .filter(|t| t.ftable == *table_name)
        .map(|t| t.fcolumn.to_string())
        .collect::<Vec<_>>();

    for key_columns in vec![
        primaries,
        uniques,
        &tree_children,
        &foreign_sources,
        &foreign_targets,
    ] {
        for column in key_columns {
            if !conflict_columns.contains(column) {
                conflict_columns.push(column.to_string());
            }
        }
    }

    conflict_columns
}

/// Given a SQL type and a value, return true if the value does not conform to the SQL type.
pub fn is_sql_type_error(sql_type: &str, value: &str) -> bool {
    let sql_type = sql_type.to_lowercase();
    if sql_type == "numeric" {
        // f64
        let numeric_value: Result<f64, std::num::ParseFloatError> = value.parse();
        match numeric_value {
            Ok(_) => false,
            Err(_) => true,
        }
    } else if sql_type == "integer" {
        // i32
        let integer_value: Result<i32, std::num::ParseIntError> = value.parse();
        match integer_value {
            Ok(_) => false,
            Err(_) => true,
        }
    } else if sql_type == "real" {
        // f64 (actually f32)
        let float_value: Result<f64, std::num::ParseFloatError> = value.parse();
        match float_value {
            Ok(_) => false,
            Err(_) => true,
        }
    } else {
        false
    }
}

/// Given a global config struct, compiled datatype and rule conditions, a database connection pool,
/// a database transaction, a table name, a row represented as a [ValveRow],
/// insert it to the database using the given transaction, then return the new row number. If
/// the optional parameter `new_row_number` is given, then that is the row number used when
/// creating the row, and is the number returned. Otherwise the row number inserted is determined
/// automatically by Valve. If skip_validation is set to true, omit the implicit call to
/// [validate_row_tx()].
#[async_recursion]
pub async fn insert_new_row_tx(
    config: &ValveConfig,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
    tx: &mut Transaction<sqlx::Any>,
    table: &str,
    row: &ValveRow,
    new_row_number: Option<u32>,
    skip_validation: bool,
) -> Result<u32> {
    // Send the row through the row validator to determine if any fields are problematic and
    // to mark them with appropriate messages:
    let row = if !skip_validation {
        validate_row_tx(
            config,
            compiled_datatype_conditions,
            compiled_rule_conditions,
            pool,
            Some(tx),
            table,
            row,
            new_row_number,
            None,
        )
        .await?
    } else {
        row.clone()
    };

    // Now prepare the row and messages for insertion to the database.
    let new_row_number = match new_row_number {
        Some(n) => n,
        None => {
            let sql = format!(
                r#"SELECT MAX("row_number") AS "row_number" FROM (
                     SELECT MAX("row_number") AS "row_number"
                       FROM "{table}_view"
                     UNION ALL
                      SELECT MAX("row") AS "row_number"
                        FROM "history"
                       WHERE "table" = '{table}'
                   ) t"#,
                table = table
            );
            let query = sqlx_query(&sql);
            let result_rows = query.fetch_all(tx.acquire().await?).await?;
            let new_row_number: i64;
            if result_rows.len() == 0 {
                new_row_number = 1;
            } else {
                let result_row = &result_rows[0];
                let result = result_row.try_get_raw("row_number")?;
                if result.is_null() {
                    new_row_number = 1;
                } else {
                    new_row_number = result_row.get("row_number");
                }
            }
            let new_row_number = new_row_number as u32 + 1;
            new_row_number
        }
    };

    let mut insert_columns = vec![];
    let mut insert_values = vec![];
    let mut insert_params = vec![];
    let mut all_messages = vec![];
    let sorted_datatypes = get_sorted_datatypes(config);
    let conflict_columns = get_conflict_columns(config, table);
    let mut use_conflict_table = false;
    for (column, cell) in row.contents.iter() {
        insert_columns.append(&mut vec![format!(r#""{}""#, column)]);
        let messages = sort_messages(&sorted_datatypes, &cell.messages);
        for message in messages {
            all_messages.push(json!({
                "column": column,
                "value": &cell.value,
                "level": message.level,
                "rule": message.rule,
                "message": message.message,
            }));
        }

        let sql_type = get_sql_type_from_global_config(config, table, column, pool);
        if is_sql_type_error(&sql_type, &cell.strvalue()) {
            insert_values.push(String::from("NULL"));
        } else {
            insert_values.push(cast_sql_param_from_text(&sql_type));
            insert_params.push(String::from(cell.strvalue()));
        }

        if !use_conflict_table && !cell.valid && conflict_columns.contains(&column) {
            use_conflict_table = true;
        }
    }

    // Used to validate the given row, counterfactually, "as if" the version of the row in the
    // database currently were replaced with `row`:
    let query_as_if = QueryAsIf {
        kind: QueryAsIfKind::Add,
        table: table.to_string(),
        alias: format!("{}_as_if", table),
        row_number: new_row_number,
        row: Some(row.clone()),
    };

    // Look through the valve config to see which tables are dependent on this table
    // and find the rows that need to be updated:
    let (_, updates_after, _) = get_rows_to_update(config, pool, tx, table, &query_as_if).await?;

    // Check it to see if the row should be redirected to the conflict table:
    let table_to_write = {
        if use_conflict_table {
            format!("{}_conflict", table)
        } else {
            String::from(table)
        }
    };

    // Add the new row to the table:
    let insert_stmt = local_sql_syntax(
        &pool,
        &format!(
            r#"INSERT INTO "{}" ("row_number", {}) VALUES ({}, {})"#,
            table_to_write,
            insert_columns.join(", "),
            new_row_number,
            insert_values.join(", "),
        ),
    );
    let mut query = sqlx_query(&insert_stmt);
    for param in &insert_params {
        query = query.bind(param);
    }
    query.execute(tx.acquire().await?).await?;

    // Next add any validation messages to the message table:
    for m in all_messages {
        let column = m.get("column").and_then(|c| c.as_str()).unwrap();
        let value = match m.get("value") {
            Some(SerdeValue::String(s)) => s.to_string(),
            Some(v) => format!("{}", v),
            _ => panic!("Message '{}' has no value", m),
        };
        let level = m.get("level").and_then(|c| c.as_str()).unwrap();
        let rule = m.get("rule").and_then(|c| c.as_str()).unwrap();
        let message = m.get("message").and_then(|c| c.as_str()).unwrap();
        let message = message.replace("'", "''");
        let message_sql = format!(
            r#"INSERT INTO "message"
               ("table", "row", "column", "value", "level", "rule", "message")
               VALUES ('{}', {}, '{}', '{}', '{}', '{}', '{}')"#,
            table, new_row_number, column, value, level, rule, message
        );
        let query = sqlx_query(&message_sql);
        query.execute(tx.acquire().await?).await?;
    }

    // Now process the updates that need to be performed after the update of the target row:
    process_updates(
        config,
        compiled_datatype_conditions,
        compiled_rule_conditions,
        pool,
        tx,
        &updates_after,
        &query_as_if,
        false,
    )
    .await?;

    Ok(new_row_number)
}

/// Given a global config map, maps of datatype and rule conditions, a database connection pool, a
/// database transaction, a table name, and a row number, delete the given row from the database.
#[async_recursion]
pub async fn delete_row_tx(
    config: &ValveConfig,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
    tx: &mut Transaction<sqlx::Any>,
    table: &str,
    row_number: &u32,
) -> Result<()> {
    // Used to validate the given row, counterfactually, "as if" the row did not exist in the
    // database:
    let query_as_if = QueryAsIf {
        kind: QueryAsIfKind::Remove,
        table: table.to_string(),
        alias: format!("{}_as_if", table),
        row_number: *row_number,
        row: None,
    };

    // Look through the valve config to see which tables are dependent on this table and find the
    // rows that need to be updated. Since this is a delete there will only be rows to update
    // before and none after the delete:
    let (updates_before, _, updates_intra) =
        get_rows_to_update(config, pool, tx, table, &query_as_if).await?;

    // Process the updates that need to be performed before the update of the target row:
    process_updates(
        config,
        compiled_datatype_conditions,
        compiled_rule_conditions,
        pool,
        tx,
        &updates_before,
        &query_as_if,
        false,
    )
    .await?;

    // Now delete the row:
    let sql1 = format!(
        "DELETE FROM \"{}\" WHERE row_number = {}",
        table, row_number,
    );
    let sql2 = format!(
        "DELETE FROM \"{}_conflict\" WHERE row_number = {}",
        table, row_number
    );
    for sql in vec![sql1, sql2] {
        let query = sqlx_query(&sql);
        query.execute(tx.acquire().await?).await?;
    }

    let sql = format!(
        r#"DELETE FROM "message" WHERE "table" = '{}' AND "row" = {}"#,
        table, row_number
    );
    let query = sqlx_query(&sql);
    query.execute(tx.acquire().await?).await?;

    // Finally process the rows from the same table as the target table that need to be re-validated
    // because of unique or primary constraints:
    process_updates(
        config,
        compiled_datatype_conditions,
        compiled_rule_conditions,
        pool,
        tx,
        &updates_intra,
        &query_as_if,
        true,
    )
    .await?;

    Ok(())
}

/// Given global config struct, maps of compiled datatype and rule conditions, a database connection
/// pool, a database transaction, a table name, a row represented as a [ValveRow],
/// and the row number to update, update the corresponding row in the database. If skip_validation
/// is set, skip the implicit call to [validate_row_tx()]. If do_not_recurse is set, do not look for
/// rows which could be affected by this update.
#[async_recursion]
pub async fn update_row_tx(
    config: &ValveConfig,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
    tx: &mut Transaction<sqlx::Any>,
    table: &str,
    row: &ValveRow,
    row_number: &u32,
    skip_validation: bool,
    do_not_recurse: bool,
) -> Result<()> {
    // First, look through the valve config to see which tables are dependent on this table and find
    // the rows that need to be updated. The variable query_as_if is used to validate the given row,
    // counterfactually, "as if" the version of the row in the database currently were replaced with
    // `row`:
    let query_as_if = QueryAsIf {
        kind: QueryAsIfKind::Replace,
        table: table.to_string(),
        alias: format!("{}_as_if", table),
        row_number: *row_number,
        row: Some(row.clone()),
    };
    let (updates_before, updates_after, updates_intra) = {
        if do_not_recurse {
            (IndexMap::new(), IndexMap::new(), IndexMap::new())
        } else {
            get_rows_to_update(config, pool, tx, table, &query_as_if).await?
        }
    };

    // Process the updates that need to be performed before the update of the target row:
    process_updates(
        config,
        compiled_datatype_conditions,
        compiled_rule_conditions,
        pool,
        tx,
        &updates_before,
        &query_as_if,
        false,
    )
    .await?;

    // Send the row through the row validator to determine if any fields are problematic and
    // to mark them with appropriate messages:
    let row = if !skip_validation {
        validate_row_tx(
            config,
            compiled_datatype_conditions,
            compiled_rule_conditions,
            pool,
            Some(tx),
            table,
            row,
            Some(*row_number),
            None,
        )
        .await?
    } else {
        row.clone()
    };

    // Perform the update in two steps:
    delete_row_tx(
        config,
        compiled_datatype_conditions,
        compiled_rule_conditions,
        pool,
        tx,
        table,
        row_number,
    )
    .await?;
    insert_new_row_tx(
        config,
        compiled_datatype_conditions,
        compiled_rule_conditions,
        pool,
        tx,
        table,
        &row,
        Some(*row_number),
        false,
    )
    .await?;

    // Now process the rows from the same table as the target table that need to be re-validated
    // because of unique or primary constraints:
    process_updates(
        config,
        compiled_datatype_conditions,
        compiled_rule_conditions,
        pool,
        tx,
        &updates_intra,
        &query_as_if,
        true,
    )
    .await?;

    // Finally process the updates from other tables that need to be performed after the update of
    // the target row:
    process_updates(
        config,
        compiled_datatype_conditions,
        compiled_rule_conditions,
        pool,
        tx,
        &updates_after,
        &query_as_if,
        false,
    )
    .await?;

    Ok(())
}

/// Given a path, read a TSV file and return a vector of rows represented as [SerdeMaps](SerdeMap).
/// Note: Use this function to read "small" TSVs only. In particular, use this for the special
/// configuration tables.
pub fn read_tsv_into_vector(path: &str) -> Result<Vec<SerdeMap>> {
    let mut rdr =
        ReaderBuilder::new()
            .delimiter(b'\t')
            .from_reader(File::open(path).map_err(|err| {
                ValveError::ConfigError(format!("Unable to open '{}': {}", path, err))
            })?);

    let mut rows: Vec<SerdeMap> = vec![];
    for result in rdr.deserialize() {
        match result {
            Err(e) => {
                return Err(ValveError::InputError(format!("Error reading {}: {}", path, e)).into())
            }
            Ok(row) => rows.push(row),
        }
    }

    if rows.len() < 1 {
        return Err(ValveError::DataError(format!("No rows in {}", path)).into());
    }

    for (i, row) in rows.iter().enumerate() {
        // enumerate() begins at 0 but we want to count rows from 1:
        let i = i + 1;
        for (col, val) in row {
            let val = val.as_str().unwrap();
            let trimmed_val = val.trim();
            if trimmed_val != val {
                return Err(ValveError::DataError(format!(
                    "Value '{}' of column '{}' in row {} of table '{}' {}",
                    val, col, i, path, "has leading and/or trailing whitespace."
                ))
                .into());
            }
        }
    }

    Ok(rows)
}

/// Given a database at the specified location, query the given table and return a vector of rows
/// represented as [SerdeMaps](SerdeMap).
pub fn read_db_table_into_vector(pool: &AnyPool, config_table: &str) -> Result<Vec<SerdeMap>> {
    let sql = format!("SELECT * FROM \"{}\"", config_table);
    let rows = block_on(sqlx_query(&sql).fetch_all(pool)).map_err(|e| {
        ValveError::InputError(format!(
            "Error while reading table '{}' from database: {}",
            config_table, e
        ))
    })?;
    let mut table_rows = vec![];
    for row in rows {
        let mut table_row = SerdeMap::new();
        for column in row.columns() {
            let cname = column.name();
            if cname != "row_number" {
                let raw_value = row.try_get_raw(format!(r#"{}"#, cname).as_str()).unwrap();
                if !raw_value.is_null() {
                    let value = get_column_value(&row, &cname, "text");
                    table_row.insert(cname.to_string(), json!(value));
                } else {
                    table_row.insert(cname.to_string(), json!(""));
                }
            }
        }
        table_rows.push(table_row);
    }
    Ok(table_rows)
}

/// Given a condition on a datatype, if the condition is a Function, then parse it using
/// StartParser, create a corresponding CompiledCondition, and return it. If the condition is a
/// Label, then look for the CompiledCondition corresponding to it in compiled_datatype_conditions
/// and return it.
pub fn compile_condition(
    condition: &str,
    parser: &StartParser,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
) -> Result<CompiledCondition> {
    if condition == "null" || condition == "not null" {
        // The case of a "null" or "not null" condition will be treated specially later during the
        // validation phase in a way that does not utilise the associated closure. Since we still
        // have to assign some closure in these cases, we use a constant closure that always
        // returns true:
        return Ok(CompiledCondition {
            original: String::from(""),
            parsed: Expression::None,
            compiled: Arc::new(|_| true),
        });
    }

    let unquoted_re = Regex::new(r#"^['"](?P<unquoted>.*)['"]$"#)?;
    let parsed_condition = parser.parse(condition);
    if let Err(_) = parsed_condition {
        return Err(
            ValveError::InputError(format!("Could not parse condition: {}", condition)).into(),
        );
    }
    let parsed_condition = parsed_condition.unwrap();
    if parsed_condition.len() != 1 {
        return Err(ValveError::InputError(format!(
            "Invalid condition: '{}'. Only one condition per column is allowed.",
            condition
        ))
        .into());
    }
    let parsed_condition = &parsed_condition[0];
    match &**parsed_condition {
        Expression::Function(name, args) => {
            if name == "equals" {
                if let Expression::Label(label) = &*args[0] {
                    let label = String::from(unquoted_re.replace(label, "$unquoted"));
                    return Ok(CompiledCondition {
                        original: condition.to_string(),
                        parsed: *parsed_condition.clone(),
                        compiled: Arc::new(move |x| x == label),
                    });
                } else {
                    return Err(ValveError::InputError(format!(
                        "ERROR: Invalid condition: {}",
                        condition
                    ))
                    .into());
                }
            } else if vec!["exclude", "match", "search"].contains(&name.as_str()) {
                if let Expression::RegexMatch(pattern, flags) = &*args[0] {
                    let mut pattern = String::from(unquoted_re.replace(pattern, "$unquoted"));
                    let mut flags = String::from(flags);
                    if flags != "" {
                        flags = format!("(?{})", flags.as_str());
                    }
                    match name.as_str() {
                        "exclude" => {
                            pattern = format!("{}{}", flags, pattern);
                            let re = Regex::new(pattern.as_str())?;
                            return Ok(CompiledCondition {
                                original: condition.to_string(),
                                parsed: *parsed_condition.clone(),
                                compiled: Arc::new(move |x| !re.is_match(x)),
                            });
                        }
                        "match" => {
                            pattern = format!("^{}{}$", flags, pattern);
                            let re = Regex::new(pattern.as_str())?;
                            return Ok(CompiledCondition {
                                original: condition.to_string(),
                                parsed: *parsed_condition.clone(),
                                compiled: Arc::new(move |x| re.is_match(x)),
                            });
                        }
                        "search" => {
                            pattern = format!("{}{}", flags, pattern);
                            let re = Regex::new(pattern.as_str())?;
                            return Ok(CompiledCondition {
                                original: condition.to_string(),
                                parsed: *parsed_condition.clone(),
                                compiled: Arc::new(move |x| re.is_match(x)),
                            });
                        }
                        _ => {
                            return Err(ValveError::InputError(format!(
                                "Unrecognized function name: {}",
                                name
                            ))
                            .into())
                        }
                    };
                } else {
                    return Err(ValveError::InputError(format!(
                        "Argument to condition: {} is not a regular expression",
                        condition
                    ))
                    .into());
                }
            } else if name == "in" {
                let mut alternatives: Vec<String> = vec![];
                for arg in args {
                    if let Expression::Label(value) = &**arg {
                        let value = unquoted_re.replace(value, "$unquoted");
                        alternatives.push(value.to_string());
                    } else {
                        return Err(ValveError::InputError(format!(
                            "Argument: {:?} to function 'in' is not a label",
                            arg
                        ))
                        .into());
                    }
                }
                return Ok(CompiledCondition {
                    original: condition.to_string(),
                    parsed: *parsed_condition.clone(),
                    compiled: Arc::new(move |x| alternatives.contains(&x.to_string())),
                });
            } else {
                return Err(ValveError::InputError(format!(
                    "Unrecognized function name: {}",
                    name
                ))
                .into());
            }
        }
        Expression::Label(value)
            if compiled_datatype_conditions.contains_key(&value.to_string()) =>
        {
            let compiled_datatype_condition = compiled_datatype_conditions
                .get(&value.to_string())
                .unwrap();
            return Ok(CompiledCondition {
                original: value.to_string(),
                parsed: compiled_datatype_condition.parsed.clone(),
                compiled: compiled_datatype_condition.compiled.clone(),
            });
        }
        _ => {
            return Err(
                ValveError::InputError(format!("Unrecognized condition: {}", condition)).into(),
            );
        }
    };
}

/// Given the config map, the name of a datatype, and a database connection pool used to determine
/// the database type, climb the datatype tree (as required), and return the first 'SQL type' found.
/// If there is no SQL type defined for the given datatype, return TEXT.
pub fn get_sql_type(
    dt_config: &HashMap<String, ValveDatatypeConfig>,
    datatype: &String,
    pool: &AnyPool,
) -> String {
    match dt_config.get(datatype) {
        None => return "TEXT".to_string(),
        Some(datatype) if datatype.sql_type != "" => {
            return datatype.sql_type.to_string();
        }
        _ => (),
    };

    let parent_datatype = dt_config
        .get(datatype)
        .and_then(|d| Some(d.parent.to_string()))
        .expect(&format!("Undefined datatype '{}'", datatype));

    return get_sql_type(dt_config, &parent_datatype, pool);
}

/// Given the global config map, a table name, a column name, and a database connection pool,
/// return the column's SQL type.
pub fn get_sql_type_from_global_config(
    config: &ValveConfig,
    table: &str,
    column: &str,
    pool: &AnyPool,
) -> String {
    let dt_config = &config.datatype;
    let dt = &config
        .table
        .get(table)
        .and_then(|t| t.column.get(column))
        .and_then(|c| Some(c.datatype.to_string()))
        .expect(&format!(
            "Could not determine datatype for '{}.{}'",
            table, column
        ));
    get_sql_type(dt_config, &dt, pool)
}

/// Given a SQL type, return the appropriate CAST(...) statement for casting the SQL_PARAM
/// from a TEXT column.
pub fn cast_sql_param_from_text(sql_type: &str) -> String {
    let s = sql_type.to_lowercase();
    if s == "numeric" {
        format!("CAST(NULLIF({}, '') AS NUMERIC)", SQL_PARAM)
    } else if s == "integer" {
        format!("CAST(NULLIF({}, '') AS INTEGER)", SQL_PARAM)
    } else if s == "real" {
        format!("CAST(NULLIF({}, '') AS REAL)", SQL_PARAM)
    } else {
        String::from(SQL_PARAM)
    }
}

/// Given a SQL type, return the appropriate CAST(...) statement for casting the SQL_PARAM
/// to a TEXT column.
pub fn cast_column_sql_to_text(column: &str, sql_type: &str) -> String {
    if sql_type.to_lowercase() == "text" {
        format!(r#""{}""#, column)
    } else {
        format!(r#"CAST("{}" AS TEXT)"#, column)
    }
}

/// Given a database row, the name of a column, and it's SQL type, return the value of that column
/// from the given row as a String.
pub fn get_column_value(row: &AnyRow, column: &str, sql_type: &str) -> String {
    let s = sql_type.to_lowercase();
    if s == "numeric" {
        let value: f64 = row
            .try_get(format!(r#"{}"#, column).as_str())
            .unwrap_or_default();
        value.to_string()
    } else if s == "integer" {
        let value: i32 = row
            .try_get(format!(r#"{}"#, column).as_str())
            .unwrap_or_default();
        value.to_string()
    } else if s == "real" {
        let value: f64 = row
            .try_get(format!(r#"{}"#, column).as_str())
            .unwrap_or_default();
        value.to_string()
    } else {
        let value: &str = row
            .try_get(format!(r#"{}"#, column).as_str())
            .unwrap_or_default();
        value.to_string()
    }
}

/// Given a SQL string, possibly with unbound parameters represented by the placeholder string
/// SQL_PARAM, and given a database pool, if the pool is of type Sqlite, then change the syntax used
/// for unbound parameters to Sqlite syntax, which uses "?", otherwise use Postgres syntax, which
/// uses numbered parameters, i.e., $1, $2, ...
pub fn local_sql_syntax(pool: &AnyPool, sql: &String) -> String {
    // Do not replace instances of SQL_PARAM if they are within quotation marks.
    let rx = Regex::new(&format!(
        r#"('[^'\\]*(?:\\.[^'\\]*)*'|"[^"\\]*(?:\\.[^"\\]*)*")|\b{}\b"#,
        SQL_PARAM
    ))
    .unwrap();

    let mut final_sql = String::from("");
    let mut pg_param_idx = 1;
    let mut saved_start = 0;
    for m in rx.find_iter(sql) {
        let this_match = &sql[m.start()..m.end()];
        final_sql.push_str(&sql[saved_start..m.start()]);
        if this_match == SQL_PARAM {
            if pool.any_kind() == AnyKind::Postgres {
                final_sql.push_str(&format!("${}", pg_param_idx));
                pg_param_idx += 1;
            } else {
                final_sql.push_str(&format!("?"));
            }
        } else {
            final_sql.push_str(&format!("{}", this_match));
        }
        saved_start = m.start() + this_match.len();
    }
    final_sql.push_str(&sql[saved_start..]);
    final_sql
}

/// Takes as arguments a list of tables and a configuration struct describing all of the constraints
/// between tables. After validating that there are no cycles amongst the foreign, tree, and
/// under dependencies, returns (i) the list of tables sorted according to their foreign key
/// dependencies, such that if table_a depends on table_b, then table_b comes before table_a in the
/// list; (ii) A map from table names to the lists of tables that depend on a given table; (iii) a
/// map from table names to the lists of tables that a given table depends on.
pub fn verify_table_deps_and_sort(
    table_list: &Vec<String>,
    constraints: &ValveConstraintConfig,
) -> (
    Vec<String>,
    HashMap<String, Vec<String>>,
    HashMap<String, Vec<String>>,
) {
    fn get_cycles(g: &DiGraphMap<&str, ()>) -> Result<Vec<String>, Vec<Vec<String>>> {
        let mut cycles = vec![];
        match toposort(&g, None) {
            Err(cycle) => {
                let problem_node = cycle.node_id();
                let neighbours = g.neighbors_directed(problem_node, Direction::Outgoing);
                for neighbour in neighbours {
                    let ways_to_problem_node =
                        all_simple_paths::<Vec<_>, _>(&g, neighbour, problem_node, 0, None);
                    for mut way in ways_to_problem_node {
                        let mut cycle = vec![problem_node];
                        cycle.append(&mut way);
                        let cycle = cycle
                            .iter()
                            .map(|&item| item.to_string())
                            .collect::<Vec<_>>();
                        cycles.push(cycle);
                    }
                }
                Err(cycles)
            }
            Ok(sorted) => {
                let mut sorted = sorted
                    .iter()
                    .map(|&item| item.to_string())
                    .collect::<Vec<_>>();
                sorted.reverse();
                Ok(sorted)
            }
        }
    }

    // Check for intra-table cycles:
    let trees = &constraints.tree;
    for table_name in table_list {
        let mut dependency_graph = DiGraphMap::<&str, ()>::new();
        let table_trees = trees
            .get(table_name)
            .expect(&format!("Undefined table '{}'", table_name));
        for tree in table_trees {
            let child = &tree.child;
            let parent = &tree.parent;
            let c_index = dependency_graph.add_node(&child);
            let p_index = dependency_graph.add_node(&parent);
            dependency_graph.add_edge(c_index, p_index, ());
        }
        match get_cycles(&dependency_graph) {
            Ok(_) => (),
            Err(cycles) => {
                let mut message = String::new();
                for cycle in cycles {
                    message.push_str(
                        format!("Cyclic dependency in table '{}': ", table_name).as_str(),
                    );
                    let end_index = cycle.len() - 1;
                    for (i, child) in cycle.iter().enumerate() {
                        if i < end_index {
                            let dep = table_trees.iter().find(|d| d.child == *child).unwrap();
                            let parent = &dep.parent;
                            message.push_str(
                                format!("tree({}) references {}", child, parent).as_str(),
                            );
                        }
                        if i < (end_index - 1) {
                            message.push_str(" and ");
                        }
                    }
                    message.push_str(". ");
                }
                panic!("{}", message);
            }
        };
    }

    // Check for inter-table cycles:
    let foreign_keys = &constraints.foreign;
    let under_keys = &constraints.under;
    let mut dependency_graph = DiGraphMap::<&str, ()>::new();
    for table_name in table_list {
        let t_index = dependency_graph.add_node(table_name);
        let fkeys = foreign_keys
            .get(table_name)
            .expect(&format!("Undefined table '{}'", table_name));
        for fkey in fkeys {
            let ftable = &fkey.ftable;
            let f_index = dependency_graph.add_node(&ftable);
            dependency_graph.add_edge(t_index, f_index, ());
        }

        let ukeys = under_keys
            .get(table_name)
            .expect(&format!("Undefined table '{}'", table_name));
        for ukey in ukeys {
            let ttable = &ukey.ttable;
            let tcolumn = &ukey.tcolumn;
            let value = &ukey.value;
            if ttable != table_name {
                let ttable_trees = trees.get(ttable).unwrap();
                if ttable_trees
                    .iter()
                    .filter(|d| d.child == *tcolumn)
                    .collect::<Vec<_>>()
                    .is_empty()
                {
                    panic!(
                        "under({}.{}, {}) refers to a non-existent tree",
                        ttable, tcolumn, value
                    );
                }
                let tt_index = dependency_graph.add_node(&ttable);
                dependency_graph.add_edge(t_index, tt_index, ());
            }
        }
    }

    match get_cycles(&dependency_graph) {
        Ok(sorted_table_list) => {
            let mut table_dependencies_in = HashMap::new();
            for node in dependency_graph.nodes() {
                let neighbors = dependency_graph
                    .neighbors_directed(node, petgraph::Direction::Incoming)
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>();
                table_dependencies_in.insert(node.to_string(), neighbors);
            }
            let mut table_dependencies_out = HashMap::new();
            for node in dependency_graph.nodes() {
                let neighbors = dependency_graph
                    .neighbors_directed(node, petgraph::Direction::Outgoing)
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>();
                table_dependencies_out.insert(node.to_string(), neighbors);
            }
            let mut sorted_table_list = sorted_table_list.clone();
            let mut with_internals = INTERNAL_TABLES
                .iter()
                .map(|t| t.to_string())
                .collect::<Vec<_>>();
            with_internals.append(&mut sorted_table_list);
            return (
                with_internals,
                table_dependencies_in,
                table_dependencies_out,
            );
        }
        Err(cycles) => {
            let mut message = String::new();
            for cycle in cycles {
                message.push_str(
                    format!("Cyclic dependency between tables {}: ", cycle.join(", ")).as_str(),
                );
                let end_index = cycle.len() - 1;
                for (i, table) in cycle.iter().enumerate() {
                    if i < end_index {
                        let dep_name = cycle.get(i + 1).unwrap().as_str();
                        let fkeys = foreign_keys.get(table).unwrap();
                        let ukeys = under_keys.get(table).unwrap();
                        let column;
                        let ref_table;
                        let ref_column;
                        if let Some(dep) = fkeys.iter().find(|d| d.ftable == *dep_name) {
                            column = &dep.column;
                            ref_table = &dep.ftable;
                            ref_column = &dep.fcolumn;
                        } else if let Some(dep) = ukeys.iter().find(|d| d.ttable == *dep_name) {
                            column = &dep.column;
                            ref_table = &dep.ttable;
                            ref_column = &dep.tcolumn;
                        } else {
                            panic!("{}. Unable to retrieve the details.", message);
                        }

                        message.push_str(
                            format!(
                                "{}.{} depends on {}.{}",
                                table, column, ref_table, ref_column,
                            )
                            .as_str(),
                        );
                    }
                    if i < (end_index - 1) {
                        message.push_str(" and ");
                    }
                }
                message.push_str(". ");
            }
            panic!("{}", message);
        }
    };
}

/// Given a global configuration struct and a table name, returns the mode of the table.
pub fn get_table_mode(config: &ValveConfig, table: &str) -> Result<String> {
    Ok(config
        .table
        .get(table)
        .ok_or::<ValveError>(
            ValveError::InputError(format!("'{}' is not in table config", table)).into(),
        )?
        .mode
        .to_string())
}

/// Given a table configuration map and a datatype configuration map, a parser, a table name, and a
/// database connection pool, return lists of: primary keys, unique constraints, foreign keys,
/// trees, and under keys.
pub fn get_table_constraints(
    tables_config: &HashMap<String, ValveTableConfig>,
    datatypes_config: &HashMap<String, ValveDatatypeConfig>,
    parser: &StartParser,
    table_name: &str,
    pool: &AnyPool,
) -> (
    Vec<String>,
    Vec<String>,
    Vec<ValveForeignConstraint>,
    Vec<ValveTreeConstraint>,
    Vec<ValveUnderConstraint>,
) {
    let mut primaries = vec![];
    let mut uniques = vec![];
    let mut foreigns = vec![];
    let mut trees = vec![];
    let mut unders = vec![];

    let columns = tables_config
        .get(table_name)
        .and_then(|t| Some(t.column.clone()))
        .expect(&format!("Undefined table '{}'", table_name));
    let mut colvals = vec![];
    for (_, column) in columns.iter() {
        colvals.push(column.clone());
    }
    for row in colvals {
        let datatype = &row.datatype;
        let sql_type = get_sql_type(datatypes_config, datatype, pool);
        let column_name = &row.column;
        let structure = &row.structure;
        if structure != "" {
            let parsed_structure = parser
                .parse(&structure)
                .expect(&format!("Could not parse structure '{}'", structure));
            for expression in parsed_structure {
                match *expression {
                    Expression::Label(value) if value == "primary" => {
                        primaries.push(column_name.to_string());
                    }
                    Expression::Label(value) if value == "unique" => {
                        uniques.push(column_name.to_string());
                    }
                    Expression::Function(name, args) if name == "from" => {
                        if args.len() != 1 {
                            panic!("Invalid foreign key: {} for: {}", structure, table_name);
                        }
                        match &*args[0] {
                            Expression::Field(ftable, fcolumn) => {
                                foreigns.push(ValveForeignConstraint {
                                    table: table_name.to_string(),
                                    column: column_name.to_string(),
                                    ftable: ftable.to_string(),
                                    fcolumn: fcolumn.to_string(),
                                });
                            }
                            _ => {
                                panic!("Invalid foreign key: {} for: {}", structure, table_name)
                            }
                        };
                    }
                    Expression::Function(name, args) if name == "tree" => {
                        if args.len() != 1 {
                            panic!(
                                "Invalid 'tree' constraint: {} for: {}",
                                structure, table_name
                            );
                        }
                        match &*args[0] {
                            Expression::Label(child) => {
                                let child_datatype = columns
                                    .get(child)
                                    .and_then(|c| Some(c.datatype.to_string()));
                                if let None = child_datatype {
                                    panic!(
                                        "Could not determine datatype for {} of tree({})",
                                        child, child
                                    );
                                }
                                let child_datatype = child_datatype.unwrap();
                                let parent = column_name;
                                let child_sql_type = get_sql_type(
                                    datatypes_config,
                                    &child_datatype.to_string(),
                                    pool,
                                );
                                if sql_type != child_sql_type {
                                    panic!(
                                        "SQL type '{}' of '{}' in 'tree({})' for table \
                                         '{}' doe snot match SQL type: '{}' of parent: '{}'.",
                                        child_sql_type, child, child, table_name, sql_type, parent
                                    );
                                }
                                trees.push(ValveTreeConstraint {
                                    child: child.to_string(),
                                    parent: column_name.to_string(),
                                });
                            }
                            _ => {
                                panic!(
                                    "Invalid 'tree' constraint: {} for: {}",
                                    structure, table_name
                                );
                            }
                        };
                    }
                    Expression::Function(name, args) if name == "under" => {
                        let generic_error = format!(
                            "Invalid 'under' constraint: {} for: {}",
                            structure, table_name
                        );
                        if args.len() != 2 {
                            panic!("{}", generic_error);
                        }
                        match (&*args[0], &*args[1]) {
                            (Expression::Field(ttable, tcolumn), Expression::Label(value)) => {
                                unders.push(ValveUnderConstraint {
                                    column: column_name.to_string(),
                                    ttable: ttable.to_string(),
                                    tcolumn: tcolumn.to_string(),
                                    value: json!(value),
                                });
                            }
                            (_, _) => panic!("{}", generic_error),
                        };
                    }
                    _ => panic!(
                        "Unrecognized structure: {} for {}.{}",
                        structure, table_name, column_name
                    ),
                };
            }
        }
    }

    return (primaries, uniques, foreigns, trees, unders);
}

/// Given a table configuration struct, a datatype configuration struct, a parser, a table name,
/// and a database connection pool, return a list of DDL statements that can be used to create the
/// database tables.
pub fn get_table_ddl(
    tables_config: &HashMap<String, ValveTableConfig>,
    datatypes_config: &HashMap<String, ValveDatatypeConfig>,
    parser: &StartParser,
    table_name: &String,
    pool: &AnyPool,
) -> Vec<String> {
    let mut statements = vec![];
    let mut create_lines = vec![
        format!(r#"CREATE TABLE "{}" ("#, table_name),
        String::from(r#"  "row_number" BIGINT,"#),
    ];

    let column_configs = {
        let normal_table_name;
        if let Some(s) = table_name.strip_suffix("_conflict") {
            normal_table_name = String::from(s);
        } else {
            normal_table_name = table_name.to_string();
        }
        let column_order = &tables_config
            .get(&normal_table_name)
            .expect(&format!("Undefined table '{}'", normal_table_name))
            .column_order;
        let columns = &tables_config
            .get(&normal_table_name)
            .expect(&format!("Undefined table '{}'", normal_table_name))
            .column;

        column_order
            .iter()
            .map(|column_name| columns.get(column_name).unwrap())
            .collect::<Vec<_>>()
    };

    let (primaries, uniques, foreigns, trees, _unders) = {
        // Conflict tables have no database constraints:
        if table_name.ends_with("_conflict") {
            (vec![], vec![], vec![], vec![], vec![])
        } else {
            get_table_constraints(tables_config, datatypes_config, parser, &table_name, &pool)
        }
    };

    let c = column_configs.len();
    let mut r = 0;
    for column_config in column_configs {
        r += 1;
        let sql_type = get_sql_type(datatypes_config, &column_config.datatype, pool);

        let short_sql_type = {
            if sql_type.to_lowercase().as_str().starts_with("varchar(") {
                "VARCHAR"
            } else {
                &sql_type
            }
        };

        if !SQL_TYPES.contains(&short_sql_type.to_lowercase().as_str()) {
            panic!(
                "Unrecognized SQL type '{}' for datatype: '{}'. Accepted SQL types are: {}",
                sql_type,
                column_config.datatype,
                SQL_TYPES.join(", ")
            );
        }

        let column_name = &column_config.column;
        let mut line = format!(r#"  "{}" {}"#, column_name, sql_type);

        // Check if the column is a primary key and indicate this in the DDL if so:
        if primaries.contains(&column_name) {
            line.push_str(" PRIMARY KEY");
        }

        // Check if the column has a unique constraint and indicate this in the DDL if so:
        if uniques.contains(&column_name) {
            line.push_str(" UNIQUE");
        }

        // If there are foreign constraints add a column to the end of the statement which we will
        // finish after this for loop is done:
        let applicable_foreigns = foreigns
            .iter()
            .filter(|fkey| {
                let table_config = &tables_config
                    .get(&fkey.ftable)
                    .expect(&format!("Undefined table '{}'", fkey.ftable));
                table_config.mode != "view"
            })
            .collect::<Vec<_>>();
        if !(r >= c && applicable_foreigns.is_empty()) {
            line.push_str(",");
        }
        create_lines.push(line);
    }

    // Add the SQL to indicate any foreign constraints:
    let num_fkeys = foreigns.len();
    for (i, fkey) in foreigns.iter().enumerate() {
        let ftable_mode = {
            let table_config = &tables_config
                .get(&fkey.ftable)
                .expect(&format!("Undefined table '{}'", fkey.ftable));
            table_config.mode.to_string()
        };
        if ftable_mode != "view" {
            create_lines.push(format!(
                r#"  FOREIGN KEY ("{}") REFERENCES "{}"("{}"){}"#,
                fkey.column,
                fkey.ftable,
                fkey.fcolumn,
                if i < (num_fkeys - 1) { "," } else { "" }
            ));
        }
    }
    create_lines.push(String::from(");"));
    // We are done generating the lines for the 'create table' statement. Join them and add the
    // result to the statements to return:
    statements.push(String::from(create_lines.join("\n")));

    // Loop through the tree constraints and if any of their associated child columns do not already
    // have an associated unique or primary index, create one implicitly here:
    for tree in trees {
        if !uniques.contains(&tree.child) && !primaries.contains(&tree.child) {
            statements.push(format!(
                r#"CREATE UNIQUE INDEX "{}_{}_idx" ON "{}"("{}");"#,
                table_name, tree.child, table_name, tree.child
            ));
        }
    }

    // Finally, create a further unique index on row_number:
    statements.push(format!(
        r#"CREATE UNIQUE INDEX "{}_row_number_idx" ON "{}"("row_number");"#,
        table_name, table_name
    ));

    return statements;
}

/// Given a list of messages and a HashMap, messages_stats, with which to collect counts of
/// message types, count the various message types encountered in the list and increment the counts
/// in messages_stats accordingly.
pub fn add_message_counts(
    messages: &Vec<ValveCellMessage>,
    messages_stats: &mut HashMap<String, usize>,
) {
    for message in messages {
        if message.level == "error" {
            let current_errors = messages_stats.get("error").unwrap();
            messages_stats.insert("error".to_string(), current_errors + 1);
        } else if message.level == "warning" {
            let current_warnings = messages_stats.get("warning").unwrap();
            messages_stats.insert("warning".to_string(), current_warnings + 1);
        } else if message.level == "info" {
            let current_infos = messages_stats.get("info").unwrap();
            messages_stats.insert("info".to_string(), current_infos + 1);
        } else {
            log::warn!("Unknown message type: {}", message.level);
        }
    }
}

/// Given a global config struct, return a list of defined datatype names sorted from the most
/// generic to the most specific. This function will panic if circular dependencies are encountered.
pub fn get_sorted_datatypes(config: &ValveConfig) -> Vec<&str> {
    let mut graph = DiGraphMap::<&str, ()>::new();
    let dt_config = &config.datatype;
    for (dt_name, dt_obj) in dt_config.iter() {
        let d_index = graph.add_node(dt_name);
        if dt_obj.parent != "" {
            let p_index = graph.add_node(&dt_obj.parent);
            graph.add_edge(d_index, p_index, ());
        }
    }

    let mut cycles = vec![];
    match toposort(&graph, None) {
        Err(cycle) => {
            let problem_node = cycle.node_id();
            let neighbours = graph.neighbors_directed(problem_node, Direction::Outgoing);
            for neighbour in neighbours {
                let ways_to_problem_node =
                    all_simple_paths::<Vec<_>, _>(&graph, neighbour, problem_node, 0, None);
                for mut way in ways_to_problem_node {
                    let mut cycle = vec![problem_node];
                    cycle.append(&mut way);
                    let cycle = cycle
                        .iter()
                        .map(|&item| item.to_string())
                        .collect::<Vec<_>>();
                    cycles.push(cycle);
                }
            }
            panic!(
                "Defined datatypes contain circular dependencies: {:?}",
                cycles
            );
        }
        Ok(mut sorted) => {
            sorted.reverse();
            sorted
        }
    }
}

/// Given a sorted list of datatypes and a list of messages for a given cell of some table, sort
/// the messages in the following way and return the sorted list of messages:
/// 1. Messages pertaining to datatype rule violations, sorted according to the order specified in
///    `sorted_datatypes`, followed by:
/// 2. Messages pertaining to violations of one of the rules in the rule table, followed by:
/// 3. Messages pertaining to structure violations.
pub fn sort_messages(
    sorted_datatypes: &Vec<&str>,
    cell_messages: &Vec<ValveCellMessage>,
) -> Vec<ValveCellMessage> {
    let mut datatype_messages = vec![];
    let mut structure_messages = vec![];
    let mut rule_messages = vec![];
    for message in cell_messages {
        let rule = message.rule.splitn(2, ":").collect::<Vec<_>>();
        if rule[0] == "rule" {
            rule_messages.push(message.clone());
        } else if rule[0] == "datatype" {
            datatype_messages.push(message.clone());
        } else {
            structure_messages.push(message.clone());
        }
    }

    if datatype_messages.len() > 0 {
        datatype_messages = {
            let mut sorted_messages = vec![];
            for datatype in sorted_datatypes {
                let mut messages = datatype_messages
                    .iter()
                    .filter(|m| m.rule == format!("datatype:{}", datatype))
                    .map(|m| m.clone())
                    .collect::<Vec<_>>();
                sorted_messages.append(&mut messages);
            }
            sorted_messages
        }
    }

    let mut messages = datatype_messages;
    messages.append(&mut rule_messages);
    messages.append(&mut structure_messages);
    messages
}

/// Given a configuration struct, a table name, some rows, their corresponding chunk number,
/// a struct used for recording message statistics, a verbose flag, and a database connection pool
/// used to determine the database type, return (i) an insert statement to the table, (ii)
/// parameters to bind to that SQL statement, (iii) an insert statement to the conflict version of
/// the table, (iv) parameters to bind to that SQL statement, (v) an insert statement to the
/// message table, and (vi) parameters to bind to that SQL statement. If the verbose flag is set,
/// the number of errors, warnings, and information messages generated are added to messages_stats.
pub async fn make_inserts(
    config: &ValveConfig,
    table_name: &String,
    rows: &mut Vec<ValveRow>,
    chunk_number: usize,
    messages_stats: &mut HashMap<String, usize>,
    verbose: bool,
    pool: &AnyPool,
) -> Result<(
    String,
    Vec<String>,
    String,
    Vec<String>,
    String,
    Vec<String>,
)> {
    fn is_conflict_row(row: &ValveRow, conflict_columns: &Vec<String>) -> bool {
        for (column, cell) in &row.contents {
            if !cell.valid && conflict_columns.contains(&column) {
                return true;
            }
        }
        return false;
    }

    fn generate_sql(
        config: &ValveConfig,
        main_table: &String,
        columns: &Vec<String>,
        rows: &mut Vec<ValveRow>,
        chunk_number: usize,
        messages_stats: &mut HashMap<String, usize>,
        verbose: bool,
        pool: &AnyPool,
    ) -> (
        String,
        Vec<String>,
        String,
        Vec<String>,
        String,
        Vec<String>,
    ) {
        let mut main_lines = vec![];
        let mut main_params = vec![];
        let mut conflict_lines = vec![];
        let mut conflict_params = vec![];
        let mut message_lines = vec![];
        let mut message_params = vec![];
        let sorted_datatypes = get_sorted_datatypes(config);
        let conflict_columns = get_conflict_columns(config, main_table);
        for (i, row) in rows.iter_mut().enumerate() {
            // enumerate begins at 0 but we need to begin at 1:
            let i = i + 1;
            row.row_number = Some(i as u32 + chunk_number as u32 * CHUNK_SIZE as u32);
            let use_conflict_table = is_conflict_row(&row, &conflict_columns);
            let mut row_values = vec![format!("{}", row.row_number.unwrap())];
            let mut row_params = vec![];
            for column in columns {
                let cell = row.contents.get(column).unwrap();
                // Insert the value of the cell into the column unless inserting it will cause a db
                // error or it has the nulltype field set, in which case insert NULL:
                let sql_type = get_sql_type_from_global_config(config, &main_table, column, pool);
                if cell.nulltype != None || is_sql_type_error(&sql_type, &cell.strvalue()) {
                    row_values.push(String::from("NULL"));
                } else {
                    row_values.push(cast_sql_param_from_text(&sql_type));
                    row_params.push(cell.strvalue());
                }

                // Generate values and params to be used for the insert to the message table:
                if verbose {
                    add_message_counts(&cell.messages, messages_stats);
                }

                for message in sort_messages(&sorted_datatypes, &cell.messages) {
                    let row = row.row_number.unwrap().to_string();
                    let message_values = vec![
                        SQL_PARAM, &row, SQL_PARAM, SQL_PARAM, SQL_PARAM, SQL_PARAM, SQL_PARAM,
                    ];

                    message_params.push(main_table.clone());
                    message_params.push(column.clone());
                    message_params.push(cell.strvalue());
                    message_params.push(message.level);
                    message_params.push(message.rule);
                    message_params.push(message.message);
                    let line = message_values.join(", ");
                    let line = format!("({})", line);
                    message_lines.push(line);
                }
            }
            let line = row_values.join(", ");
            let line = format!("({})", line);
            if use_conflict_table {
                conflict_lines.push(line);
                conflict_params.append(&mut row_params);
            } else {
                main_lines.push(line);
                main_params.append(&mut row_params);
            }
        }

        // Generate the SQL output for the insert to the table:
        fn get_table_output(lines: &Vec<String>, table: &str, columns: &Vec<String>) -> String {
            let mut output = String::from("");
            if !lines.is_empty() {
                output.push_str(&format!(
                    r#"INSERT INTO "{}" ("row_number", {}) VALUES"#,
                    table,
                    {
                        let mut quoted_columns = vec![];
                        for column in columns {
                            let quoted_column = format!(r#""{}""#, column);
                            quoted_columns.push(quoted_column);
                        }
                        quoted_columns.join(", ")
                    }
                ));
                output.push_str("\n");
                output.push_str(&lines.join(",\n"));
                output.push_str(";");
            }
            output
        }

        let main_output = get_table_output(&main_lines, &main_table, &columns);
        let conflict_table = format!("{}_conflict", main_table);
        let conflict_output = get_table_output(&conflict_lines, &conflict_table, &columns);

        // Generate the output for the insert to the message table:
        let mut message_output = String::from("");
        if !message_lines.is_empty() {
            message_output.push_str(r#"INSERT INTO "message" "#);
            message_output
                .push_str(r#"("table", "row", "column", "value", "level", "rule", "message") "#);
            message_output.push_str("VALUES");
            message_output.push_str("\n");
            message_output.push_str(&message_lines.join(",\n"));
            message_output.push_str(";");
        }

        (
            main_output,
            main_params,
            conflict_output,
            conflict_params,
            message_output,
            message_params,
        )
    }

    // Use the "column_order" field of the table config for this table to retrieve the column names
    // in the correct order:
    let column_names = &config
        .table
        .get(table_name)
        .expect(&format!("Undefined table '{}'", table_name))
        .column_order;

    let (main_sql, main_params, conflict_sql, conflict_params, message_sql, message_params) =
        generate_sql(
            config,
            &table_name,
            column_names,
            rows,
            chunk_number,
            messages_stats,
            verbose,
            pool,
        );

    Ok((
        main_sql,
        main_params,
        conflict_sql,
        conflict_params,
        message_sql,
        message_params,
    ))
}

/// Given a configuration map, a database connection pool, a table name, some rows to load,
/// and the chunk number corresponding to the rows, load the rows to the database. If the validate
/// flag is set, do inter-row validation on the rows before inserting  them to the table. If the
/// verbose flag is set to true, keep track of the number of error/warning/info statistics and
/// record them using `messages_stats`.
pub async fn insert_chunk(
    config: &ValveConfig,
    pool: &AnyPool,
    table_name: &String,
    rows: &mut Vec<ValveRow>,
    chunk_number: usize,
    messages_stats: &mut HashMap<String, usize>,
    verbose: bool,
    validate: bool,
) -> Result<()> {
    // First, do the tree validation. TODO: I don't remember why this needs to be done first, but
    // it does. Add a comment here explaining why.
    if validate {
        validate_rows_trees(config, pool, table_name, rows).await?;
    }

    // Try to insert the rows to the db first without validating unique and foreign constraints.
    // If there are constraint violations this will cause a database error, in which case we then
    // explicitly do the constraint validation and insert the resulting rows.
    // Note that instead of passing messages_stats here, we are going to initialize an empty map
    // and pass that instead. The reason is that if a database error gets thrown, and then we
    // redo the validation later, some of the messages will be double-counted. So to avoid that
    // we send an empty map here, and in the case of no database error, we will just add the
    // contents of the temporary map to messages_stats (in the Ok branch of the match statement
    // below).
    let mut tmp_messages_stats = HashMap::new();
    tmp_messages_stats.insert("error".to_string(), 0);
    tmp_messages_stats.insert("warning".to_string(), 0);
    tmp_messages_stats.insert("info".to_string(), 0);
    let (main_sql, main_params, conflict_sql, conflict_params, message_sql, message_params) =
        make_inserts(
            config,
            table_name,
            rows,
            chunk_number,
            &mut tmp_messages_stats,
            verbose,
            pool,
        )
        .await?;

    let main_sql = local_sql_syntax(&pool, &main_sql);
    let mut main_query = sqlx_query(&main_sql);
    for param in &main_params {
        main_query = main_query.bind(param);
    }
    let main_result = main_query.execute(pool).await;
    match main_result {
        Ok(_) => {
            let conflict_sql = local_sql_syntax(&pool, &conflict_sql);
            let mut conflict_query = sqlx_query(&conflict_sql);
            for param in &conflict_params {
                conflict_query = conflict_query.bind(param);
            }
            conflict_query.execute(pool).await?;

            let message_sql = local_sql_syntax(&pool, &message_sql);
            let mut message_query = sqlx_query(&message_sql);
            for param in &message_params {
                message_query = message_query.bind(param);
            }
            message_query.execute(pool).await?;

            if verbose {
                let curr_errors = messages_stats.get("error").unwrap();
                messages_stats.insert(
                    "error".to_string(),
                    curr_errors + tmp_messages_stats.get("error").unwrap(),
                );
                let curr_warnings = messages_stats.get("warning").unwrap();
                messages_stats.insert(
                    "warning".to_string(),
                    curr_warnings + tmp_messages_stats.get("warning").unwrap(),
                );
                let curr_infos = messages_stats.get("info").unwrap();
                messages_stats.insert(
                    "info".to_string(),
                    curr_infos + tmp_messages_stats.get("info").unwrap(),
                );
            }
        }
        Err(e) => {
            if validate {
                validate_rows_constraints(config, pool, table_name, rows).await?;
                let (
                    main_sql,
                    main_params,
                    conflict_sql,
                    conflict_params,
                    message_sql,
                    message_params,
                ) = make_inserts(
                    config,
                    table_name,
                    rows,
                    chunk_number,
                    messages_stats,
                    verbose,
                    pool,
                )
                .await?;

                let main_sql = local_sql_syntax(&pool, &main_sql);
                let mut main_query = sqlx_query(&main_sql);
                for param in &main_params {
                    main_query = main_query.bind(param);
                }
                main_query.execute(pool).await?;

                let conflict_sql = local_sql_syntax(&pool, &conflict_sql);
                let mut conflict_query = sqlx_query(&conflict_sql);
                for param in &conflict_params {
                    conflict_query = conflict_query.bind(param);
                }
                conflict_query.execute(pool).await?;

                let message_sql = local_sql_syntax(&pool, &message_sql);
                let mut message_query = sqlx_query(&message_sql);
                for param in &message_params {
                    message_query = message_query.bind(param);
                }
                message_query.execute(pool).await?;
            } else {
                return Err(ValveError::DatabaseError(e).into());
            }
        }
    };

    Ok(())
}

/// Given a configuration map, a database connection pool, maps for compiled datatype and rule
/// conditions, a table name, some chunks of rows to insert into the table in the database,
/// and the headers of the rows to be inserted, load the rows to the given table. If the validate
/// flag is set, do validate the rows before inserting them to the table. If the verbose flag is
/// set, keep track of the number of error/warning/info statistics and record them using
/// `messages_stats`.
pub async fn insert_chunks(
    config: &ValveConfig,
    pool: &AnyPool,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    table_name: &String,
    chunks: &IntoChunks<StringRecordsIter<'_, std::fs::File>>,
    headers: &StringRecord,
    messages_stats: &mut HashMap<String, usize>,
    verbose: bool,
    validate: bool,
) -> Result<()> {
    if !MULTI_THREADED {
        for (chunk_number, chunk) in chunks.into_iter().enumerate() {
            let mut rows: Vec<_> = chunk.collect();
            let mut intra_validated_rows = {
                let only_nulltype = !validate;
                validate_rows_intra(
                    config,
                    compiled_datatype_conditions,
                    compiled_rule_conditions,
                    table_name,
                    headers,
                    &mut rows,
                    only_nulltype,
                )
            };
            insert_chunk(
                config,
                pool,
                table_name,
                &mut intra_validated_rows,
                chunk_number,
                messages_stats,
                verbose,
                validate,
            )
            .await?;
        }
        Ok(())
    } else {
        // Here is how this works. First of all note that we are given a number of chunks of rows,
        // where the number of rows in each chunk is determined by CHUNK_SIZE (defined above). We
        // then divide the chunks into batches, where the number of chunks in each batch is
        // determined by the number of CPUs present on the system. We then iterate over the
        // batches one by one, assigning each chunk in a given batch to a worker thread whose
        // job is to perform intra-row validation on that chunk. The workers work in parallel, one
        // per CPU, and after all the workers have completed and their results have been collected,
        // we then perform inter-row validation on the chunks in the batch, this time serially.
        // Once this is done, we move on to the next batch and continue in this fashion.
        let num_cpus = num_cpus::get();
        let batches = chunks.into_iter().chunks(num_cpus);
        let mut chunk_number = 0;
        for batch in batches.into_iter() {
            let mut results = BTreeMap::new();
            crossbeam::scope(|scope| {
                let mut workers = vec![];
                for chunk in batch.into_iter() {
                    let mut rows: Vec<_> = chunk.collect();
                    workers.push(scope.spawn(move |_| {
                        let only_nulltype = !validate;
                        validate_rows_intra(
                            config,
                            compiled_datatype_conditions,
                            compiled_rule_conditions,
                            table_name,
                            headers,
                            &mut rows,
                            only_nulltype,
                        )
                    }));
                }

                for worker in workers {
                    let result = worker.join().unwrap();
                    results.insert(chunk_number, result);
                    chunk_number += 1;
                }
            })
            .expect("A child thread panicked");

            for (chunk_number, mut intra_validated_rows) in results {
                insert_chunk(
                    config,
                    pool,
                    table_name,
                    &mut intra_validated_rows,
                    chunk_number,
                    messages_stats,
                    verbose,
                    validate,
                )
                .await?;
            }
        }

        Ok(())
    }
}
