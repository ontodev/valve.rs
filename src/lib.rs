//! <!-- Please do not edit README.md directly. To generate a new readme from the crate documentation
//!      in src/lib.rs, install cargo-readme using `cargo install cargo-readme` and then run:
//!      `cargo readme > README.md` -->
//!
//! # valve.rs
//! A lightweight validation engine written in rust.
//!
//! ## Command line usage
//! Run:
//! ```
//! valve --help
//! ```
//! to see command line options.
//!
//! ## Python bindings
//! See [valve.py](https://github.com/ontodev/valve.py)

#[macro_use]
extern crate lalrpop_util;

pub mod ast;
pub mod validate;

lalrpop_mod!(pub valve_grammar);

use crate::validate::{
    validate_row, validate_rows_constraints, validate_rows_intra, validate_rows_trees,
    validate_tree_foreign_keys, validate_under, QueryAsIf, QueryAsIfKind, ResultRow,
};
use crate::{ast::Expression, valve_grammar::StartParser};
use async_recursion::async_recursion;
use chrono::Utc;
use crossbeam;
use futures::executor::block_on;
use indexmap::IndexMap;
use indoc::indoc;
use itertools::{IntoChunks, Itertools};
use lazy_static::lazy_static;
use petgraph::{
    algo::{all_simple_paths, toposort},
    graphmap::DiGraphMap,
    Direction,
};
use regex::Regex;
use serde_json::{json, Value as SerdeValue};
use sqlx::{
    any::{AnyConnectOptions, AnyKind, AnyPool, AnyPoolOptions, AnyRow},
    query as sqlx_query, Acquire, Column,
    Error::Configuration as SqlxCErr,
    Row, Transaction, ValueRef,
};
use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    path::Path,
    process,
    str::FromStr,
    sync::Arc,
};

/// The number of rows that are validated at a time by a thread.
static CHUNK_SIZE: usize = 500;

/// Run valve in multi-threaded mode.
static MULTI_THREADED: bool = true;

// Note that SQL_PARAM must be a 'word' (from the point of view of regular expressions) since in the
// local_sql_syntax() function below we are matchng against it using '\b' which represents a word
// boundary. If you want to use a non-word placeholder then you must also change '\b' in the regex
// to '\B'.
/// The word (in the regex sense) placeholder to use for query parameters when binding using sqlx.
static SQL_PARAM: &str = "VALVEPARAM";

lazy_static! {
    static ref PG_SQL_TYPES: Vec<&'static str> =
        vec!["text", "varchar", "numeric", "integer", "real"];
    static ref SL_SQL_TYPES: Vec<&'static str> = vec!["text", "numeric", "integer", "real"];
}

/// An alias for [serde_json::Map](..//serde_json/struct.Map.html)<String, [serde_json::Value](../serde_json/enum.Value.html)>.
// Note: serde_json::Map is
// [backed by a BTreeMap by default](https://docs.serde.rs/serde_json/map/index.html)
pub type SerdeMap = serde_json::Map<String, SerdeValue>;

/// Represents a structure such as those found in the `structure` column of the `column` table in
/// both its parsed format (i.e., as an [Expression](ast/enum.Expression.html)) as well as in its
/// original format (i.e., as a plain String).
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

/// Given the path to a configuration table (either a table.tsv file or a database containing a
/// table named "table"), load and check the 'table', 'column', and 'datatype' tables, and return
/// SerdeMaps corresponding to specials, tables, datatypes, and rules.
pub fn read_config_files(
    path: &str,
    config_table: &str,
) -> (SerdeMap, SerdeMap, SerdeMap, SerdeMap) {
    let special_table_types = json!({
        "table": {"required": true},
        "column": {"required": true},
        "datatype": {"required": true},
        "rule": {"required": false},
    });
    let special_table_types = special_table_types.as_object().unwrap();

    // Initialize the special table entries in the specials config map:
    let mut specials_config = SerdeMap::new();
    for t in special_table_types.keys() {
        specials_config.insert(t.to_string(), SerdeValue::Null);
    }

    // Load the table table from the given path:
    let mut tables_config = SerdeMap::new();
    let rows = {
        // Read in the configuration entry point (the "table table") from either a file or a
        // database table.
        if path.to_lowercase().ends_with(".tsv") {
            read_tsv_into_vector(path)
        } else {
            read_db_table_into_vector(path, config_table)
        }
    };

    for mut row in rows {
        for column in vec!["table", "path", "type"] {
            if !row.contains_key(column) || row.get(column) == None {
                panic!("Missing required column '{}' reading '{}'", column, path);
            }
        }

        for column in vec!["table", "path"] {
            if row.get(column).and_then(|c| c.as_str()).unwrap() == "" {
                panic!("Missing required value for '{}' reading '{}'", column, path);
            }
        }

        for column in vec!["type"] {
            if row.get(column).and_then(|c| c.as_str()).unwrap() == "" {
                row.remove(&column.to_string());
            }
        }

        if let Some(SerdeValue::String(row_type)) = row.get("type") {
            if row_type == "table" {
                let row_path = row.get("path").unwrap();
                if path.to_lowercase().ends_with(".tsv") && row_path != path {
                    panic!(
                        "Special 'table' path '{}' does not match this path '{}'",
                        row_path, path
                    );
                }
            }

            if special_table_types.contains_key(row_type) {
                match specials_config.get(row_type) {
                    Some(SerdeValue::Null) => (),
                    _ => panic!(
                        "Multiple tables with type '{}' declared in '{}'",
                        row_type, path
                    ),
                }
                let row_table = row.get("table").and_then(|t| t.as_str()).unwrap();
                specials_config.insert(
                    row_type.to_string(),
                    SerdeValue::String(row_table.to_string()),
                );
            } else {
                panic!("Unrecognized table type '{}' in '{}'", row_type, path);
            }
        }

        row.insert(String::from("column"), SerdeValue::Object(SerdeMap::new()));
        let row_table = row.get("table").and_then(|t| t.as_str()).unwrap();
        tables_config.insert(row_table.to_string(), SerdeValue::Object(row));
    }

    // Check that all the required special tables are present
    for (table_type, table_spec) in special_table_types.iter() {
        if let Some(SerdeValue::Bool(true)) = table_spec.get("required") {
            if let Some(SerdeValue::Null) = specials_config.get(table_type) {
                panic!("Missing required '{}' table in '{}'", table_type, path);
            }
        }
    }

    // Helper function for extracting special configuration (other than the main 'table'
    // configuration) from either a file or a table in the database, depending on the value of
    // `path`. When `path` ends in '.tsv', the path of the config table corresponding to
    // `table_type` is looked up, the TSV is read, and the rows are returned. When `path` does not
    // end in '.tsv', the table name corresponding to `table_type` is looked up in the database
    // indicated by `path`, the table is read, and the rows are returned.
    fn get_special_config(
        table_type: &str,
        specials_config: &SerdeMap,
        tables_config: &SerdeMap,
        path: &str,
    ) -> Vec<SerdeMap> {
        if path.to_lowercase().ends_with(".tsv") {
            let table_name = specials_config
                .get(table_type)
                .and_then(|d| d.as_str())
                .unwrap();
            let path = String::from(
                tables_config
                    .get(table_name)
                    .and_then(|t| t.get("path"))
                    .and_then(|p| p.as_str())
                    .unwrap(),
            );
            return read_tsv_into_vector(&path.to_string());
        } else {
            let mut db_table = None;
            for (table_name, table_config) in tables_config {
                let this_type = table_config.get("type");
                if let Some(this_type) = this_type {
                    let this_type = this_type.as_str().unwrap();
                    if this_type == table_type {
                        db_table = Some(table_name);
                        break;
                    }
                }
            }
            if db_table == None {
                panic!(
                    "Could not determine special table name for type '{}'.",
                    table_type
                );
            }
            let db_table = db_table.unwrap();
            read_db_table_into_vector(path, db_table)
        }
    }

    // Load datatype table
    let mut datatypes_config = SerdeMap::new();
    let rows = get_special_config("datatype", &specials_config, &tables_config, path);
    for mut row in rows {
        for column in vec![
            "datatype",
            "parent",
            "condition",
            "SQLite type",
            "PostgreSQL type",
        ] {
            if !row.contains_key(column) || row.get(column) == None {
                panic!("Missing required column '{}' reading '{}'", column, path);
            }
        }

        for column in vec!["datatype"] {
            if row.get(column).and_then(|c| c.as_str()).unwrap() == "" {
                panic!("Missing required value for '{}' reading '{}'", column, path);
            }
        }

        for column in vec!["parent", "condition", "SQLite type", "PostgreSQL type"] {
            if row.get(column).and_then(|c| c.as_str()).unwrap() == "" {
                row.remove(&column.to_string());
            }
        }

        let dt_name = row.get("datatype").and_then(|d| d.as_str()).unwrap();
        datatypes_config.insert(dt_name.to_string(), SerdeValue::Object(row));
    }

    for dt in vec!["text", "empty", "line", "word"] {
        if !datatypes_config.contains_key(dt) {
            panic!("Missing required datatype: '{}'", dt);
        }
    }

    // Load column table
    let rows = get_special_config("column", &specials_config, &tables_config, path);
    for mut row in rows {
        for column in vec!["table", "column", "label", "nulltype", "datatype"] {
            if !row.contains_key(column) || row.get(column) == None {
                panic!("Missing required column '{}' reading '{}'", column, path);
            }
        }

        for column in vec!["table", "column", "datatype"] {
            if row.get(column).and_then(|c| c.as_str()).unwrap() == "" {
                panic!("Missing required value for '{}' reading '{}'", column, path);
            }
        }

        for column in vec!["nulltype"] {
            if row.get(column).and_then(|c| c.as_str()).unwrap() == "" {
                row.remove(&column.to_string());
            }
        }

        let row_table = row.get("table").and_then(|t| t.as_str()).unwrap();
        if !tables_config.contains_key(row_table) {
            panic!("Undefined table '{}' reading '{}'", row_table, path);
        }

        if let Some(SerdeValue::String(nulltype)) = row.get("nulltype") {
            if !datatypes_config.contains_key(nulltype) {
                panic!("Undefined nulltype '{}' reading '{}'", nulltype, path);
            }
        }

        let datatype = row.get("datatype").and_then(|d| d.as_str()).unwrap();
        if !datatypes_config.contains_key(datatype) {
            panic!("Undefined datatype '{}' reading '{}'", datatype, path);
        }

        let row_table = row.get("table").and_then(|t| t.as_str()).unwrap();
        let column_name = row.get("column").and_then(|c| c.as_str()).unwrap();

        let columns_config = tables_config
            .get_mut(row_table)
            .and_then(|t| t.get_mut("column"))
            .and_then(|c| c.as_object_mut())
            .unwrap();
        columns_config.insert(column_name.to_string(), SerdeValue::Object(row));
    }

    // Load rule table if it exists
    let mut rules_config = SerdeMap::new();
    if let Some(SerdeValue::String(table_name)) = specials_config.get("rule") {
        let rows = get_special_config(table_name, &specials_config, &tables_config, path);
        for row in rows {
            for column in vec![
                "table",
                "when column",
                "when condition",
                "then column",
                "then condition",
                "level",
                "description",
            ] {
                if !row.contains_key(column) || row.get(column) == None {
                    panic!("Missing required column '{}' reading '{}'", column, path);
                }
                if row.get(column).and_then(|c| c.as_str()).unwrap() == "" {
                    panic!("Missing required value for '{}' reading '{}'", column, path);
                }
            }

            let row_table = row.get("table").and_then(|t| t.as_str()).unwrap();
            if !tables_config.contains_key(row_table) {
                panic!("Undefined table '{}' reading '{}'", row_table, path);
            }

            // Add the rule specified in the given row to the list of rules associated with the
            // value of the when column:
            let row_when_column = row.get("when column").and_then(|c| c.as_str()).unwrap();
            if !rules_config.contains_key(row_table) {
                rules_config.insert(String::from(row_table), SerdeValue::Object(SerdeMap::new()));
            }

            let table_rule_config = rules_config
                .get_mut(row_table)
                .and_then(|t| t.as_object_mut())
                .unwrap();
            if !table_rule_config.contains_key(row_when_column) {
                table_rule_config.insert(String::from(row_when_column), SerdeValue::Array(vec![]));
            }
            let column_rule_config = table_rule_config
                .get_mut(&row_when_column.to_string())
                .and_then(|w| w.as_array_mut())
                .unwrap();
            column_rule_config.push(SerdeValue::Object(row));
        }
    }

    // Manually add the messsage table config:
    tables_config.insert(
        "message".to_string(),
        json!({
            "table": "message",
            "description": "Validation messages for all of the tables and columns",
            "type": "message",
            "column_order": [
                "table",
                "row",
                "column",
                "value",
                "level",
                "rule",
                "message",
            ],
            "column": {
                "table": {
                    "table": "message",
                    "column": "table",
                    "description": "The table referred to by the message",
                    "datatype": "table_name",
                    "structure": ""
                },
                "row": {
                    "table": "message",
                    "column": "row",
                    "description": "The row number of the table referred to by the message",
                    "datatype": "natural_number",
                    "structure": ""
                },
                "column": {
                    "table": "message",
                    "column": "column",
                    "description": "The column of the table referred to by the message",
                    "datatype": "column_name",
                    "structure": ""
                },
                "value": {
                    "table": "message",
                    "column": "value",
                    "description": "The value that is the reason for the message",
                    "datatype": "text",
                    "structure": ""
                },
                "level": {
                    "table": "message",
                    "column": "level",
                    "description": "The severity of the violation",
                    "datatype": "word",
                    "structure": ""
                },
                "rule": {
                    "table": "message",
                    "column": "rule",
                    "description": "The rule violated by the value",
                    "datatype": "CURIE",
                    "structure": ""
                },
                "message": {
                    "table": "message",
                    "column": "message",
                    "description": "The message",
                    "datatype": "line",
                    "structure": ""
                }
            }
        }),
    );

    // Manually add the history table config:
    tables_config.insert(
        "history".to_string(),
        json!({
            "table": "history",
            "description": "History of changes to the VALVE database",
            "type": "history",
            "column_order": [
                "table",
                "row",
                "from",
                "to",
                "user",
            ],
            "column": {
                "table": {
                    "table": "history",
                    "column": "table",
                    "description": "The table referred to by the history entry",
                    "datatype": "table_name",
                    "structure": "",
                },
                "row": {
                    "table": "history",
                    "column": "row",
                    "description": "The row number of the table referred to by the history entry",
                    "datatype": "natural_number",
                    "structure": "",
                },
                "from": {
                    "table": "history",
                    "column": "from",
                    "description": "The initial value of the row",
                    "datatype": "text",
                    "structure": "",
                },
                "to": {
                    "table": "history",
                    "column": "to",
                    "description": "The final value of the row",
                    "datatype": "text",
                    "structure": "",
                },
                "summary": {
                    "table": "history",
                    "column": "summary",
                    "description": "Summarizes the changes to each column of the row",
                    "datatype": "text",
                    "structure": "",
                },
                "user": {
                    "table": "history",
                    "column": "user",
                    "description": "User responsible for the change",
                    "datatype": "line",
                    "structure": "",
                },
                "undone_by": {
                    "table": "history",
                    "column": "undone_by",
                    "description": "User who has undone the change. Null if it has not been undone",
                    "datatype": "line",
                    "structure": "",
                },
            }
        }),
    );

    // Finally, return all the configs:
    (
        specials_config,
        tables_config,
        datatypes_config,
        rules_config,
    )
}

/// Given the global configuration map and a parser, compile all of the datatype conditions,
/// add them to a hash map whose keys are the text versions of the conditions and whose values
/// are the compiled conditions, and then finally return the hash map.
pub fn get_compiled_datatype_conditions(
    config: &SerdeMap,
    parser: &StartParser,
) -> HashMap<String, CompiledCondition> {
    let mut compiled_datatype_conditions: HashMap<String, CompiledCondition> = HashMap::new();
    let datatypes_config = config.get("datatype").and_then(|t| t.as_object()).unwrap();
    for (_, row) in datatypes_config.iter() {
        let row = row.as_object().unwrap();
        let dt_name = row.get("datatype").and_then(|d| d.as_str()).unwrap();
        let condition = row.get("condition").and_then(|c| c.as_str());
        let compiled_condition =
            compile_condition(condition, parser, &compiled_datatype_conditions);
        if let Some(_) = condition {
            compiled_datatype_conditions.insert(dt_name.to_string(), compiled_condition);
        }
    }

    compiled_datatype_conditions
}

/// Given the global config map, a hash map of compiled datatype conditions (indexed by the text
/// version of the conditions), and a parser, compile all of the rule conditions, add them to a
/// hash which has the following structure:
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
    config: &SerdeMap,
    compiled_datatype_conditions: HashMap<String, CompiledCondition>,
    parser: &StartParser,
) -> HashMap<String, HashMap<String, Vec<ColumnRule>>> {
    let mut compiled_rule_conditions = HashMap::new();
    let tables_config = config.get("table").and_then(|t| t.as_object()).unwrap();
    let rules_config = config.get("rule").and_then(|t| t.as_object()).unwrap();
    for (rules_table, table_rules) in rules_config.iter() {
        let table_rules = table_rules.as_object().unwrap();
        for (_, column_rules) in table_rules.iter() {
            let column_rules = column_rules.as_array().unwrap();
            for row in column_rules {
                // Compile and collect the when and then conditions.
                let mut column_rule_key = None;
                for column in vec!["when column", "then column"] {
                    let row_column = row.get(column).and_then(|c| c.as_str()).unwrap();
                    if column == "when column" {
                        column_rule_key = Some(row_column.to_string());
                    }
                    if !tables_config
                        .get(rules_table)
                        .and_then(|t| t.get("column"))
                        .and_then(|c| c.as_object())
                        .and_then(|c| Some(c.contains_key(row_column)))
                        .unwrap()
                    {
                        panic!(
                            "Undefined column '{}.{}' in rules table",
                            rules_table, row_column
                        );
                    }
                }
                let column_rule_key = column_rule_key.unwrap();

                let mut when_compiled = None;
                let mut then_compiled = None;
                for column in vec!["when condition", "then condition"] {
                    let condition_option = row.get(column).and_then(|c| c.as_str());
                    if let Some(_) = condition_option {
                        let compiled_condition = compile_condition(
                            condition_option,
                            parser,
                            &compiled_datatype_conditions,
                        );
                        if column == "when condition" {
                            when_compiled = Some(compiled_condition);
                        } else if column == "then condition" {
                            then_compiled = Some(compiled_condition);
                        }
                    }
                }

                if let (Some(when_compiled), Some(then_compiled)) = (when_compiled, then_compiled) {
                    if !compiled_rule_conditions.contains_key(rules_table) {
                        let table_rules = HashMap::new();
                        compiled_rule_conditions.insert(rules_table.to_string(), table_rules);
                    }
                    let table_rules = compiled_rule_conditions.get_mut(rules_table).unwrap();
                    if !table_rules.contains_key(&column_rule_key) {
                        table_rules.insert(column_rule_key.to_string(), vec![]);
                    }
                    let column_rules = table_rules.get_mut(&column_rule_key).unwrap();
                    column_rules.push(ColumnRule {
                        when: when_compiled,
                        then: then_compiled,
                    });
                }
            }
        }
    }

    compiled_rule_conditions
}

/// Given the global config map and a parser, parse all of the structure conditions, add them to
/// a hash map whose keys are given by the text versions of the conditions and whose values are
/// given by the parsed versions, and finally return the hashmap.
pub fn get_parsed_structure_conditions(
    config: &SerdeMap,
    parser: &StartParser,
) -> HashMap<String, ParsedStructure> {
    let mut parsed_structure_conditions = HashMap::new();
    let tables_config = config.get("table").and_then(|t| t.as_object()).unwrap();
    for (table, table_config) in tables_config.iter() {
        let columns_config = table_config
            .get("column")
            .and_then(|c| c.as_object())
            .unwrap();
        for (_, row) in columns_config.iter() {
            let row_table = table;
            let structure = row.get("structure").and_then(|s| s.as_str());
            match structure {
                Some(structure) if structure != "" => {
                    let parsed_structure = parser.parse(structure);
                    if let Err(e) = parsed_structure {
                        panic!(
                            "While parsing structure: '{}' for column: '{}.{}' got error:\n{}",
                            structure,
                            row_table,
                            row.get("table").and_then(|t| t.as_str()).unwrap(),
                            e
                        );
                    }
                    let parsed_structure = parsed_structure.unwrap();
                    let parsed_structure = &parsed_structure[0];
                    let parsed_structure = ParsedStructure {
                        original: structure.to_string(),
                        parsed: *parsed_structure.clone(),
                    };
                    parsed_structure_conditions.insert(structure.to_string(), parsed_structure);
                }
                _ => (),
            };
        }
    }

    parsed_structure_conditions
}

/// Given the name of a table and a database connection pool, generate SQL for creating a view
/// based on the table that provides a unified representation of the normal and conflict versions
/// of the table, plus columns summarising the information associated with the given table that is
/// contained in the message and history tables. The SQL generated is in the form of a tuple of
/// Strings, with the first string being a SQL statement for dropping the view, and the second
/// string being a SQL statement for creating it.
fn get_sql_for_standard_view(table: &str, pool: &AnyPool) -> (String, String) {
    let mut drop_view_sql = format!(r#"DROP VIEW IF EXISTS "{}_view""#, table);
    let message_t;
    if pool.any_kind() == AnyKind::Postgres {
        drop_view_sql.push_str(" CASCADE");
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
    drop_view_sql.push_str(";");

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

    (drop_view_sql, create_view_sql)
}

/// Given the tables configuration map, the name of a table and a database connection pool,
/// generate SQL for creating a more user-friendly version of the view than the one generated by
/// [get_sql_for_standard_view()]. Unlike the standard view generated by that function, the view
/// generated by this function (called my_table_text_view) always shows all of the values (which are
/// all rendered as text) of every column in the table, even when those values contain SQL datatype
/// errors. Like the function for generating a standard view, the SQL generated by this function is
/// returned in the form of a tuple of Strings, with the first string being a SQL statement
/// for dropping the view, and the second string being a SQL statement for creating it.
fn get_sql_for_text_view(
    tables_config: &mut SerdeMap,
    table: &str,
    pool: &AnyPool,
) -> (String, String) {
    let is_clause = if pool.any_kind() == AnyKind::Sqlite {
        "IS"
    } else {
        "IS NOT DISTINCT FROM"
    };

    let real_columns = tables_config
        .get(table)
        .and_then(|t| t.as_object())
        .and_then(|t| t.get("column"))
        .and_then(|t| t.as_object())
        .and_then(|t| Some(t.keys()))
        .and_then(|k| Some(k.map(|k| k.to_string())))
        .and_then(|t| Some(t.collect::<Vec<_>>()))
        .unwrap();

    // Add a second "text view" such that the datatypes of all values are TEXT and appear
    // directly in their corresponsing columns (rather than as NULLs) even when they have
    // SQL datatype errors.
    let mut drop_view_sql = format!(r#"DROP VIEW IF EXISTS "{}_text_view""#, table);
    if pool.any_kind() == AnyKind::Postgres {
        drop_view_sql.push_str(" CASCADE");
    }

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

    (drop_view_sql, create_view_sql)
}

/// Given config maps for tables and datatypes, a database connection pool, and a StartParser,
/// read in the TSV files corresponding to the tables defined in the tables config, and use that
/// information to fill in constraints information into a new config map that is then returned along
/// with a list of the tables in the database sorted according to their mutual dependencies. If
/// the flag `verbose` is set to true, emit SQL to create the database schema to STDOUT.
/// If `command` is set to [ValveCommand::Create], execute the SQL statements to create the
/// database using the given connection pool. If it is set to [ValveCommand::Load], execute the SQL
/// to load it as well.
pub async fn configure_db(
    tables_config: &mut SerdeMap,
    datatypes_config: &mut SerdeMap,
    pool: &AnyPool,
    parser: &StartParser,
    verbose: bool,
    command: &ValveCommand,
) -> Result<(Vec<String>, SerdeMap), sqlx::Error> {
    // This is the SerdeMap that we will be returning:
    let mut constraints_config = SerdeMap::new();
    constraints_config.insert(String::from("foreign"), SerdeValue::Object(SerdeMap::new()));
    constraints_config.insert(String::from("unique"), SerdeValue::Object(SerdeMap::new()));
    constraints_config.insert(String::from("primary"), SerdeValue::Object(SerdeMap::new()));
    constraints_config.insert(String::from("tree"), SerdeValue::Object(SerdeMap::new()));
    constraints_config.insert(String::from("under"), SerdeValue::Object(SerdeMap::new()));

    // Begin by reading in the TSV files corresponding to the tables defined in tables_config, and
    // use that information to create the associated database tables, while saving constraint
    // information to constrains_config.
    let mut setup_statements = HashMap::new();
    for table_name in tables_config.keys().cloned().collect::<Vec<_>>() {
        let optional_path = tables_config
            .get(&table_name)
            .and_then(|r| r.get("path"))
            .and_then(|p| p.as_str());

        let mut path = None;
        match optional_path {
            None => {
                // If an entry of the tables_config has no path then it is an internal table which
                // need not be configured explicitly. Currently the only examples are the message
                // and history tables.
                if table_name != "message" && table_name != "history" {
                    panic!("No path defined for table {}", table_name);
                }
                continue;
            }
            Some(p) if !Path::new(p).is_file() => {
                eprintln!("WARN: File does not exist {}", p);
            }
            Some(p) if Path::new(p).canonicalize().is_err() => {
                eprintln!("WARN: File path could not be made canonical {}", p);
            }
            Some(p) => path = Some(p.to_string()),
        };

        let defined_columns: Vec<String> = tables_config
            .get(&table_name)
            .and_then(|r| r.get("column"))
            .and_then(|v| v.as_object())
            .and_then(|o| Some(o.keys()))
            .and_then(|k| Some(k.cloned()))
            .and_then(|k| Some(k.collect()))
            .unwrap();

        // We use column_order to explicitly indicate the order in which the columns should appear
        // in the table, for later reference. The default is to preserve the order from the actual
        // table file. If that does not exist, we use the ordering in defined_columns.
        let mut column_order = vec![];
        if let Some(path) = path {
            // Get the actual columns from the data itself. Note that we set has_headers to
            // false(even though the files have header rows) in order to explicitly read the
            // header row.
            let mut rdr = csv::ReaderBuilder::new()
                .has_headers(false)
                .delimiter(b'\t')
                .from_reader(File::open(path.clone()).unwrap_or_else(|err| {
                    panic!("Unable to open '{}': {}", path.clone(), err);
                }));
            let mut iter = rdr.records();
            if let Some(result) = iter.next() {
                let actual_columns = result
                    .unwrap()
                    .iter()
                    .map(|c| c.to_string())
                    .collect::<Vec<_>>();
                // Make sure that the actual columns found in the table file, and the columns
                // defined in the column config, exactly match in terms of their content:
                for column_name in &actual_columns {
                    column_order.push(json!(column_name));
                    if !defined_columns.contains(&column_name.to_string()) {
                        panic!(
                            "Column '{}.{}' not in column config",
                            table_name, column_name
                        );
                    }
                }
                for column_name in &defined_columns {
                    if !actual_columns.contains(&column_name.to_string()) {
                        panic!(
                            "Defined column '{}.{}' not found in table",
                            table_name, column_name
                        );
                    }
                }
            } else {
                panic!("'{}' is empty", path);
            }
        }

        if column_order.is_empty() {
            column_order = defined_columns.iter().map(|c| json!(c)).collect::<Vec<_>>();
        }
        tables_config
            .get_mut(&table_name)
            .and_then(|t| t.as_object_mut())
            .and_then(|o| {
                o.insert(
                    String::from("column_order"),
                    SerdeValue::Array(column_order),
                )
            });

        // Create the table and its corresponding conflict table:
        let mut table_statements = vec![];
        for table in vec![table_name.to_string(), format!("{}_conflict", table_name)] {
            let (mut statements, table_constraints) =
                create_table_statement(tables_config, datatypes_config, parser, &table, &pool);
            table_statements.append(&mut statements);
            if !table.ends_with("_conflict") {
                for constraint_type in vec!["foreign", "unique", "primary", "tree", "under"] {
                    let table_constraints = table_constraints.get(constraint_type).unwrap().clone();
                    constraints_config
                        .get_mut(constraint_type)
                        .and_then(|o| o.as_object_mut())
                        .and_then(|o| o.insert(table_name.to_string(), table_constraints));
                }
            }
        }

        let (drop_view_sql, create_view_sql) = get_sql_for_standard_view(&table_name, pool);
        let (drop_text_view_sql, create_text_view_sql) =
            get_sql_for_text_view(tables_config, &table_name, pool);
        table_statements.push(drop_text_view_sql);
        table_statements.push(drop_view_sql);
        table_statements.push(create_view_sql);
        table_statements.push(create_text_view_sql);

        setup_statements.insert(table_name.to_string(), table_statements);
    }

    // Sort the tables according to their foreign key dependencies so that tables are always loaded
    // after the tables they depend on. Ignore the internal message and history tables:
    let sorted_tables = verify_table_deps_and_sort(
        &setup_statements.keys().cloned().collect(),
        &constraints_config,
    );

    if *command != ValveCommand::Config || verbose {
        // Generate DDL for the history table:
        let mut history_statements = vec![];
        history_statements.push({
            let mut sql = r#"DROP TABLE IF EXISTS "history""#.to_string();
            if pool.any_kind() == AnyKind::Postgres {
                sql.push_str(" CASCADE");
            }
            sql.push_str(";");
            sql
        });
        history_statements.push(format!(
            indoc! {r#"
                CREATE TABLE "history" (
                  {row_number}
                  "table" TEXT,
                  "row" BIGINT,
                  "from" TEXT,
                  "to" TEXT,
                  "summary" TEXT,
                  "user" TEXT,
                  "undone_by" TEXT,
                  {timestamp}
                );
              "#},
            row_number = {
                if pool.any_kind() == AnyKind::Sqlite {
                    "\"history_id\" INTEGER PRIMARY KEY,"
                } else {
                    "\"history_id\" SERIAL PRIMARY KEY,"
                }
            },
            timestamp = {
                if pool.any_kind() == AnyKind::Sqlite {
                    "\"timestamp\" TIMESTAMP DEFAULT(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'))"
                } else {
                    "\"timestamp\" TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
                }
            },
        ));
        history_statements
            .push(r#"CREATE INDEX "history_tr_idx" ON "history"("table", "row");"#.to_string());
        setup_statements.insert("history".to_string(), history_statements);

        // Generate DDL for the message table:
        let mut message_statements = vec![];
        message_statements.push({
            let mut sql = r#"DROP TABLE IF EXISTS "message""#.to_string();
            if pool.any_kind() == AnyKind::Postgres {
                sql.push_str(" CASCADE");
            }
            sql.push_str(";");
            sql
        });
        message_statements.push(format!(
            indoc! {r#"
                CREATE TABLE "message" (
                  {}
                  "table" TEXT,
                  "row" BIGINT,
                  "column" TEXT,
                  "value" TEXT,
                  "level" TEXT,
                  "rule" TEXT,
                  "message" TEXT
                );
              "#},
            {
                if pool.any_kind() == AnyKind::Sqlite {
                    "\"message_id\" INTEGER PRIMARY KEY,"
                } else {
                    "\"message_id\" SERIAL PRIMARY KEY,"
                }
            },
        ));
        message_statements.push(
            r#"CREATE INDEX "message_trc_idx" ON "message"("table", "row", "column");"#.to_string(),
        );
        setup_statements.insert("message".to_string(), message_statements);

        // Add the message and history tables to the beginning of the list of tables to create
        // (the message table in particular needs to be at the beginning since the table views all
        // reference it).
        let mut tables_to_create = vec!["message".to_string(), "history".to_string()];
        tables_to_create.append(&mut sorted_tables.clone());
        for table in &tables_to_create {
            let table_statements = setup_statements.get(table).unwrap();
            if *command != ValveCommand::Config {
                for stmt in table_statements {
                    sqlx_query(stmt)
                        .execute(pool)
                        .await
                        .expect(format!("The SQL statement: {} returned an error", stmt).as_str());
                }
            }
            if verbose {
                let output = String::from(table_statements.join("\n"));
                println!("{}\n", output);
            }
        }
    }

    return Ok((sorted_tables, constraints_config));
}

/// Various VALVE commands, used with [valve()](valve).
#[derive(Debug, PartialEq, Eq)]
pub enum ValveCommand {
    /// Configure but do not create or load.
    Config,
    /// Configure and create but do not load.
    Create,
    /// Configure, create, and load.
    Load,
}

/// Given a path to a configuration table (either a table.tsv file or a database containing a
/// table named "table"), and a directory in which to find/create a database: configure the
/// database using the configuration which can be looked up using the table table, and
/// optionally create and/or load it according to the value of `command` (see [ValveCommand]).
/// If the `verbose` flag is set to true, output status messages while loading. If `config_table`
/// is given and `table_table` indicates a database, query the table called `config_table` for the
/// table table information. Returns the configuration map as a String. If `initial_load` is set to
/// true, then (SQLite only) the database settings will be tuned for initial loading. Note that
/// these settings are unsafe and should be used for initial loading only, as data integrity will
/// not be guaranteed in the case of an interrupted transaction.
pub async fn valve(
    table_table: &str,
    database: &str,
    command: &ValveCommand,
    verbose: bool,
    initial_load: bool,
    config_table: &str,
) -> Result<String, sqlx::Error> {
    let parser = StartParser::new();

    let (specials_config, mut tables_config, mut datatypes_config, rules_config) =
        read_config_files(&table_table.to_string(), config_table);

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
    if *command == ValveCommand::Load && pool.any_kind() == AnyKind::Sqlite {
        sqlx_query("PRAGMA foreign_keys = ON")
            .execute(&pool)
            .await?;
        if initial_load {
            // These pragmas are unsafe but they are used during initial loading since data
            // integrity is not a priority in this case.
            sqlx_query("PRAGMA journal_mode = OFF")
                .execute(&pool)
                .await?;
            sqlx_query("PRAGMA synchronous = 0").execute(&pool).await?;
            sqlx_query("PRAGMA cache_size = 1000000")
                .execute(&pool)
                .await?;
            sqlx_query("PRAGMA temp_store = MEMORY")
                .execute(&pool)
                .await?;
        }
    }

    let (sorted_table_list, constraints_config) = configure_db(
        &mut tables_config,
        &mut datatypes_config,
        &pool,
        &parser,
        verbose,
        command,
    )
    .await?;

    let mut config = SerdeMap::new();
    config.insert(
        String::from("special"),
        SerdeValue::Object(specials_config.clone()),
    );
    config.insert(
        String::from("table"),
        SerdeValue::Object(tables_config.clone()),
    );
    config.insert(
        String::from("datatype"),
        SerdeValue::Object(datatypes_config.clone()),
    );
    config.insert(
        String::from("rule"),
        SerdeValue::Object(rules_config.clone()),
    );
    config.insert(
        String::from("constraints"),
        SerdeValue::Object(constraints_config.clone()),
    );
    let mut sorted_table_serdevalue_list: Vec<SerdeValue> = vec![];
    for table in &sorted_table_list {
        sorted_table_serdevalue_list.push(SerdeValue::String(table.to_string()));
    }
    config.insert(
        String::from("sorted_table_list"),
        SerdeValue::Array(sorted_table_serdevalue_list),
    );

    let compiled_datatype_conditions = get_compiled_datatype_conditions(&config, &parser);
    let compiled_rule_conditions =
        get_compiled_rule_conditions(&config, compiled_datatype_conditions.clone(), &parser);

    if *command == ValveCommand::Load {
        if verbose {
            eprintln!(
                "{} - Processing {} tables.",
                Utc::now(),
                sorted_table_list.len()
            );
        }
        load_db(
            &config,
            &pool,
            &compiled_datatype_conditions,
            &compiled_rule_conditions,
            verbose,
        )
        .await?;
    }

    let config = SerdeValue::Object(config);
    Ok(config.to_string())
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
pub fn query_with_message_values(table: &str, global_config: &SerdeMap, pool: &AnyPool) -> String {
    let real_columns = global_config
        .get("table")
        .and_then(|t| t.get(table))
        .and_then(|t| t.as_object())
        .and_then(|t| t.get("column"))
        .and_then(|t| t.as_object())
        .and_then(|t| Some(t.keys()))
        .and_then(|k| Some(k.map(|k| k.to_string())))
        .and_then(|t| Some(t.collect::<Vec<_>>()))
        .unwrap();

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
    global_config: &SerdeMap,
    pool: &AnyPool,
    tx: &mut Transaction<'_, sqlx::Any>,
) -> Result<IndexMap<u32, SerdeMap>, String> {
    // Since the consequence of an update could involve currently invalid rows
    // (in the conflict table) becoming valid or vice versa, we need to check rows for
    // which the value of the column is the same as `value`
    let sql = {
        format!(
            r#"{main_query} WHERE "{column}" = '{value}'{except}"#,
            main_query = query_with_message_values(table, global_config, pool),
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
    let mut table_rows = IndexMap::new();
    for row in query
        .fetch_all(tx.acquire().await.map_err(|e| e.to_string())?)
        .await
        .map_err(|e| e.to_string())?
    {
        let mut table_row = SerdeMap::new();
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
                let cell = json!({
                    "value": value,
                    "valid": true,
                    "messages": json!([]),
                });
                table_row.insert(cname.to_string(), json!(cell));
            }
        }
        let row_number = row_number.ok_or("Row: has no row number".to_string())?;
        table_rows.insert(row_number, table_row);
    }

    Ok(table_rows)
}

/// Given a global configuration map, a database connection pool, a database transaction, a table
/// name and a row number, get the logical contents of that row (whether or not it is valid),
/// including any messages, from the database.
pub async fn get_row_from_db(
    global_config: &SerdeMap,
    pool: &AnyPool,
    tx: &mut Transaction<'_, sqlx::Any>,
    table: &str,
    row_number: &u32,
) -> Result<SerdeMap, sqlx::Error> {
    let sql = format!(
        "{} WHERE row_number = {}",
        query_with_message_values(table, global_config, pool),
        row_number
    );
    let query = sqlx_query(&sql);
    let rows = query.fetch_all(tx.acquire().await?).await?;
    if rows.len() == 0 {
        return Err(SqlxCErr(
            format!(
                "In get_row_from_db(). No rows found for row_number: {}",
                row_number
            )
            .into(),
        ));
    }
    let sql_row = &rows[0];

    let messages = {
        let raw_messages = sql_row.try_get_raw("message")?;
        if raw_messages.is_null() {
            vec![]
        } else {
            let messages: &str = sql_row.get("message");
            match serde_json::from_str::<SerdeValue>(messages) {
                Err(e) => return Err(SqlxCErr(e.into())),
                Ok(SerdeValue::Array(m)) => m,
                _ => return Err(SqlxCErr(format!("{} is not an array.", messages).into())),
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
) -> Result<String, String> {
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
    let rows = query
        .fetch_all(tx.acquire().await.map_err(|e| e.to_string())?)
        .await
        .map_err(|e| e.to_string())?;
    if rows.len() == 0 {
        return Err(format!(
            "In get_db_value(). No rows found for row_number: {}",
            row_number
        ));
    }
    let result_row = &rows[0];
    let value: &str = result_row.try_get(column).unwrap();
    Ok(value.to_string())
}

/// Given a global config map, a database connection pool, a database transaction, a table name,
/// and a [QueryAsIf] struct representing a custom modification to the query of the table, get
/// the rows that will potentially be affected by the database change to the row indicated in
/// query_as_if.
pub async fn get_rows_to_update(
    global_config: &SerdeMap,
    pool: &AnyPool,
    tx: &mut Transaction<'_, sqlx::Any>,
    table: &str,
    query_as_if: &QueryAsIf,
) -> Result<
    (
        IndexMap<String, IndexMap<u32, SerdeMap>>,
        IndexMap<String, IndexMap<u32, SerdeMap>>,
        IndexMap<String, IndexMap<u32, SerdeMap>>,
    ),
    String,
> {
    fn get_cell_value(row: &SerdeMap, column: &str) -> Result<String, String> {
        match row.get(column).and_then(|cell| cell.get("value")) {
            Some(SerdeValue::String(s)) => Ok(format!("{}", s)),
            Some(SerdeValue::Number(n)) => Ok(format!("{}", n)),
            Some(SerdeValue::Bool(b)) => Ok(format!("{}", b)),
            _ => Err(format!(
                "Value missing or of unknown type in column {} of row to update: {:?}",
                column, row
            )),
        }
    }

    // Collect foreign key dependencies:
    let foreign_dependencies = {
        let mut foreign_dependencies = vec![];
        let global_fconstraints = global_config
            .get("constraints")
            .and_then(|c| c.get("foreign"))
            .and_then(|c| c.as_object())
            .unwrap();
        for (dependent_table, fconstraints) in global_fconstraints {
            for entry in fconstraints.as_array().unwrap() {
                let ftable = entry.get("ftable").and_then(|c| c.as_str()).unwrap();
                if ftable == table {
                    let mut fdep = entry.as_object().unwrap().clone();
                    fdep.insert("table".to_string(), json!(dependent_table));
                    foreign_dependencies.push(fdep);
                }
            }
        }
        foreign_dependencies
    };

    let mut rows_to_update_before = IndexMap::new();
    let mut rows_to_update_after = IndexMap::new();
    for fdep in &foreign_dependencies {
        let dependent_table = fdep.get("table").and_then(|c| c.as_str()).unwrap();
        let dependent_column = fdep.get("column").and_then(|c| c.as_str()).unwrap();
        let target_column = fdep.get("fcolumn").and_then(|c| c.as_str()).unwrap();
        let target_table = fdep.get("ftable").and_then(|c| c.as_str()).unwrap();

        // Query the database using `row_number` to get the current value of the column for
        // the row.
        let updates_before = match query_as_if.kind {
            QueryAsIfKind::Add => {
                if let None = query_as_if.row {
                    eprintln!(
                        "WARN: No row in query_as_if: {:?} for {:?}",
                        query_as_if, query_as_if.kind
                    );
                }
                IndexMap::new()
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
                    global_config,
                    pool,
                    tx,
                )
                .await?
            }
        };

        let updates_after = match &query_as_if.row {
            None => {
                if query_as_if.kind != QueryAsIfKind::Remove {
                    eprintln!(
                        "WARN: No row in query_as_if: {:?} for {:?}",
                        query_as_if, query_as_if.kind
                    );
                }
                IndexMap::new()
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
                    global_config,
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
    let primaries = global_config
        .get("constraints")
        .and_then(|c| c.as_object())
        .and_then(|c| c.get("primary"))
        .and_then(|t| t.as_object())
        .and_then(|t| t.get(table))
        .and_then(|t| t.as_array())
        .and_then(|t| Some(t.iter()))
        .and_then(|t| Some(t.map(|t| t.as_str().unwrap().to_string())))
        .and_then(|t| Some(t.collect::<Vec<_>>()))
        .unwrap();
    let uniques = global_config
        .get("constraints")
        .and_then(|c| c.as_object())
        .and_then(|c| c.get("unique"))
        .and_then(|t| t.as_object())
        .and_then(|t| t.get(table))
        .and_then(|t| t.as_array())
        .and_then(|t| Some(t.iter()))
        .and_then(|t| Some(t.map(|t| t.as_str().unwrap().to_string())))
        .and_then(|t| Some(t.collect::<Vec<_>>()))
        .unwrap();
    let columns = global_config
        .get("table")
        .and_then(|t| t.get(table))
        .and_then(|t| t.as_object())
        .and_then(|t| t.get("column"))
        .and_then(|t| t.as_object())
        .and_then(|t| Some(t.keys()))
        .and_then(|k| Some(k.map(|k| k.to_string())))
        .and_then(|t| Some(t.collect::<Vec<_>>()))
        .unwrap();

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
                    eprintln!(
                        "WARN: No row in query_as_if: {:?} for {:?}",
                        query_as_if, query_as_if.kind
                    );
                }
                IndexMap::new()
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
                    global_config,
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
/// a database transaction, a number of updates to process, a [QueryAsIf] struct indicating how
/// we should modify 'in thought' the current state of the database, and a flag indicating whether
/// we should allow recursive updates, validate and then update each row indicated in `updates`.
pub async fn process_updates(
    global_config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
    tx: &mut Transaction<'_, sqlx::Any>,
    updates: &IndexMap<String, IndexMap<u32, SerdeMap>>,
    query_as_if: &QueryAsIf,
    do_not_recurse: bool,
) -> Result<(), sqlx::Error> {
    for (update_table, rows_to_update) in updates {
        for (row_number, row) in rows_to_update {
            // Validate each row 'counterfactually':
            let vrow = validate_row(
                global_config,
                compiled_datatype_conditions,
                compiled_rule_conditions,
                pool,
                Some(tx),
                update_table,
                row,
                Some(*row_number),
                Some(&query_as_if),
            )
            .await?;

            // Update the row in the database:
            update_row_tx(
                global_config,
                compiled_datatype_conditions,
                compiled_rule_conditions,
                pool,
                tx,
                update_table,
                &vrow,
                row_number,
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
/// database. Note that `from` and `to` cannot both be None.
pub async fn record_row_change(
    tx: &mut Transaction<'_, sqlx::Any>,
    table: &str,
    row_number: &u32,
    from: Option<&SerdeMap>,
    to: Option<&SerdeMap>,
    user: &str,
) -> Result<(), sqlx::Error> {
    if let (None, None) = (from, to) {
        return Err(SqlxCErr(
            "Arguments 'from' and 'to' to function record_row_change() cannot both be None".into(),
        ));
    }

    fn to_text(smap: Option<&SerdeMap>, quoted: bool) -> String {
        match smap {
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

    fn summarize(from: Option<&SerdeMap>, to: Option<&SerdeMap>) -> Result<String, String> {
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
                let numeric_re = Regex::new(r"^[0-9]*\.?[0-9]+$").unwrap();
                for (column, cell) in from.iter() {
                    let old_value = cell
                        .get("value")
                        .and_then(|v| match v {
                            SerdeValue::String(s) => Some(format!("{}", s)),
                            SerdeValue::Number(n) => Some(format!("{}", n)),
                            SerdeValue::Bool(b) => Some(format!("{}", b)),
                            _ => None,
                        })
                        .ok_or(format!("No value in {}", cell))?;
                    let new_value = to
                        .get(column)
                        .and_then(|v| v.get("value"))
                        .and_then(|v| match v {
                            SerdeValue::String(s) => Some(format!("{}", s)),
                            SerdeValue::Number(n) => Some(format!("{}", n)),
                            SerdeValue::Bool(b) => Some(format!("{}", b)),
                            _ => None,
                        })
                        .ok_or(format!("No value for column: {} in {:?}", column, to))?;
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

    let summary = summarize(from, to).map_err(|e| SqlxCErr(e.into()))?;
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

/// Given a row and a column name, extract the contents of the row as a JSON object and return it.
fn get_json_from_row(row: &AnyRow, column: &str) -> Option<SerdeMap> {
    let raw_value = row.try_get_raw(column).unwrap();
    if !raw_value.is_null() {
        let value: &str = row.get(column);
        match serde_json::from_str::<SerdeValue>(value) {
            Err(e) => {
                eprintln!("WARN: {}", e);
                None
            }
            Ok(SerdeValue::Object(value)) => Some(value),
            _ => {
                eprintln!("WARN: {} is not an object.", value);
                None
            }
        }
    } else {
        None
    }
}

/// Given a user, a history_id, a database transaction, and an undone_state indicating whether to
/// set the associated history record as undone (if undone_state == true) or as not undone
/// (otherwise). When setting the record to undone, user is used for the 'undone_by' field of the
/// history table, otherwise undone_by is set to NULL and the user is indicated as the one
/// responsible for the change (instead of whoever made the change originally).
async fn switch_undone_state(
    user: &str,
    history_id: u16,
    undone_state: bool,
    tx: &mut Transaction<'_, sqlx::Any>,
    pool: &AnyPool,
) -> Result<(), sqlx::Error> {
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

/// Given a database pool fetch the last row inserted to the history table that has not been undone.
pub async fn get_record_to_undo(pool: &AnyPool) -> Result<Option<AnyRow>, sqlx::Error> {
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

/// Given a database pool fetch the row in the history table that has been most recently marked as
/// undone.
pub async fn get_record_to_redo(pool: &AnyPool) -> Result<Option<AnyRow>, sqlx::Error> {
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

/// Given a global configuration map, maps of compiled datatype and ruled conditions, a database
/// connection pool, and the user who initiated the undo, find the last recorded change to the
/// database and undo it, indicating in the history table that undo_user is responsible.
#[async_recursion]
pub async fn undo(
    global_config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
    undo_user: &str,
) -> Result<(), sqlx::Error> {
    let last_change = match get_record_to_undo(pool).await? {
        None => {
            eprintln!("WARN: Nothing to undo.");
            return Ok(());
        }
        Some(r) => r,
    };
    let history_id: i32 = last_change.get("history_id");
    let history_id = history_id as u16;
    let table: &str = last_change.get("table");
    let row_number: i64 = last_change.get("row");
    let row_number = row_number as u32;
    let from = get_json_from_row(&last_change, "from");
    let to = get_json_from_row(&last_change, "to");

    match (from, to) {
        (None, None) => {
            return Err(SqlxCErr(
                "Cannot redo unknown operation from None to None".into(),
            ))
        }
        (None, Some(_)) => {
            // Undo an insert:
            let mut tx = pool.begin().await?;

            delete_row_tx(
                global_config,
                compiled_datatype_conditions,
                compiled_rule_conditions,
                pool,
                &mut tx,
                table,
                &row_number,
            )
            .await?;

            switch_undone_state(undo_user, history_id, true, &mut tx, pool).await?;
            tx.commit().await?;
        }
        (Some(from), None) => {
            // Undo a delete:
            let mut tx = pool.begin().await?;

            insert_new_row_tx(
                global_config,
                compiled_datatype_conditions,
                compiled_rule_conditions,
                pool,
                &mut tx,
                table,
                &from,
                Some(row_number),
                false,
            )
            .await?;

            switch_undone_state(undo_user, history_id, true, &mut tx, pool).await?;
            tx.commit().await?;
        }
        (Some(from), Some(_)) => {
            // Undo an an update:
            let mut tx = pool.begin().await?;

            update_row_tx(
                global_config,
                compiled_datatype_conditions,
                compiled_rule_conditions,
                pool,
                &mut tx,
                table,
                &from,
                &row_number,
                false,
                false,
            )
            .await?;

            switch_undone_state(undo_user, history_id, true, &mut tx, pool).await?;
            tx.commit().await?;
        }
    }
    Ok(())
}

/// Given a global configuration map, maps of compiled datatype and ruled conditions, a database
/// connection pool, and the user who initiated the redo, find the last recorded change to the
/// database that was undone and redo it, indicating in the history table that redo_user is
/// responsible for the redo.
#[async_recursion]
pub async fn redo(
    global_config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
    redo_user: &str,
) -> Result<(), sqlx::Error> {
    let last_undo = match get_record_to_redo(pool).await? {
        None => {
            eprintln!("WARN: Nothing to redo.");
            return Ok(());
        }
        Some(last_undo) => {
            let undone_by = last_undo.try_get_raw("undone_by")?;
            if undone_by.is_null() {
                eprintln!("WARN: Nothing to redo.");
                return Ok(());
            }
            last_undo
        }
    };
    let history_id: i32 = last_undo.get("history_id");
    let history_id = history_id as u16;
    let table: &str = last_undo.get("table");
    let row_number: i64 = last_undo.get("row");
    let row_number = row_number as u32;
    let from = get_json_from_row(&last_undo, "from");
    let to = get_json_from_row(&last_undo, "to");

    match (from, to) {
        (None, None) => {
            return Err(SqlxCErr(
                "Cannot redo unknown operation from None to None".into(),
            ))
        }
        (None, Some(to)) => {
            // Redo an insert:
            let mut tx = pool.begin().await?;

            insert_new_row_tx(
                global_config,
                compiled_datatype_conditions,
                compiled_rule_conditions,
                pool,
                &mut tx,
                table,
                &to,
                Some(row_number),
                false,
            )
            .await?;

            switch_undone_state(redo_user, history_id, false, &mut tx, pool).await?;
            tx.commit().await?;
        }
        (Some(_), None) => {
            // Redo a delete:
            let mut tx = pool.begin().await?;

            delete_row_tx(
                global_config,
                compiled_datatype_conditions,
                compiled_rule_conditions,
                pool,
                &mut tx,
                table,
                &row_number,
            )
            .await?;

            switch_undone_state(redo_user, history_id, false, &mut tx, pool).await?;
            tx.commit().await?;
        }
        (Some(_), Some(to)) => {
            // Redo an an update:
            let mut tx = pool.begin().await?;

            update_row_tx(
                global_config,
                compiled_datatype_conditions,
                compiled_rule_conditions,
                pool,
                &mut tx,
                table,
                &to,
                &row_number,
                false,
                false,
            )
            .await?;

            switch_undone_state(redo_user, history_id, false, &mut tx, pool).await?;
            tx.commit().await?;
        }
    }
    Ok(())
}

/// Given a global config map and a table name, return a list of the columns from the table
/// that may potentially result in database conflicts.
fn get_conflict_columns(global_config: &SerdeMap, table_name: &str) -> Vec<SerdeValue> {
    let mut conflict_columns = vec![];
    let primaries = global_config
        .get("constraints")
        .and_then(|c| c.as_object())
        .and_then(|c| c.get("primary"))
        .and_then(|t| t.as_object())
        .and_then(|t| t.get(table_name))
        .and_then(|t| t.as_array())
        .unwrap();

    let uniques = global_config
        .get("constraints")
        .and_then(|c| c.as_object())
        .and_then(|c| c.get("unique"))
        .and_then(|t| t.as_object())
        .and_then(|t| t.get(table_name))
        .and_then(|t| t.as_array())
        .unwrap();

    // We take tree-children because these imply a unique database constraint on the corresponding
    // column.
    let tree_children = global_config
        .get("constraints")
        .and_then(|c| c.as_object())
        .and_then(|o| o.get("tree"))
        .and_then(|t| t.as_object())
        .and_then(|o| o.get(table_name))
        .and_then(|t| t.as_array())
        .unwrap()
        .iter()
        .map(|v| v.as_object().unwrap())
        .map(|v| v.get("child").unwrap().clone())
        .collect::<Vec<_>>();

    let foreign_sources = global_config
        .get("constraints")
        .and_then(|c| c.as_object())
        .and_then(|o| o.get("foreign"))
        .and_then(|t| t.as_object())
        .and_then(|o| o.get(table_name))
        .and_then(|t| t.as_array())
        .unwrap()
        .iter()
        .map(|v| v.as_object().unwrap())
        .map(|v| v.get("column").unwrap().clone())
        .collect::<Vec<_>>();

    let foreign_targets = global_config
        .get("constraints")
        .and_then(|c| c.as_object())
        .and_then(|o| o.get("foreign"))
        .and_then(|t| t.as_object())
        .and_then(|o| o.get(table_name))
        .and_then(|t| t.as_array())
        .unwrap()
        .iter()
        .map(|v| v.as_object().unwrap())
        .filter(|o| o.get("ftable").unwrap().as_str() == Some(table_name))
        .map(|v| v.get("fcolumn").unwrap().clone())
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
                conflict_columns.push(column.clone());
            }
        }
    }

    conflict_columns
}

/// Given a SQL type and a value, return true if the value does not conform to the SQL type.
fn is_sql_type_error(sql_type: &str, value: &str) -> bool {
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

/// A wrapper around [insert_new_row_tx()] in which the following steps are also performed:
/// - A database transaction is created and then committed once the given new row has been inserted.
/// - The row is validated before insertion and the update to the database is recorded to the
///   history table indicating that the given user is responsible for the change.
#[async_recursion]
pub async fn insert_new_row(
    global_config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
    table: &str,
    row: &SerdeMap,
    new_row_number: Option<u32>,
    user: &str,
) -> Result<u32, sqlx::Error> {
    let mut tx = pool.begin().await?;

    let row = validate_row(
        global_config,
        compiled_datatype_conditions,
        compiled_rule_conditions,
        pool,
        Some(&mut tx),
        table,
        row,
        new_row_number,
        None,
    )
    .await?;

    let rn = insert_new_row_tx(
        global_config,
        compiled_datatype_conditions,
        compiled_rule_conditions,
        pool,
        &mut tx,
        table,
        &row,
        new_row_number,
        true,
    )
    .await?;

    record_row_change(&mut tx, table, &rn, None, Some(&row), user).await?;
    tx.commit().await?;
    Ok(rn)
}

/// Given a global config map, compiled datatype and rule conditions, a database connection pool, a
/// database transaction, a table name, and a row, assign the given new row number to the row and
/// insert it to the database using the given transaction, then return the new row number.
/// If skip_validation is set to true, omit the implicit call to [validate_row()].
#[async_recursion]
pub async fn insert_new_row_tx(
    global_config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
    tx: &mut Transaction<sqlx::Any>,
    table: &str,
    row: &SerdeMap,
    new_row_number: Option<u32>,
    skip_validation: bool,
) -> Result<u32, sqlx::Error> {
    // Send the row through the row validator to determine if any fields are problematic and
    // to mark them with appropriate messages:
    let row = if !skip_validation {
        validate_row(
            global_config,
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
    let sorted_datatypes = get_sorted_datatypes(global_config);
    let conflict_columns = get_conflict_columns(global_config, table);
    let mut use_conflict_table = false;
    for (column, cell) in row.iter() {
        insert_columns.append(&mut vec![format!(r#""{}""#, column)]);

        let cell = cell
            .as_object()
            .ok_or(SqlxCErr(format!("Cell {:?} is not an object", cell).into()))?;
        let valid = cell.get("valid").and_then(|v| v.as_bool()).ok_or(SqlxCErr(
            format!("No bool named 'valid' in {:?}", cell).into(),
        ))?;
        let value = cell.get("value").and_then(|v| v.as_str()).ok_or(SqlxCErr(
            format!("No string named 'value' in {:?}", cell).into(),
        ))?;
        let messages = sort_messages(
            &sorted_datatypes,
            cell.get("messages")
                .and_then(|m| m.as_array())
                .ok_or(SqlxCErr(
                    format!("No array named 'messages' in {:?}", cell).into(),
                ))?,
        );

        for message in messages {
            all_messages.push(json!({
                "column": column,
                "value": value,
                "level": message.get("level").and_then(|s| s.as_str())
                    .ok_or(
                        SqlxCErr(format!("No 'level' in {:?}", message).into())
                    )?,
                "rule": message.get("rule").and_then(|s| s.as_str())
                    .ok_or(
                        SqlxCErr(format!("No 'rule' in {:?}", message).into())
                    )?,
                "message": message.get("message").and_then(|s| s.as_str())
                    .ok_or(
                        SqlxCErr(format!("No 'message' in {:?}", message).into())
                    )?,
            }));
        }

        let sql_type = get_sql_type_from_global_config(global_config, table, column, pool).ok_or(
            SqlxCErr(format!("Could not get SQL type for {}.{}", table, column).into()),
        )?;
        if is_sql_type_error(&sql_type, value) {
            insert_values.push(String::from("NULL"));
        } else {
            insert_values.push(cast_sql_param_from_text(&sql_type));
            insert_params.push(String::from(value));
        }

        if !use_conflict_table && !valid && conflict_columns.contains(&json!(column)) {
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
    let (_, updates_after, _) = get_rows_to_update(global_config, pool, tx, table, &query_as_if)
        .await
        .map_err(|e| SqlxCErr(e.into()))?;

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
        let value = m.get("value").and_then(|c| c.as_str()).unwrap();
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
        global_config,
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

/// A wrapper around [delete_row_tx()] in which the database transaction is implicitly created
/// and then committed once the given row has been deleted, and the change to the database is
/// recorded in the history table indicating that the given user is responsible for the change.
#[async_recursion]
pub async fn delete_row(
    global_config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
    table: &str,
    row_number: &u32,
    user: &str,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    let row = get_row_from_db(global_config, pool, &mut tx, &table, row_number).await?;
    record_row_change(&mut tx, &table, row_number, Some(&row), None, user).await?;

    delete_row_tx(
        global_config,
        compiled_datatype_conditions,
        compiled_rule_conditions,
        pool,
        &mut tx,
        table,
        row_number,
    )
    .await?;

    tx.commit().await?;
    Ok(())
}

/// Given a global config map, maps of datatype and rule conditions, a database connection pool, a
/// database transaction, a table name, and a row number, delete the given row from the database.
#[async_recursion]
pub async fn delete_row_tx(
    global_config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
    tx: &mut Transaction<sqlx::Any>,
    table: &str,
    row_number: &u32,
) -> Result<(), sqlx::Error> {
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
        get_rows_to_update(global_config, pool, tx, table, &query_as_if)
            .await
            .map_err(|e| SqlxCErr(e.into()))?;

    // Process the updates that need to be performed before the update of the target row:
    process_updates(
        global_config,
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
        global_config,
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

/// A wrapper around [update_row_tx()] in which the database transaction is implicitly created
/// and then committed once the given row has been updated, the given row is validated before
/// the update, and the update is recorded to the history table indicating that the given user
/// is responsible for the change.
#[async_recursion]
pub async fn update_row(
    global_config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
    table_name: &str,
    row: &SerdeMap,
    row_number: &u32,
    user: &str,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    // Get the old version of the row from the database so that we can later record it to the
    // history table:
    let old_row = get_row_from_db(global_config, pool, &mut tx, table_name, row_number).await?;

    let row = validate_row(
        global_config,
        compiled_datatype_conditions,
        compiled_rule_conditions,
        pool,
        Some(&mut tx),
        table_name,
        row,
        Some(*row_number),
        None,
    )
    .await?;

    update_row_tx(
        global_config,
        compiled_datatype_conditions,
        compiled_rule_conditions,
        pool,
        &mut tx,
        table_name,
        &row,
        row_number,
        true,
        false,
    )
    .await?;

    // Record the row update in the history table:
    record_row_change(
        &mut tx,
        table_name,
        row_number,
        Some(&old_row),
        Some(&row),
        user,
    )
    .await?;

    tx.commit().await?;
    Ok(())
}

/// Given global config map, maps of compiled datatype and rule conditions, a database connection
/// pool, a database transaction, a table name, a row, and the row number to update, update the
/// corresponding row in the database. If skip_validation is set, skip the implicit call to
/// [validate_row()]. If do_not_recurse, is set, do not look for rows which could be affected by
/// this update.
#[async_recursion]
pub async fn update_row_tx(
    global_config: &SerdeMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    pool: &AnyPool,
    tx: &mut Transaction<sqlx::Any>,
    table: &str,
    row: &SerdeMap,
    row_number: &u32,
    skip_validation: bool,
    do_not_recurse: bool,
) -> Result<(), sqlx::Error> {
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
            get_rows_to_update(global_config, pool, tx, table, &query_as_if)
                .await
                .map_err(|e| SqlxCErr(e.into()))?
        }
    };

    // Process the updates that need to be performed before the update of the target row:
    process_updates(
        global_config,
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
        validate_row(
            global_config,
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
        global_config,
        compiled_datatype_conditions,
        compiled_rule_conditions,
        pool,
        tx,
        table,
        row_number,
    )
    .await?;
    insert_new_row_tx(
        global_config,
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
        global_config,
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
        global_config,
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

/// Given a path, read a TSV file and return a vector of rows represented as SerdeMaps.
/// Note: Use this function to read "small" TSVs only. In particular, use this for the special
/// configuration tables.
fn read_tsv_into_vector(path: &str) -> Vec<SerdeMap> {
    let mut rdr =
        csv::ReaderBuilder::new()
            .delimiter(b'\t')
            .from_reader(File::open(path).unwrap_or_else(|err| {
                panic!("Unable to open '{}': {}", path, err);
            }));

    let rows: Vec<_> = rdr
        .deserialize()
        .map(|result| {
            let row: SerdeMap = result.expect(format!("Error reading: {}", path).as_str());
            row
        })
        .collect();

    if rows.len() < 1 {
        panic!("No rows in {}", path);
    }

    for (i, row) in rows.iter().enumerate() {
        // enumerate() begins at 0 but we want to count rows from 1:
        let i = i + 1;
        for (col, val) in row {
            let val = val.as_str().unwrap();
            let trimmed_val = val.trim();
            if trimmed_val != val {
                eprintln!(
                    "Error: Value '{}' of column '{}' in row {} of table '{}' {}",
                    val, col, i, path, "has leading and/or trailing whitespace."
                );
                process::exit(1);
            }
        }
    }

    rows
}

/// Given a database at the specified location, query the "table" table and return a vector of rows
/// represented as SerdeMaps.
fn read_db_table_into_vector(database: &str, config_table: &str) -> Vec<SerdeMap> {
    let connection_options;
    if database.starts_with("postgresql://") {
        connection_options = AnyConnectOptions::from_str(database).unwrap();
    } else {
        let connection_string;
        if !database.starts_with("sqlite://") {
            connection_string = format!("sqlite://{}?mode=ro", database);
        } else {
            connection_string = database.to_string();
        }
        connection_options = AnyConnectOptions::from_str(connection_string.as_str()).unwrap();
    }

    let pool = block_on(
        AnyPoolOptions::new()
            .max_connections(5)
            .connect_with(connection_options),
    )
    .unwrap();

    let sql = format!("SELECT * FROM \"{}\"", config_table);
    let rows = block_on(sqlx_query(&sql).fetch_all(&pool)).unwrap();
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
    table_rows
}

/// Given a condition on a datatype, if the condition is a Function, then parse it using
/// StartParser, create a corresponding CompiledCondition, and return it. If the condition is a
/// Label, then look for the CompiledCondition corresponding to it in compiled_datatype_conditions
/// and return it.
fn compile_condition(
    condition_option: Option<&str>,
    parser: &StartParser,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
) -> CompiledCondition {
    let unquoted_re = Regex::new(r#"^['"](?P<unquoted>.*)['"]$"#).unwrap();
    match condition_option {
        // The case of no condition, or a "null" or "not null" condition, will be treated specially
        // later during the validation phase in a way that does not utilise the associated closure.
        // Since we still have to assign some closure in these cases, we use a constant closure that
        // always returns true:
        None => {
            return CompiledCondition {
                original: String::from(""),
                parsed: Expression::None,
                compiled: Arc::new(|_| true),
            }
        }
        Some("null") => {
            return CompiledCondition {
                original: String::from("null"),
                parsed: Expression::Null,
                compiled: Arc::new(|_| true),
            }
        }
        Some("not null") => {
            return CompiledCondition {
                original: String::from("not null"),
                parsed: Expression::NotNull,
                compiled: Arc::new(|_| true),
            }
        }
        Some(condition) => {
            let parsed_condition = parser.parse(condition);
            if let Err(_) = parsed_condition {
                panic!("ERROR: Could not parse condition: {}", condition);
            }
            let parsed_condition = parsed_condition.unwrap();
            if parsed_condition.len() != 1 {
                panic!(
                    "ERROR: Invalid condition: '{}'. Only one condition per column is allowed.",
                    condition
                );
            }
            let parsed_condition = &parsed_condition[0];
            match &**parsed_condition {
                Expression::Function(name, args) => {
                    if name == "equals" {
                        if let Expression::Label(label) = &*args[0] {
                            let label = String::from(unquoted_re.replace(label, "$unquoted"));
                            return CompiledCondition {
                                original: condition.to_string(),
                                parsed: *parsed_condition.clone(),
                                compiled: Arc::new(move |x| x == label),
                            };
                        } else {
                            panic!("ERROR: Invalid condition: {}", condition);
                        }
                    } else if vec!["exclude", "match", "search"].contains(&name.as_str()) {
                        if let Expression::RegexMatch(pattern, flags) = &*args[0] {
                            let mut pattern =
                                String::from(unquoted_re.replace(pattern, "$unquoted"));
                            let mut flags = String::from(flags);
                            if flags != "" {
                                flags = format!("(?{})", flags.as_str());
                            }
                            match name.as_str() {
                                "exclude" => {
                                    pattern = format!("{}{}", flags, pattern);
                                    let re = Regex::new(pattern.as_str()).unwrap();
                                    return CompiledCondition {
                                        original: condition.to_string(),
                                        parsed: *parsed_condition.clone(),
                                        compiled: Arc::new(move |x| !re.is_match(x)),
                                    };
                                }
                                "match" => {
                                    pattern = format!("^{}{}$", flags, pattern);
                                    let re = Regex::new(pattern.as_str()).unwrap();
                                    return CompiledCondition {
                                        original: condition.to_string(),
                                        parsed: *parsed_condition.clone(),
                                        compiled: Arc::new(move |x| re.is_match(x)),
                                    };
                                }
                                "search" => {
                                    pattern = format!("{}{}", flags, pattern);
                                    let re = Regex::new(pattern.as_str()).unwrap();
                                    return CompiledCondition {
                                        original: condition.to_string(),
                                        parsed: *parsed_condition.clone(),
                                        compiled: Arc::new(move |x| re.is_match(x)),
                                    };
                                }
                                _ => panic!("Unrecognized function name: {}", name),
                            };
                        } else {
                            panic!(
                                "Argument to condition: {} is not a regular expression",
                                condition
                            );
                        }
                    } else if name == "in" {
                        let mut alternatives: Vec<String> = vec![];
                        for arg in args {
                            if let Expression::Label(value) = &**arg {
                                let value = unquoted_re.replace(value, "$unquoted");
                                alternatives.push(value.to_string());
                            } else {
                                panic!("Argument: {:?} to function 'in' is not a label", arg);
                            }
                        }
                        return CompiledCondition {
                            original: condition.to_string(),
                            parsed: *parsed_condition.clone(),
                            compiled: Arc::new(move |x| alternatives.contains(&x.to_string())),
                        };
                    } else {
                        panic!("Unrecognized function name: {}", name);
                    }
                }
                Expression::Label(value)
                    if compiled_datatype_conditions.contains_key(&value.to_string()) =>
                {
                    let compiled_datatype_condition = compiled_datatype_conditions
                        .get(&value.to_string())
                        .unwrap();
                    return CompiledCondition {
                        original: value.to_string(),
                        parsed: compiled_datatype_condition.parsed.clone(),
                        compiled: compiled_datatype_condition.compiled.clone(),
                    };
                }
                _ => {
                    panic!("Unrecognized condition: {}", condition);
                }
            };
        }
    };
}

/// Given the config map, the name of a datatype, and a database connection pool used to determine
/// the database type, climb the datatype tree (as required), and return the first 'SQL type' found.
fn get_sql_type(dt_config: &SerdeMap, datatype: &String, pool: &AnyPool) -> Option<String> {
    if !dt_config.contains_key(datatype) {
        return None;
    }

    let sql_type_column = {
        if pool.any_kind() == AnyKind::Sqlite {
            "SQLite type"
        } else {
            "PostgreSQL type"
        }
    };

    if let Some(sql_type) = dt_config.get(datatype).and_then(|d| d.get(sql_type_column)) {
        return Some(sql_type.as_str().and_then(|s| Some(s.to_string())).unwrap());
    }

    let parent_datatype = dt_config
        .get(datatype)
        .and_then(|d| d.get("parent"))
        .and_then(|p| p.as_str())
        .unwrap();

    return get_sql_type(dt_config, &parent_datatype.to_string(), pool);
}

/// Given the global config map, a table name, a column name, and a database connection pool
/// used to determine the database type return the column's SQL type.
pub fn get_sql_type_from_global_config(
    global_config: &SerdeMap,
    table: &str,
    column: &str,
    pool: &AnyPool,
) -> Option<String> {
    let dt_config = global_config
        .get("datatype")
        .and_then(|d| d.as_object())
        .unwrap();
    let dt = global_config
        .get("table")
        .and_then(|t| t.get(table))
        .and_then(|t| t.get("column"))
        .and_then(|c| c.get(column))
        .and_then(|c| c.get("datatype"))
        .and_then(|d| d.as_str())
        .and_then(|d| Some(d.to_string()))
        .expect(&format!("Could not get datatype for {}.{}", table, column));
    get_sql_type(&dt_config, &dt, pool)
}

/// Given a SQL type, return the appropriate CAST(...) statement for casting the SQL_PARAM
/// from a TEXT column.
fn cast_sql_param_from_text(sql_type: &str) -> String {
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
fn cast_column_sql_to_text(column: &str, sql_type: &str) -> String {
    if sql_type.to_lowercase() == "text" {
        format!(r#""{}""#, column)
    } else {
        format!(r#"CAST("{}" AS TEXT)"#, column)
    }
}

/// Given a database row, the name of a column, and it's SQL type, return the value of that column
/// from the given row as a String.
fn get_column_value(row: &AnyRow, column: &str, sql_type: &str) -> String {
    let s = sql_type.to_lowercase();
    if s == "numeric" {
        let value: f64 = row.get(format!(r#"{}"#, column).as_str());
        value.to_string()
    } else if s == "integer" {
        let value: i32 = row.get(format!(r#"{}"#, column).as_str());
        value.to_string()
    } else if s == "real" {
        let value: f64 = row.get(format!(r#"{}"#, column).as_str());
        value.to_string()
    } else {
        let value: &str = row.get(format!(r#"{}"#, column).as_str());
        value.to_string()
    }
}

/// Given a SQL string, possibly with unbound parameters represented by the placeholder string
/// SQL_PARAM, and given a database pool, if the pool is of type Sqlite, then change the syntax used
/// for unbound parameters to Sqlite syntax, which uses "?", otherwise use Postgres syntax, which
/// uses numbered parameters, i.e., $1, $2, ...
fn local_sql_syntax(pool: &AnyPool, sql: &String) -> String {
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

/// Takes as arguments a list of tables and a configuration map describing all of the constraints
/// between tables. After validating that there are no cycles amongst the foreign, tree, and
/// under dependencies, returns the list of tables sorted according to their foreign key
/// dependencies, such that if table_a depends on table_b, then table_b comes before table_a in the
/// list that is returned.
fn verify_table_deps_and_sort(table_list: &Vec<String>, constraints: &SerdeMap) -> Vec<String> {
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

    let trees = constraints.get("tree").and_then(|t| t.as_object()).unwrap();
    for table_name in table_list {
        let mut dependency_graph = DiGraphMap::<&str, ()>::new();
        let table_trees = trees.get(table_name).and_then(|t| t.as_array()).unwrap();
        for tree in table_trees {
            let tree = tree.as_object().unwrap();
            let child = tree.get("child").and_then(|c| c.as_str()).unwrap();
            let parent = tree.get("parent").and_then(|p| p.as_str()).unwrap();
            let c_index = dependency_graph.add_node(child);
            let p_index = dependency_graph.add_node(parent);
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
                            let dep = table_trees
                                .iter()
                                .find(|d| d.get("child").unwrap().as_str() == Some(child))
                                .and_then(|d| d.as_object())
                                .unwrap();
                            let parent = dep.get("parent").unwrap();
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

    let foreign_keys = constraints
        .get("foreign")
        .and_then(|f| f.as_object())
        .unwrap();
    let under_keys = constraints
        .get("under")
        .and_then(|u| u.as_object())
        .unwrap();
    let mut dependency_graph = DiGraphMap::<&str, ()>::new();
    for table_name in table_list {
        let t_index = dependency_graph.add_node(table_name);
        let fkeys = foreign_keys
            .get(table_name)
            .and_then(|f| f.as_array())
            .unwrap();
        for fkey in fkeys {
            let ftable = fkey.get("ftable").and_then(|f| f.as_str()).unwrap();
            let f_index = dependency_graph.add_node(ftable);
            dependency_graph.add_edge(t_index, f_index, ());
        }

        let ukeys = under_keys
            .get(table_name)
            .and_then(|u| u.as_array())
            .unwrap();
        for ukey in ukeys {
            let ttable = ukey.get("ttable").and_then(|t| t.as_str()).unwrap();
            let tcolumn = ukey.get("tcolumn").and_then(|t| t.as_str()).unwrap();
            let value = ukey.get("value").and_then(|t| t.as_str()).unwrap();
            if ttable != table_name {
                let ttable_trees = trees.get(ttable).and_then(|t| t.as_array()).unwrap();
                if ttable_trees
                    .iter()
                    .filter(|d| d.get("child").unwrap().as_str() == Some(tcolumn))
                    .collect::<Vec<_>>()
                    .is_empty()
                {
                    panic!(
                        "under({}.{}, {}) refers to a non-existent tree",
                        ttable, tcolumn, value
                    );
                }
                let tt_index = dependency_graph.add_node(ttable);
                dependency_graph.add_edge(t_index, tt_index, ());
            }
        }
    }

    match get_cycles(&dependency_graph) {
        Ok(sorted_table_list) => {
            return sorted_table_list;
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
                        let fkeys = foreign_keys.get(table).and_then(|f| f.as_array()).unwrap();
                        let ukeys = under_keys.get(table).and_then(|u| u.as_array()).unwrap();
                        let column;
                        let ref_table;
                        let ref_column;
                        if let Some(dep) = fkeys
                            .iter()
                            .find(|d| d.get("ftable").unwrap().as_str() == Some(dep_name))
                            .and_then(|d| d.as_object())
                        {
                            column = dep.get("column").unwrap();
                            ref_table = dep.get("ftable").unwrap();
                            ref_column = dep.get("fcolumn").unwrap();
                        } else if let Some(dep) = ukeys
                            .iter()
                            .find(|d| d.get("ttable").unwrap().as_str() == Some(dep_name))
                            .and_then(|d| d.as_object())
                        {
                            column = dep.get("column").unwrap();
                            ref_table = dep.get("ttable").unwrap();
                            ref_column = dep.get("tcolumn").unwrap();
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

/// Given the config maps for tables and datatypes, and a table name, generate a SQL schema string,
/// including each column C and its matching C_meta column, then return the schema string as well as
/// a list of the table's constraints.
fn create_table_statement(
    tables_config: &mut SerdeMap,
    datatypes_config: &mut SerdeMap,
    parser: &StartParser,
    table_name: &String,
    pool: &AnyPool,
) -> (Vec<String>, SerdeValue) {
    let mut drop_table_sql = format!(r#"DROP TABLE IF EXISTS "{}""#, table_name);
    if pool.any_kind() == AnyKind::Postgres {
        drop_table_sql.push_str(" CASCADE");
    }
    drop_table_sql.push_str(";");
    let mut statements = vec![drop_table_sql];
    let mut create_lines = vec![
        format!(r#"CREATE TABLE "{}" ("#, table_name),
        String::from(r#"  "row_number" BIGINT,"#),
    ];

    let normal_table_name;
    if let Some(s) = table_name.strip_suffix("_conflict") {
        normal_table_name = String::from(s);
    } else {
        normal_table_name = table_name.to_string();
    }

    let column_names = tables_config
        .get(&normal_table_name)
        .and_then(|t| t.get("column_order"))
        .and_then(|c| c.as_array())
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect::<Vec<_>>();

    let columns = tables_config
        .get(normal_table_name.as_str())
        .and_then(|c| c.as_object())
        .and_then(|o| o.get("column"))
        .and_then(|c| c.as_object())
        .unwrap();

    let mut table_constraints = json!({
        "foreign": [],
        "unique": [],
        "primary": [],
        "tree": [],
        "under": [],
    });

    let mut colvals: Vec<SerdeMap> = vec![];
    for column_name in &column_names {
        let column = columns
            .get(column_name)
            .and_then(|c| c.as_object())
            .unwrap();
        colvals.push(column.clone());
    }

    let c = colvals.len();
    let mut r = 0;
    for row in colvals {
        r += 1;
        let sql_type = get_sql_type(
            datatypes_config,
            &row.get("datatype")
                .and_then(|d| d.as_str())
                .and_then(|s| Some(s.to_string()))
                .unwrap(),
            pool,
        );

        if let None = sql_type {
            panic!("Missing SQL type for {}", row.get("datatype").unwrap());
        }
        let sql_type = sql_type.unwrap();

        let short_sql_type = {
            if sql_type.to_lowercase().as_str().starts_with("varchar(") {
                "VARCHAR"
            } else {
                &sql_type
            }
        };

        if pool.any_kind() == AnyKind::Postgres {
            if !PG_SQL_TYPES.contains(&short_sql_type.to_lowercase().as_str()) {
                panic!(
                    "Unrecognized PostgreSQL SQL type '{}' for datatype: '{}'. \
                     Accepted SQL types for PostgreSQL are: {}",
                    sql_type,
                    row.get("datatype").and_then(|d| d.as_str()).unwrap(),
                    PG_SQL_TYPES.join(", ")
                );
            }
        } else {
            if !SL_SQL_TYPES.contains(&short_sql_type.to_lowercase().as_str()) {
                panic!(
                    "Unrecognized SQLite SQL type '{}' for datatype '{}'. \
                     Accepted SQL datatypes for SQLite are: {}",
                    sql_type,
                    row.get("datatype").and_then(|d| d.as_str()).unwrap(),
                    SL_SQL_TYPES.join(", ")
                );
            }
        }

        let column_name = row.get("column").and_then(|s| s.as_str()).unwrap();
        let mut line = format!(r#"  "{}" {}"#, column_name, sql_type);
        let structure = row.get("structure").and_then(|s| s.as_str());
        if let Some(structure) = structure {
            if structure != "" && !table_name.ends_with("_conflict") {
                let parsed_structure = parser.parse(structure).unwrap();
                for expression in parsed_structure {
                    match *expression {
                        Expression::Label(value) if value == "primary" => {
                            line.push_str(" PRIMARY KEY");
                            let primary_keys = table_constraints
                                .get_mut("primary")
                                .and_then(|v| v.as_array_mut())
                                .unwrap();
                            primary_keys.push(SerdeValue::String(column_name.to_string()));
                        }
                        Expression::Label(value) if value == "unique" => {
                            line.push_str(" UNIQUE");
                            let unique_constraints = table_constraints
                                .get_mut("unique")
                                .and_then(|v| v.as_array_mut())
                                .unwrap();
                            unique_constraints.push(SerdeValue::String(column_name.to_string()));
                        }
                        Expression::Function(name, args) if name == "from" => {
                            if args.len() != 1 {
                                panic!("Invalid foreign key: {} for: {}", structure, table_name);
                            }
                            match &*args[0] {
                                Expression::Field(ftable, fcolumn) => {
                                    let foreign_keys = table_constraints
                                        .get_mut("foreign")
                                        .and_then(|v| v.as_array_mut())
                                        .unwrap();
                                    let foreign_key = json!({
                                        "column": column_name,
                                        "ftable": ftable,
                                        "fcolumn": fcolumn,
                                    });
                                    foreign_keys.push(foreign_key);
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
                                        .and_then(|c| c.get("datatype"))
                                        .and_then(|d| d.as_str());
                                    if let None = child_datatype {
                                        panic!(
                                            "Could not determine SQL datatype for {} of tree({})",
                                            child, child
                                        );
                                    }
                                    let child_datatype = child_datatype.unwrap();
                                    let parent = column_name;
                                    let child_sql_type = get_sql_type(
                                        datatypes_config,
                                        &child_datatype.to_string(),
                                        pool,
                                    )
                                    .unwrap();
                                    if sql_type != child_sql_type {
                                        panic!(
                                            "SQL type '{}' of '{}' in 'tree({})' for table \
                                             '{}' doe snot match SQL type: '{}' of parent: '{}'.",
                                            child_sql_type,
                                            child,
                                            child,
                                            table_name,
                                            sql_type,
                                            parent
                                        );
                                    }
                                    let tree_constraints = table_constraints
                                        .get_mut("tree")
                                        .and_then(|t| t.as_array_mut())
                                        .unwrap();
                                    let entry = json!({"parent": column_name,
                                                       "child": child});
                                    tree_constraints.push(entry);
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
                                    let under_constraints = table_constraints
                                        .get_mut("under")
                                        .and_then(|u| u.as_array_mut())
                                        .unwrap();
                                    let entry = json!({"column": column_name,
                                                       "ttable": ttable,
                                                       "tcolumn": tcolumn,
                                                       "value": value});
                                    under_constraints.push(entry);
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
        if r >= c
            && table_constraints
                .get("foreign")
                .and_then(|v| v.as_array())
                .and_then(|v| Some(v.is_empty()))
                .unwrap()
        {
            line.push_str("");
        } else {
            line.push_str(",");
        }
        create_lines.push(line);
    }

    let foreign_keys = table_constraints
        .get("foreign")
        .and_then(|v| v.as_array())
        .unwrap();
    let num_fkeys = foreign_keys.len();
    for (i, fkey) in foreign_keys.iter().enumerate() {
        create_lines.push(format!(
            r#"  FOREIGN KEY ("{}") REFERENCES "{}"("{}"){}"#,
            fkey.get("column").and_then(|s| s.as_str()).unwrap(),
            fkey.get("ftable").and_then(|s| s.as_str()).unwrap(),
            fkey.get("fcolumn").and_then(|s| s.as_str()).unwrap(),
            if i < (num_fkeys - 1) { "," } else { "" }
        ));
    }
    create_lines.push(String::from(");"));
    // We are done generating the lines for the 'create table' statement. Join them and add the
    // result to the statements to return:
    statements.push(String::from(create_lines.join("\n")));

    // Loop through the tree constraints and if any of their associated child columns do not already
    // have an associated unique or primary index, create one implicitly here:
    let tree_constraints = table_constraints
        .get("tree")
        .and_then(|v| v.as_array())
        .unwrap();
    for tree in tree_constraints {
        let unique_keys = table_constraints
            .get("unique")
            .and_then(|v| v.as_array())
            .unwrap();
        let primary_keys = table_constraints
            .get("primary")
            .and_then(|v| v.as_array())
            .unwrap();
        let tree_child = tree.get("child").and_then(|c| c.as_str()).unwrap();
        if !unique_keys.contains(&SerdeValue::String(tree_child.to_string()))
            && !primary_keys.contains(&SerdeValue::String(tree_child.to_string()))
        {
            statements.push(format!(
                r#"CREATE UNIQUE INDEX "{}_{}_idx" ON "{}"("{}");"#,
                table_name, tree_child, table_name, tree_child
            ));
        }
    }

    // Finally, create a further unique index on row_number:
    statements.push(format!(
        r#"CREATE UNIQUE INDEX "{}_row_number_idx" ON "{}"("row_number");"#,
        table_name, table_name
    ));

    return (statements, table_constraints);
}

/// Given a list of messages and a HashMap, messages_stats, with which to collect counts of
/// message types, count the various message types encountered in the list and increment the counts
/// in messages_stats accordingly.
fn add_message_counts(messages: &Vec<SerdeValue>, messages_stats: &mut HashMap<String, usize>) {
    for message in messages {
        let message = message.as_object().unwrap();
        let level = message.get("level").unwrap();
        if level == "error" {
            let current_errors = messages_stats.get("error").unwrap();
            messages_stats.insert("error".to_string(), current_errors + 1);
        } else if level == "warning" {
            let current_warnings = messages_stats.get("warning").unwrap();
            messages_stats.insert("warning".to_string(), current_warnings + 1);
        } else if level == "info" {
            let current_infos = messages_stats.get("info").unwrap();
            messages_stats.insert("info".to_string(), current_infos + 1);
        } else {
            eprintln!("Warning: unknown message type: {}", level);
        }
    }
}

/// Given a global config map, return a list of defined datatype names sorted from the most generic
/// to the most specific. This function will panic if circular dependencies are encountered.
fn get_sorted_datatypes(global_config: &SerdeMap) -> Vec<&str> {
    let mut graph = DiGraphMap::<&str, ()>::new();
    let dt_config = global_config
        .get("datatype")
        .and_then(|d| d.as_object())
        .unwrap();
    for (dt_name, dt_obj) in dt_config.iter() {
        let d_index = graph.add_node(dt_name);
        if let Some(parent) = dt_obj.get("parent").and_then(|p| p.as_str()) {
            let p_index = graph.add_node(parent);
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
fn sort_messages(sorted_datatypes: &Vec<&str>, cell_messages: &Vec<SerdeValue>) -> Vec<SerdeValue> {
    let mut datatype_messages = vec![];
    let mut structure_messages = vec![];
    let mut rule_messages = vec![];
    for message in cell_messages {
        let rule = message
            .get("rule")
            .and_then(|r| Some(r.as_str().unwrap().splitn(2, ":").collect::<Vec<_>>()))
            .unwrap();
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
                    .filter(|m| {
                        m.get("rule").and_then(|r| r.as_str()).unwrap()
                            == format!("datatype:{}", datatype)
                    })
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

/// Given a configuration map, a table name, a number of rows, their corresponding chunk number,
/// and a database connection pool used to determine the database type, return two four-place
/// tuples, corresponding to the normal and conflict tables, respectively. Each of these contains
/// (i) a SQL string for an insert statement to the table, (ii) parameters to bind to that SQL
/// statement, (iii) a SQL string for an insert statement the message table, and (iv) parameters
/// to bind to that SQL statement. If the verbose flag is set, the number of errors, warnings,
/// and information messages generated are added to messages_stats, the contents of which will
/// later be written to stderr.
async fn make_inserts(
    config: &SerdeMap,
    table_name: &String,
    rows: &mut Vec<ResultRow>,
    chunk_number: usize,
    messages_stats: &mut HashMap<String, usize>,
    verbose: bool,
    pool: &AnyPool,
) -> Result<
    (
        String,
        Vec<String>,
        String,
        Vec<String>,
        String,
        Vec<String>,
    ),
    sqlx::Error,
> {
    fn is_conflict_row(row: &ResultRow, conflict_columns: &Vec<SerdeValue>) -> bool {
        for (column, cell) in &row.contents {
            let column = SerdeValue::String(column.to_string());
            if !cell.valid && conflict_columns.contains(&column) {
                return true;
            }
        }
        return false;
    }

    fn generate_sql(
        config: &SerdeMap,
        main_table: &String,
        columns: &Vec<String>,
        rows: &mut Vec<ResultRow>,
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
                let sql_type =
                    get_sql_type_from_global_config(config, &main_table, column, pool).unwrap();
                if cell.nulltype != None || is_sql_type_error(&sql_type, &cell.value) {
                    row_values.push(String::from("NULL"));
                } else {
                    row_values.push(cast_sql_param_from_text(&sql_type));
                    row_params.push(cell.value.clone());
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

                    let message = message.as_object().unwrap();
                    message_params.push(main_table.clone());
                    message_params.push(column.clone());
                    message_params.push(cell.value.clone());
                    message_params.push(
                        message
                            .get("level")
                            .and_then(|s| s.as_str())
                            .unwrap()
                            .to_string(),
                    );
                    message_params.push(
                        message
                            .get("rule")
                            .and_then(|s| s.as_str())
                            .unwrap()
                            .to_string(),
                    );
                    message_params.push(
                        message
                            .get("message")
                            .and_then(|s| s.as_str())
                            .unwrap()
                            .to_string(),
                    );
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
    let column_names = config
        .get("table")
        .and_then(|t| t.get(table_name))
        .and_then(|t| t.get("column_order"))
        .and_then(|c| c.as_array())
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect::<Vec<_>>();

    let (main_sql, main_params, conflict_sql, conflict_params, message_sql, message_params) =
        generate_sql(
            &config,
            &table_name,
            &column_names,
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

/// Given a configuration map, a database connection pool, a table name, some rows to validate,
/// and the chunk number corresponding to the rows, do inter-row validation on the rows and insert
/// them to the table. If the verbose flag is set to true, error/warning/info stats will be
/// collected in messages_stats and later written to stderr.
async fn validate_rows_inter_and_insert(
    config: &SerdeMap,
    pool: &AnyPool,
    table_name: &String,
    rows: &mut Vec<ResultRow>,
    chunk_number: usize,
    messages_stats: &mut HashMap<String, usize>,
    verbose: bool,
) -> Result<(), sqlx::Error> {
    // First, do the tree validation:
    validate_rows_trees(config, pool, table_name, rows).await?;

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
        Err(_) => {
            validate_rows_constraints(config, pool, table_name, rows).await?;
            let (main_sql, main_params, conflict_sql, conflict_params, message_sql, message_params) =
                make_inserts(
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
        }
    };

    Ok(())
}

/// Given a configuration map, a database connection pool, maps for compiled datatype and rule
/// conditions, a table name, a number of chunks of rows to insert into the table in the database,
/// and the headers of the rows to be inserted, validate each chunk and insert the validated rows
/// to the table. If the verbose flag is set to true, error/warning/info stats will be collected in
/// messages_stats and later written to stderr.
async fn validate_and_insert_chunks(
    config: &SerdeMap,
    pool: &AnyPool,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    table_name: &String,
    chunks: &IntoChunks<csv::StringRecordsIter<'_, std::fs::File>>,
    headers: &csv::StringRecord,
    messages_stats: &mut HashMap<String, usize>,
    verbose: bool,
) -> Result<(), sqlx::Error> {
    if !MULTI_THREADED {
        for (chunk_number, chunk) in chunks.into_iter().enumerate() {
            let mut rows: Vec<_> = chunk.collect();
            let mut intra_validated_rows = validate_rows_intra(
                config,
                compiled_datatype_conditions,
                compiled_rule_conditions,
                table_name,
                headers,
                &mut rows,
            );
            validate_rows_inter_and_insert(
                config,
                pool,
                table_name,
                &mut intra_validated_rows,
                chunk_number,
                messages_stats,
                verbose,
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
                        validate_rows_intra(
                            config,
                            compiled_datatype_conditions,
                            compiled_rule_conditions,
                            table_name,
                            headers,
                            &mut rows,
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
                validate_rows_inter_and_insert(
                    config,
                    pool,
                    table_name,
                    &mut intra_validated_rows,
                    chunk_number,
                    messages_stats,
                    verbose,
                )
                .await?;
            }
        }

        Ok(())
    }
}

/// Given a configuration map, a database connection pool, a parser, HashMaps representing
/// compiled datatype and rule conditions, and a HashMap representing parsed structure conditions,
/// read in the data TSV files corresponding to each configured table, then validate and load all of
/// the corresponding data rows. If the verbose flag is set to true, output progress messages to
/// stderr during load.
async fn load_db(
    config: &SerdeMap,
    pool: &AnyPool,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    verbose: bool,
) -> Result<(), sqlx::Error> {
    let mut table_list = vec![];
    for table in config
        .get("sorted_table_list")
        .and_then(|l| l.as_array())
        .unwrap()
    {
        table_list.push(table.as_str().and_then(|s| Some(s.to_string())).unwrap());
    }
    let table_list = table_list; // Change the table_list to read only after populating it.
    let num_tables = table_list.len();
    let mut total_errors = 0;
    let mut total_warnings = 0;
    let mut total_infos = 0;
    let mut table_num = 1;
    for table_name in table_list {
        let path = String::from(
            config
                .get("table")
                .and_then(|t| t.as_object())
                .and_then(|o| o.get(&table_name))
                .and_then(|n| n.get("path"))
                .and_then(|p| p.as_str())
                .unwrap(),
        );
        let mut rdr = {
            match File::open(path.clone()) {
                Err(e) => {
                    eprintln!("WARN: Unable to open '{}': {}", path.clone(), e);
                    continue;
                }
                Ok(table_file) => csv::ReaderBuilder::new()
                    .has_headers(false)
                    .delimiter(b'\t')
                    .from_reader(table_file),
            }
        };
        if verbose {
            eprintln!(
                "{} - Loading table {}/{}: {}",
                Utc::now(),
                table_num,
                num_tables,
                table_name
            );
        }
        table_num += 1;

        // Extract the headers, which we will need later:
        let mut records = rdr.records();
        let headers;
        if let Some(result) = records.next() {
            headers = result.unwrap();
        } else {
            panic!("'{}' is empty", path);
        }

        for header in headers.iter() {
            if header.trim().is_empty() {
                panic!(
                    "One or more of the header fields is empty for table '{}'",
                    table_name
                );
            }
        }

        // HashMap used to report info about the number of error/warning/info messages for this
        // table when the verbose flag is set to true:
        let mut messages_stats = HashMap::new();
        messages_stats.insert("error".to_string(), 0);
        messages_stats.insert("warning".to_string(), 0);
        messages_stats.insert("info".to_string(), 0);

        // Split the data into chunks of size CHUNK_SIZE before passing them to the validation
        // logic:
        let chunks = records.chunks(CHUNK_SIZE);
        validate_and_insert_chunks(
            config,
            pool,
            compiled_datatype_conditions,
            compiled_rule_conditions,
            &table_name,
            &chunks,
            &headers,
            &mut messages_stats,
            verbose,
        )
        .await?;

        // We need to wait until all of the rows for a table have been loaded before validating the
        // "foreign" constraints on a table's trees, since this checks if the values of one column
        // (the tree's parent) are all contained in another column (the tree's child):
        // We also need to wait before validating a table's "under" constraints. Although the tree
        // associated with such a constraint need not be defined on the same table, it can be.
        let mut recs_to_update =
            validate_tree_foreign_keys(config, pool, None, &table_name, None).await?;
        recs_to_update.append(&mut validate_under(config, pool, None, &table_name, None).await?);

        for record in recs_to_update {
            let row_number = record.get("row_number").unwrap();
            let column_name = record.get("column").and_then(|s| s.as_str()).unwrap();
            let value = record.get("value").and_then(|s| s.as_str()).unwrap();
            let level = record.get("level").and_then(|s| s.as_str()).unwrap();
            let rule = record.get("rule").and_then(|s| s.as_str()).unwrap();
            let message = record.get("message").and_then(|s| s.as_str()).unwrap();

            let sql = local_sql_syntax(
                &pool,
                &format!(
                    r#"INSERT INTO "message"
                       ("table", "row", "column", "value", "level", "rule", "message")
                       VALUES ({}, {}, {}, {}, {}, {}, {})"#,
                    SQL_PARAM, row_number, SQL_PARAM, SQL_PARAM, SQL_PARAM, SQL_PARAM, SQL_PARAM
                ),
            );
            let mut query = sqlx_query(&sql);
            query = query.bind(&table_name);
            query = query.bind(&column_name);
            query = query.bind(&value);
            query = query.bind(&level);
            query = query.bind(&rule);
            query = query.bind(&message);
            query.execute(pool).await?;

            if verbose {
                // Add the generated message to messages_stats:
                let messages = vec![json!({
                    "message": message,
                    "level": level,
                })];
                add_message_counts(&messages, &mut messages_stats);
            }
        }

        if verbose {
            // Output a report on the messages generated to stderr:
            let errors = messages_stats.get("error").unwrap();
            let warnings = messages_stats.get("warning").unwrap();
            let infos = messages_stats.get("info").unwrap();
            let status_message = format!(
                "{} errors, {} warnings, and {} information messages generated for {}",
                errors, warnings, infos, table_name
            );
            eprintln!("{} - {}", Utc::now(), status_message);
            total_errors += errors;
            total_warnings += warnings;
            total_infos += infos;
        }
    }

    if verbose {
        eprintln!(
            "{} - Loading complete with {} errors, {} warnings, and {} information messages",
            Utc::now(),
            total_errors,
            total_warnings,
            total_infos
        );
    }

    Ok(())
}
