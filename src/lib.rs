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

mod ast;
pub mod validate;

lalrpop_mod!(pub valve_grammar);

use crate::validate::{
    validate_rows_constraints, validate_rows_intra, validate_rows_trees,
    validate_tree_foreign_keys, validate_under, ResultRow,
};
use crate::{ast::Expression, valve_grammar::StartParser};
use chrono::Utc;
use crossbeam;
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
    query as sqlx_query, Row, ValueRef,
};
use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
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
    // TODO: include synonyms?
    static ref SQL_TYPES: Vec<&'static str> = vec!["text", "integer", "real", "blob"];
}

/// An alias for [serde_json::Map](..//serde_json/struct.Map.html)<String, [serde_json::Value](../serde_json/enum.Value.html)>.
// Note: serde_json::Map is
// [backed by a BTreeMap by default](https://docs.serde.rs/serde_json/map/index.html)
pub type ConfigMap = serde_json::Map<String, SerdeValue>;

/// Represents a structure such as those found in the `structure` column of the `column` table in
/// both its parsed format (i.e., as an [Expression](ast/enum.Expression.html)) as well as in its
/// original format (i.e., as a plain String).
#[derive(Clone)]
pub struct ParsedStructure {
    original: String,
    parsed: Expression,
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
    original: String,
    parsed: Expression,
    compiled: Arc<dyn Fn(&str) -> bool + Sync + Send>,
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
pub struct ColumnRule {
    when: CompiledCondition,
    then: CompiledCondition,
}

// We use Debug here instead of Display because we have only implemented Debug for Expressions.
// See the comment about this in ast.rs.
impl std::fmt::Debug for ColumnRule {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{{\"column_rule\": {{\"when\": {:?}, \"then\": {:?}}}}}", &self.when, &self.then)
    }
}

/// Given the path to a table.tsv file, load and check the 'table', 'column', and 'datatype'
/// tables, and return ConfigMaps corresponding to specials, tables, datatypes, and rules.
pub fn read_config_files(path: &str) -> (ConfigMap, ConfigMap, ConfigMap, ConfigMap) {
    let special_table_types = json!({
        "table": {"required": true},
        "column": {"required": true},
        "datatype": {"required": true},
        "rule": {"required": false},
    });
    let special_table_types = special_table_types.as_object().unwrap();

    // Initialize the special table entries in the specials config map:
    let mut specials_config = ConfigMap::new();
    for t in special_table_types.keys() {
        specials_config.insert(t.to_string(), SerdeValue::Null);
    }

    // Load the table table from the given path:
    let mut tables_config = ConfigMap::new();
    let rows = read_tsv_into_vector(path);
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
                if row_path != path {
                    panic!(
                        "Special 'table' path '{}' does not match this path '{}'",
                        row_path, path
                    );
                }
            }

            if special_table_types.contains_key(row_type) {
                match specials_config.get(row_type) {
                    Some(SerdeValue::Null) => (),
                    _ => panic!("Multiple tables with type '{}' declared in '{}'", row_type, path),
                }
                let row_table = row.get("table").and_then(|t| t.as_str()).unwrap();
                specials_config
                    .insert(row_type.to_string(), SerdeValue::String(row_table.to_string()));
            } else {
                panic!("Unrecognized table type '{}' in '{}'", row_type, path);
            }
        }

        row.insert(String::from("column"), SerdeValue::Object(ConfigMap::new()));
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

    // Load datatype table
    let mut datatypes_config = ConfigMap::new();
    let table_name = specials_config.get("datatype").and_then(|d| d.as_str()).unwrap();
    let path = String::from(
        tables_config.get(table_name).and_then(|t| t.get("path")).and_then(|p| p.as_str()).unwrap(),
    );
    let rows = read_tsv_into_vector(&path.to_string());
    for mut row in rows {
        for column in vec!["datatype", "parent", "condition", "SQL type"] {
            if !row.contains_key(column) || row.get(column) == None {
                panic!("Missing required column '{}' reading '{}'", column, path);
            }
        }

        for column in vec!["datatype"] {
            if row.get(column).and_then(|c| c.as_str()).unwrap() == "" {
                panic!("Missing required value for '{}' reading '{}'", column, path);
            }
        }

        for column in vec!["parent", "condition", "SQL type"] {
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
    let table_name = specials_config.get("column").and_then(|d| d.as_str()).unwrap();
    let path = String::from(
        tables_config.get(table_name).and_then(|t| t.get("path")).and_then(|p| p.as_str()).unwrap(),
    );
    let rows = read_tsv_into_vector(&path.to_string());
    for mut row in rows {
        for column in vec!["table", "column", "nulltype", "datatype"] {
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
    let mut rules_config = ConfigMap::new();
    if let Some(SerdeValue::String(table_name)) = specials_config.get("rule") {
        let path = String::from(
            tables_config
                .get(table_name)
                .and_then(|t| t.get("path"))
                .and_then(|p| p.as_str())
                .unwrap(),
        );
        let rows = read_tsv_into_vector(&path.to_string());
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
                rules_config.insert(String::from(row_table), SerdeValue::Object(ConfigMap::new()));
            }

            let table_rule_config =
                rules_config.get_mut(row_table).and_then(|t| t.as_object_mut()).unwrap();
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

    // Finally, return all the configs:
    (specials_config, tables_config, datatypes_config, rules_config)
}

/// Given the global configuration map and a parser, compile all of the datatype conditions,
/// add them to a hash map whose keys are the text versions of the conditions and whose values
/// are the compiled conditions, and then finally return the hash map.
pub fn get_compiled_datatype_conditions(
    config: &ConfigMap,
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
    config: &ConfigMap,
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
                        panic!("Undefined column '{}.{}' in rules table", rules_table, row_column);
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
                    column_rules.push(ColumnRule { when: when_compiled, then: then_compiled });
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
    config: &ConfigMap,
    parser: &StartParser,
) -> HashMap<String, ParsedStructure> {
    let mut parsed_structure_conditions = HashMap::new();
    let tables_config = config.get("table").and_then(|t| t.as_object()).unwrap();
    for (table, table_config) in tables_config.iter() {
        let columns_config = table_config.get("column").and_then(|c| c.as_object()).unwrap();
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

/// Given config maps for tables and datatypes, a database connection pool, and a StartParser,
/// read in the TSV files corresponding to the tables defined in the tables config, and use that
/// information to fill in constraints information into a new config map that is then returned along
/// with a list of the tables in the database sorted according to their mutual dependencies. If
/// the flag `verbose` is set to true, emit SQL to create the database schema to STDOUT.
/// If the flag `write_to_db` is set to true, execute the SQL in the database using the given
/// connection pool.
pub async fn configure_db(
    tables_config: &mut ConfigMap,
    datatypes_config: &mut ConfigMap,
    pool: &AnyPool,
    parser: &StartParser,
    verbose: bool,
    write_to_db: bool,
) -> Result<(Vec<String>, ConfigMap), sqlx::Error> {
    // This is the ConfigMap that we will be returning:
    let mut constraints_config = ConfigMap::new();
    constraints_config.insert(String::from("foreign"), SerdeValue::Object(ConfigMap::new()));
    constraints_config.insert(String::from("unique"), SerdeValue::Object(ConfigMap::new()));
    constraints_config.insert(String::from("primary"), SerdeValue::Object(ConfigMap::new()));
    constraints_config.insert(String::from("tree"), SerdeValue::Object(ConfigMap::new()));
    constraints_config.insert(String::from("under"), SerdeValue::Object(ConfigMap::new()));

    // Begin by reading in the TSV files corresponding to the tables defined in tables_config, and
    // use that information to create the associated database tables, while saving constraint
    // information to constrains_config.
    let mut setup_statements = HashMap::new();
    let table_names: Vec<String> = tables_config.keys().cloned().collect();
    for table_name in table_names {
        let path = tables_config
            .get(&table_name)
            .and_then(|r| r.get("path"))
            .and_then(|p| p.as_str())
            .unwrap();

        // Get the columns that have been previously configured:
        let defined_columns: Vec<String> = tables_config
            .get(&table_name)
            .and_then(|r| r.get("column"))
            .and_then(|v| v.as_object())
            .and_then(|o| Some(o.keys()))
            .and_then(|k| Some(k.cloned()))
            .and_then(|k| Some(k.collect()))
            .unwrap();

        // Get the actual columns from the data itself. Note that we set has_headers to false
        // (even though the files have header rows) in order to explicitly read the header row.
        let mut rdr = csv::ReaderBuilder::new().has_headers(false).delimiter(b'\t').from_reader(
            File::open(path.clone()).unwrap_or_else(|err| {
                panic!("Unable to open '{}': {}", path.clone(), err);
            }),
        );
        let mut iter = rdr.records();
        let actual_columns;
        if let Some(result) = iter.next() {
            actual_columns = result.unwrap();
        } else {
            panic!("'{}' is empty", path);
        }

        // We use column_order to explicitly indicate the order in which the columns should appear
        // in the table, for later reference.
        let mut column_order = vec![];
        let mut all_columns: ConfigMap = ConfigMap::new();
        for column_name in &actual_columns {
            let column;
            if !defined_columns.contains(&column_name.to_string()) {
                let mut cmap = ConfigMap::new();
                cmap.insert(String::from("table"), SerdeValue::String(table_name.to_string()));
                cmap.insert(String::from("column"), SerdeValue::String(column_name.to_string()));
                cmap.insert(String::from("nulltype"), SerdeValue::String(String::from("empty")));
                cmap.insert(String::from("datatype"), SerdeValue::String(String::from("text")));
                column = SerdeValue::Object(cmap);
            } else {
                column = tables_config
                    .get(&table_name)
                    .and_then(|r| r.get("column"))
                    .and_then(|v| v.as_object())
                    .and_then(|o| o.get(column_name))
                    .unwrap()
                    .clone();
            }
            column_order.push(SerdeValue::String(column_name.to_string()));
            all_columns.insert(column_name.to_string(), column);
        }

        tables_config.get_mut(&table_name).and_then(|t| t.as_object_mut()).and_then(|o| {
            o.insert(String::from("column"), SerdeValue::Object(all_columns));
            o.insert(String::from("column_order"), SerdeValue::Array(column_order))
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

        // Create a view as the union of the regular and conflict versions of the table:
        let mut drop_view_sql = format!(r#"DROP VIEW IF EXISTS "{}_view""#, table_name);
        if pool.any_kind() == AnyKind::Postgres {
            drop_view_sql.push_str(" CASCADE");
        }
        drop_view_sql.push_str(";");
        let create_view_sql = format!(
            r#"CREATE VIEW "{t}_view" AS SELECT * FROM "{t}" UNION ALL SELECT * FROM "{t}_conflict";"#,
            t = table_name,
        );
        table_statements.push(drop_view_sql);
        table_statements.push(create_view_sql);

        setup_statements.insert(table_name.to_string(), table_statements);
    }

    // Sort the tables according to their foreign key dependencies so that tables are always loaded
    // after the tables they depend on:
    let unsorted_tables: Vec<String> = setup_statements.keys().cloned().collect();
    let sorted_tables = verify_table_deps_and_sort(&unsorted_tables, &constraints_config);

    if write_to_db || verbose {
        // Generate DDL for the message table:
        let mut message_statements = vec![];
        message_statements.push(r#"DROP TABLE IF EXISTS "message";"#.to_string());
        message_statements.push(
            indoc! {r#"
              CREATE TABLE "message" (
                "table" TEXT,
                "row" BIGINT,
                "column" TEXT,
                "value" TEXT,
                "level" TEXT,
                "rule" TEXT,
                "message" TEXT,
                PRIMARY KEY ("table", "row", "column", "rule")
              );
            "#}
            .to_string(),
        );
        message_statements.push(
            r#"CREATE INDEX "message_trc_idx" ON "message"("table", "row", "column");"#.to_string(),
        );
        setup_statements.insert("message".to_string(), message_statements);

        // Add the message table to the list of tables to create:
        let mut tables_to_create = sorted_tables.clone();
        tables_to_create.push("message".to_string());
        for table in &tables_to_create {
            let table_statements = setup_statements.get(table).unwrap();
            if write_to_db {
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

/// Given a path to a table table file (table.tsv), a directory in which to find/create a database:
/// configure the database using the configuration which can be looked up using the table table,
/// and optionally load it if the `load` flag is set to true. If the `verbose` flag is set to true,
/// output status messages while loading.
pub async fn configure_and_or_load(
    table_table: &str,
    database: &str,
    load: bool,
    verbose: bool,
) -> Result<String, sqlx::Error> {
    let parser = StartParser::new();

    let (specials_config, mut tables_config, mut datatypes_config, rules_config) =
        read_config_files(&table_table.to_string());

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

    let pool = AnyPoolOptions::new().max_connections(5).connect_with(connection_options).await?;
    if load && pool.any_kind() == AnyKind::Sqlite {
        sqlx_query("PRAGMA foreign_keys = ON").execute(&pool).await?;
    }

    let (sorted_table_list, constraints_config) =
        configure_db(&mut tables_config, &mut datatypes_config, &pool, &parser, verbose, load)
            .await?;

    let mut config = ConfigMap::new();
    config.insert(String::from("special"), SerdeValue::Object(specials_config.clone()));
    config.insert(String::from("table"), SerdeValue::Object(tables_config.clone()));
    config.insert(String::from("datatype"), SerdeValue::Object(datatypes_config.clone()));
    config.insert(String::from("rule"), SerdeValue::Object(rules_config.clone()));
    config.insert(String::from("constraints"), SerdeValue::Object(constraints_config.clone()));
    let mut sorted_table_serdevalue_list: Vec<SerdeValue> = vec![];
    for table in &sorted_table_list {
        sorted_table_serdevalue_list.push(SerdeValue::String(table.to_string()));
    }
    config
        .insert(String::from("sorted_table_list"), SerdeValue::Array(sorted_table_serdevalue_list));

    let compiled_datatype_conditions = get_compiled_datatype_conditions(&config, &parser);
    let compiled_rule_conditions =
        get_compiled_rule_conditions(&config, compiled_datatype_conditions.clone(), &parser);

    if load {
        if verbose {
            eprintln!("{} - Processing {} tables.", Utc::now(), sorted_table_list.len());
        }
        load_db(&config, &pool, &compiled_datatype_conditions, &compiled_rule_conditions, verbose)
            .await?;
    }

    let config = SerdeValue::Object(config);
    Ok(config.to_string())
}

/// Given a global config map, a database connection pool, a table name, and a row, assign a new
/// row number to the row and insert it to the database, then return the new row number.
pub async fn insert_new_row(
    global_config: &ConfigMap,
    pool: &AnyPool,
    table_name: &str,
    row: &ConfigMap,
) -> Result<u32, sqlx::Error> {
    // The new row number to insert is the current highest row number + 1.
    let sql = format!(r#"SELECT MAX("row_number") AS "row_number" FROM "{}_view""#, table_name);
    let query = sqlx_query(&sql);
    let result_row = query.fetch_one(pool).await?;
    let result = result_row.try_get_raw("row_number").unwrap();
    let new_row_number: i64;
    if result.is_null() {
        new_row_number = 1;
    } else {
        new_row_number = result_row.get("row_number");
    }
    let new_row_number = new_row_number as u32 + 1;

    let mut insert_columns = vec![];
    let mut insert_values = vec![];
    let mut insert_params = vec![];
    let mut messages = vec![];
    for (column, cell) in row.iter() {
        insert_columns.append(&mut vec![format!(r#""{}""#, column)]);
        let cell = cell.as_object().unwrap();
        let cell_valid = cell.get("valid").and_then(|v| v.as_bool()).unwrap();
        let cell_value = cell.get("value").and_then(|v| v.as_str()).unwrap();
        let mut cell_for_insert = cell.clone();
        if cell_valid {
            cell_for_insert.remove("value");
            let sql_type =
                get_sql_type_from_global_config(&global_config, &table_name.to_string(), &column)
                    .unwrap();
            insert_values.push(cast_sql_param_from_text(&sql_type));
            insert_params.push(String::from(cell_value));
        } else {
            insert_values.push(String::from("NULL"));
            let cell_messages = cell.get("messages").and_then(|m| m.as_array()).unwrap();
            for cell_message in cell_messages {
                messages.push(json!({
                    "column": column,
                    "value": cell_value,
                    "level": cell_message.get("level").and_then(|s| s.as_str()).unwrap(),
                    "rule": cell_message.get("rule").and_then(|s| s.as_str()).unwrap(),
                    "message": cell_message.get("message").and_then(|s| s.as_str()).unwrap(),
                }));
            }
        }
    }

    // First add the new row to the table:
    let insert_stmt = local_sql_syntax(
        &pool,
        &format!(
            r#"INSERT INTO "{}" ("row_number", {}) VALUES ({}, {})"#,
            table_name,
            insert_columns.join(", "),
            new_row_number,
            insert_values.join(", "),
        ),
    );

    let mut query = sqlx_query(&insert_stmt);
    for param in &insert_params {
        query = query.bind(param);
    }
    query.execute(pool).await?;

    // Next add any validation messages to the message table:
    for m in messages {
        let column = m.get("column").and_then(|c| c.as_str()).unwrap();
        let value = m.get("value").and_then(|c| c.as_str()).unwrap();
        let level = m.get("level").and_then(|c| c.as_str()).unwrap();
        let rule = m.get("rule").and_then(|c| c.as_str()).unwrap();
        let message = m.get("message").and_then(|c| c.as_str()).unwrap();
        let message_sql = format!(
            r#"INSERT INTO "message"
               ("table", "row", "column", "value", "level", "rule", "message")
               VALUES ('{}', {}, '{}', '{}', '{}', '{}', '{}')"#,
            table_name, new_row_number, column, value, level, rule, message
        );
        let query = sqlx_query(&message_sql);
        query.execute(pool).await?;
    }

    Ok(new_row_number)
}

/// Given global config map, a database connection pool, a table name, a row, and the row number to
/// update, update the corresponding row in the database with new values as specified by `row`.
pub async fn update_row(
    global_config: &ConfigMap,
    pool: &AnyPool,
    table_name: &str,
    row: &ConfigMap,
    row_number: u32,
) -> Result<(), sqlx::Error> {
    let mut assignments = vec![];
    let mut params = vec![];
    let mut messages = vec![];
    for (column, cell) in row.iter() {
        let cell = cell.as_object().unwrap();
        let cell_valid = cell.get("valid").and_then(|v| v.as_bool()).unwrap();
        let cell_value = cell.get("value").and_then(|v| v.as_str()).unwrap();
        let mut cell_for_insert = cell.clone();
        if cell_valid {
            cell_for_insert.remove("value");
            let sql_type =
                get_sql_type_from_global_config(&global_config, &table_name.to_string(), &column)
                    .unwrap();
            assignments.push(format!(r#""{}" = {}"#, column, cast_sql_param_from_text(&sql_type)));
            params.push(String::from(cell_value));
        } else {
            assignments.push(format!(r#""{}" = NULL"#, column));
            let cell_messages = cell.get("messages").and_then(|m| m.as_array()).unwrap();
            for cell_message in cell_messages {
                messages.push(json!({
                    "column": String::from(column),
                    "value": String::from(cell_value),
                    "level": cell_message.get("level").and_then(|s| s.as_str()).unwrap(),
                    "rule": cell_message.get("rule").and_then(|s| s.as_str()).unwrap(),
                    "message": cell_message.get("message").and_then(|s| s.as_str()).unwrap(),
                }));
            }
        }
    }

    // First update the given row in the table:
    let mut update_stmt = format!(r#"UPDATE "{}" SET "#, table_name);
    update_stmt.push_str(&assignments.join(", "));
    update_stmt.push_str(&format!(r#" WHERE "row_number" = {}"#, row_number));
    let update_stmt = local_sql_syntax(&pool, &update_stmt);

    let mut query = sqlx_query(&update_stmt);
    for param in &params {
        query = query.bind(param);
    }
    query.execute(pool).await?;

    // Next delete any messages that had been previously inserted to the message table for the old
    // version of this row:
    let delete_sql = format!(
        r#"DELETE FROM "message" WHERE "table" = '{}' AND "row" = {}"#,
        table_name, row_number
    );
    let query = sqlx_query(&delete_sql);
    query.execute(pool).await?;

    // Finally add the messages to the message table for the new version of this row:
    for m in messages {
        let column = m.get("column").and_then(|c| c.as_str()).unwrap();
        let value = m.get("value").and_then(|c| c.as_str()).unwrap();
        let level = m.get("level").and_then(|c| c.as_str()).unwrap();
        let rule = m.get("rule").and_then(|c| c.as_str()).unwrap();
        let message = m.get("message").and_then(|c| c.as_str()).unwrap();
        let insert_sql = format!(
            r#"INSERT INTO "message"
               ("table", "row", "column", "value", "level", "rule", "message")
               VALUES ('{}', {}, '{}', '{}', '{}', '{}', '{}')"#,
            table_name, row_number, column, value, level, rule, message
        );
        let query = sqlx_query(&insert_sql);
        query.execute(pool).await?;
    }

    Ok(())
}

/// Given a path, read a TSV file and return a vector of rows represented as ConfigMaps.
/// Note: Use this function to read "small" TSVs only. In particular, use this for the special
/// configuration tables.
fn read_tsv_into_vector(path: &str) -> Vec<ConfigMap> {
    let mut rdr = csv::ReaderBuilder::new().delimiter(b'\t').from_reader(
        File::open(path).unwrap_or_else(|err| {
            panic!("Unable to open '{}': {}", path, err);
        }),
    );

    let rows: Vec<_> = rdr
        .deserialize()
        .map(|result| {
            let row: ConfigMap = result.expect(format!("Error reading: {}", path).as_str());
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
                    let compiled_datatype_condition =
                        compiled_datatype_conditions.get(&value.to_string()).unwrap();
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

/// Given the config map and the name of a datatype, climb the datatype tree (as required),
/// and return the first 'SQL type' found.
fn get_sql_type(dt_config: &ConfigMap, datatype: &String) -> Option<String> {
    if !dt_config.contains_key(datatype) {
        return None;
    }

    if let Some(sql_type) = dt_config.get(datatype).and_then(|d| d.get("SQL type")) {
        return Some(sql_type.as_str().and_then(|s| Some(s.to_string())).unwrap());
    }

    let parent_datatype =
        dt_config.get(datatype).and_then(|d| d.get("parent")).and_then(|p| p.as_str()).unwrap();

    return get_sql_type(dt_config, &parent_datatype.to_string());
}

/// Given the global config map, a table name, and a column name, return the column's SQL type.
fn get_sql_type_from_global_config(
    global_config: &ConfigMap,
    table: &str,
    column: &str,
) -> Option<String> {
    let dt_config = global_config.get("datatype").and_then(|d| d.as_object()).unwrap();
    let normal_table_name;
    if let Some(s) = table.strip_suffix("_conflict") {
        normal_table_name = String::from(s);
    } else {
        normal_table_name = table.to_string();
    }
    let dt = global_config
        .get("table")
        .and_then(|t| t.get(normal_table_name))
        .and_then(|t| t.get("column"))
        .and_then(|c| c.get(column))
        .and_then(|c| c.get("datatype"))
        .and_then(|d| d.as_str())
        .and_then(|d| Some(d.to_string()))
        .unwrap();
    get_sql_type(&dt_config, &dt)
}

/// Given a SQL type, return the appropriate CAST(...) statement for casting the SQL_PARAM
/// from a TEXT column.
fn cast_sql_param_from_text(sql_type: &str) -> String {
    if sql_type.to_lowercase() == "integer" {
        format!("CAST(NULLIF({}, '') AS INTEGER)", SQL_PARAM)
    } else {
        String::from(SQL_PARAM)
    }
}

/// Given a SQL type, return the appropriate CAST(...) statement for casting the SQL_PARAM
/// to a TEXT column.
fn cast_column_sql_to_text(column: &str, sql_type: &str) -> String {
    if sql_type.to_lowercase() == "integer" {
        format!(r#"CAST("{}" AS TEXT)"#, column)
    } else {
        format!(r#""{}""#, column)
    }
}

/// Given a database row, the name of a column, and it's SQL type, return the value of that column
// from the given row as a String.
fn get_column_value(row: &AnyRow, column: &str, sql_type: &str) -> String {
    if sql_type.to_lowercase() == "integer" {
        let value: i32 = row.get(format!(r#"{}"#, column).as_str());
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
fn verify_table_deps_and_sort(table_list: &Vec<String>, constraints: &ConfigMap) -> Vec<String> {
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
                        let cycle = cycle.iter().map(|&item| item.to_string()).collect::<Vec<_>>();
                        cycles.push(cycle);
                    }
                }
                Err(cycles)
            }
            Ok(sorted) => {
                let mut sorted = sorted.iter().map(|&item| item.to_string()).collect::<Vec<_>>();
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

    let foreign_keys = constraints.get("foreign").and_then(|f| f.as_object()).unwrap();
    let under_keys = constraints.get("under").and_then(|u| u.as_object()).unwrap();
    let mut dependency_graph = DiGraphMap::<&str, ()>::new();
    for table_name in table_list {
        let t_index = dependency_graph.add_node(table_name);
        let fkeys = foreign_keys.get(table_name).and_then(|f| f.as_array()).unwrap();
        for fkey in fkeys {
            let ftable = fkey.get("ftable").and_then(|f| f.as_str()).unwrap();
            let f_index = dependency_graph.add_node(ftable);
            dependency_graph.add_edge(t_index, f_index, ());
        }

        let ukeys = under_keys.get(table_name).and_then(|u| u.as_array()).unwrap();
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
                                table,
                                column,
                                ref_table,
                                ref_column,
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
    tables_config: &mut ConfigMap,
    datatypes_config: &mut ConfigMap,
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

    let mut colvals: Vec<ConfigMap> = vec![];
    for column_name in &column_names {
        let column = columns.get(column_name).and_then(|c| c.as_object()).unwrap();
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
        );

        if let None = sql_type {
            panic!("Missing SQL type for {}", row.get("datatype").unwrap());
        }
        let sql_type = sql_type.unwrap();
        if !SQL_TYPES.contains(&sql_type.to_lowercase().as_str()) {
            panic!("Unrecognized SQL type '{}' for {}", sql_type, row.get("datatype").unwrap());
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
                                    let child_sql_type =
                                        get_sql_type(datatypes_config, &child_datatype.to_string())
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

    let foreign_keys = table_constraints.get("foreign").and_then(|v| v.as_array()).unwrap();
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
    let tree_constraints = table_constraints.get("tree").and_then(|v| v.as_array()).unwrap();
    for tree in tree_constraints {
        let unique_keys = table_constraints.get("unique").and_then(|v| v.as_array()).unwrap();
        let primary_keys = table_constraints.get("primary").and_then(|v| v.as_array()).unwrap();
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

/// Given a configuration map, a table name, a number of rows, and their corresponding chunk number,
/// return two four-place tuples, corresponding to the normal and conflict tables, respectively.
/// Each of these contains (i) a SQL string for an insert statement to the table, (ii) parameters
/// to bind to that SQL statement, (iii) a SQL string for an insert statement the message table, and
/// (iv) parameters to bind to that SQL statement. If the verbose flag is set, the number of errors,
/// warnings, and information messages generated are added to messages_stats, the contents of which
/// will later be written to stderr.
async fn make_inserts(
    config: &ConfigMap,
    table_name: &String,
    rows: &mut Vec<ResultRow>,
    chunk_number: usize,
    messages_stats: &mut HashMap<String, usize>,
    verbose: bool,
) -> Result<
    ((String, Vec<String>, String, Vec<String>), (String, Vec<String>, String, Vec<String>)),
    sqlx::Error,
> {
    let conflict_columns = {
        let mut conflict_columns = vec![];
        let primaries = config
            .get("constraints")
            .and_then(|c| c.as_object())
            .and_then(|c| c.get("primary"))
            .and_then(|t| t.as_object())
            .and_then(|t| t.get(table_name))
            .and_then(|t| t.as_array())
            .unwrap();

        let uniques = config
            .get("constraints")
            .and_then(|c| c.as_object())
            .and_then(|c| c.get("unique"))
            .and_then(|t| t.as_object())
            .and_then(|t| t.get(table_name))
            .and_then(|t| t.as_array())
            .unwrap();

        let trees = config
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

        for key_columns in vec![primaries, uniques, &trees] {
            for column in key_columns {
                if !conflict_columns.contains(column) {
                    conflict_columns.push(column.clone());
                }
            }
        }

        conflict_columns
    };

    fn generate_sql(
        config: &ConfigMap,
        table_name: &String,
        column_names: &Vec<String>,
        rows: &Vec<ResultRow>,
        messages_stats: &mut HashMap<String, usize>,
        verbose: bool,
    ) -> (String, Vec<String>, String, Vec<String>) {
        let mut lines = vec![];
        let mut params = vec![];
        let mut message_lines = vec![];
        let mut message_params = vec![];
        for row in rows.iter() {
            let mut values = vec![format!("{}", row.row_number.unwrap())];
            for column in column_names {
                let cell = row.contents.get(column).unwrap();

                // Insert the value of the cell into the column unless it is invalid or has the
                // nulltype field set, in which case insert NULL:
                if cell.nulltype == None && cell.valid {
                    let sql_type =
                        get_sql_type_from_global_config(&config, &table_name, &column).unwrap();
                    values.push(cast_sql_param_from_text(&sql_type));
                    params.push(cell.value.clone());
                } else {
                    values.push(String::from("NULL"));
                }

                // If the cell isn't valid, generate values and params to be used for the insert to
                // the message table:
                if !cell.valid {
                    if verbose {
                        add_message_counts(&cell.messages, messages_stats);
                    }
                    for message in &cell.messages {
                        let row = row.row_number.unwrap().to_string();
                        let message_values = vec![
                            SQL_PARAM, &row, SQL_PARAM, SQL_PARAM, SQL_PARAM, SQL_PARAM, SQL_PARAM,
                        ];

                        let message = message.as_object().unwrap();
                        message_params.push({
                            let normal_table_name;
                            if let Some(s) = table_name.strip_suffix("_conflict") {
                                normal_table_name = String::from(s);
                            } else {
                                normal_table_name = table_name.to_string();
                            }
                            normal_table_name
                        });
                        message_params.push(column.clone());
                        message_params.push(cell.value.clone());
                        message_params.push(
                            message.get("level").and_then(|s| s.as_str()).unwrap().to_string(),
                        );
                        message_params.push(
                            message.get("rule").and_then(|s| s.as_str()).unwrap().to_string(),
                        );
                        message_params.push(
                            message.get("message").and_then(|s| s.as_str()).unwrap().to_string(),
                        );
                        let line = message_values.join(", ");
                        let line = format!("({})", line);
                        message_lines.push(line);
                    }
                }
            }
            let line = values.join(", ");
            let line = format!("({})", line);
            lines.push(line);
        }

        // Generate the SQL output for the insert to the table:
        let mut output = String::from("");
        if !lines.is_empty() {
            output.push_str(&format!(
                r#"INSERT INTO "{}" ("row_number", {}) VALUES"#,
                table_name,
                {
                    let mut all_columns = vec![];
                    for column_name in column_names {
                        let quoted_column_name = format!(r#""{}""#, column_name);
                        all_columns.push(quoted_column_name);
                    }
                    all_columns.join(", ")
                }
            ));
            output.push_str("\n");
            output.push_str(&lines.join(",\n"));
            output.push_str(";");
        }

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

        (output, params, message_output, message_params)
    }

    fn has_conflict(row: &ResultRow, conflict_columns: &Vec<SerdeValue>) -> bool {
        for (column, cell) in &row.contents {
            let column = SerdeValue::String(column.to_string());
            if conflict_columns.contains(&column) && !cell.valid {
                return true;
            }
        }
        return false;
    }

    let mut main_rows = vec![];
    let mut conflict_rows = vec![];
    for (i, row) in rows.iter_mut().enumerate() {
        // enumerate begins at 0 but we need to begin at 1:
        let i = i + 1;
        row.row_number = Some(i as u32 + chunk_number as u32 * CHUNK_SIZE as u32);
        if has_conflict(&row, &conflict_columns) {
            conflict_rows.push(row.clone());
        } else {
            main_rows.push(row.clone());
        }
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

    let (main_sql, main_params, main_message_sql, main_message_params) =
        generate_sql(&config, &table_name, &column_names, &main_rows, messages_stats, verbose);
    let (conflict_sql, conflict_params, conflict_message_sql, conflict_message_params) =
        generate_sql(
            &config,
            &format!("{}_conflict", table_name),
            &column_names,
            &conflict_rows,
            messages_stats,
            verbose,
        );

    Ok((
        (main_sql, main_params, main_message_sql, main_message_params),
        (conflict_sql, conflict_params, conflict_message_sql, conflict_message_params),
    ))
}

/// Given a configuration map, a database connection pool, a table name, some rows to validate,
/// and the chunk number corresponding to the rows, do inter-row validation on the rows and insert
/// them to the table. If the verbose flag is set to true, error/warning/info stats will be
/// collected in messages_stats and later written to stderr.
async fn validate_rows_inter_and_insert(
    config: &ConfigMap,
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
    let (
        (main_sql, main_params, main_message_sql, main_message_params),
        (conflict_sql, conflict_params, conflict_message_sql, conflict_message_params),
    ) = make_inserts(config, table_name, rows, chunk_number, &mut tmp_messages_stats, verbose)
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

            let main_message_sql = local_sql_syntax(&pool, &main_message_sql);
            let mut main_message_query = sqlx_query(&main_message_sql);
            for param in &main_message_params {
                main_message_query = main_message_query.bind(param);
            }
            main_message_query.execute(pool).await?;

            let conflict_message_sql = local_sql_syntax(&pool, &conflict_message_sql);
            let mut conflict_message_query = sqlx_query(&conflict_message_sql);
            for param in &conflict_message_params {
                conflict_message_query = conflict_message_query.bind(param);
            }
            conflict_message_query.execute(pool).await?;

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

            let (
                (main_sql, main_params, main_message_sql, main_message_params),
                (conflict_sql, conflict_params, conflict_message_sql, conflict_message_params),
            ) = make_inserts(config, table_name, rows, chunk_number, messages_stats, verbose)
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

            let main_message_sql = local_sql_syntax(&pool, &main_message_sql);
            let mut main_message_query = sqlx_query(&main_message_sql);
            for param in &main_message_params {
                main_message_query = main_message_query.bind(param);
            }
            main_message_query.execute(pool).await?;

            let conflict_message_sql = local_sql_syntax(&pool, &conflict_message_sql);
            let mut conflict_message_query = sqlx_query(&conflict_message_sql);
            for param in &conflict_message_params {
                conflict_message_query = conflict_message_query.bind(param);
            }
            conflict_message_query.execute(pool).await?;
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
    config: &ConfigMap,
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
    config: &ConfigMap,
    pool: &AnyPool,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    verbose: bool,
) -> Result<(), sqlx::Error> {
    let mut table_list = vec![];
    for table in config.get("sorted_table_list").and_then(|l| l.as_array()).unwrap() {
        table_list.push(table.as_str().and_then(|s| Some(s.to_string())).unwrap());
    }
    let table_list = table_list; // Change the table_list to read only after populating it.
    let num_tables = table_list.len();
    let mut total_errors = 0;
    let mut total_warnings = 0;
    let mut total_infos = 0;
    let mut table_num = 1;
    for table_name in table_list {
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
        let path = String::from(
            config
                .get("table")
                .and_then(|t| t.as_object())
                .and_then(|o| o.get(&table_name))
                .and_then(|n| n.get("path"))
                .and_then(|p| p.as_str())
                .unwrap(),
        );
        let mut rdr = csv::ReaderBuilder::new().has_headers(false).delimiter(b'\t').from_reader(
            File::open(path.clone()).unwrap_or_else(|err| {
                panic!("Unable to open '{}': {}", path.clone(), err);
            }),
        );

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
                panic!("One or more of the header fields is empty for table '{}'", table_name);
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
            validate_tree_foreign_keys(config, pool, &table_name, None).await?;
        recs_to_update.append(&mut validate_under(config, pool, &table_name, None).await?);

        for record in recs_to_update {
            let row_number = record.get("row_number").unwrap();
            let column_name = record.get("column").and_then(|s| s.as_str()).unwrap();
            let value = record.get("value").and_then(|s| s.as_str()).unwrap();
            let level = record.get("level").and_then(|s| s.as_str()).unwrap();
            let rule = record.get("rule").and_then(|s| s.as_str()).unwrap();
            let message = record.get("message").and_then(|s| s.as_str()).unwrap();

            let sql = format!(
                r#"UPDATE "{}" SET "{}" = NULL WHERE "row_number" = {}"#,
                table_name, column_name, row_number
            );
            let query = sqlx_query(&sql);
            query.execute(pool).await?;

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
