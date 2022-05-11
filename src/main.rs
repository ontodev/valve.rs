#[macro_use]
extern crate lalrpop_util;

mod ast;
mod validate;

lalrpop_mod!(pub cmi_pb_grammar);

use crate::ast::Expression;
use crate::cmi_pb_grammar::StartParser;
pub use crate::validate::{validate_rows_intra, validate_rows_trees, ResultCell, ResultRow};

// provides `try_next` for sqlx:
use futures::TryStreamExt;

use crossbeam;
use crossbeam::thread::ScopedJoinHandle;
use itertools::{IntoChunks, Itertools};
use lazy_static::lazy_static;
use petgraph::algo::{all_simple_paths, toposort};
use petgraph::graphmap::DiGraphMap;
use petgraph::Direction;
use regex::Regex;
use serde_json::{
    json,
    // SerdeMap by default backed by BTreeMap (see https://docs.serde.rs/serde_json/map/index.html)
    Map as SerdeMap,
    Value as SerdeValue,
};
use sqlx::query;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::process;
use std::str::FromStr;
use std::sync::Arc;

pub type ConfigMap = SerdeMap<String, SerdeValue>;

lazy_static! {
    static ref SQLITE_TYPES: Vec<&'static str> = vec!["text", "integer", "real", "blob"];
}

static CHUNK_SIZE: usize = 2500;
pub static MULTI_THREADED: bool = false;

/*
compiled_rule_conditions = {
    "my_table": {
        "when_column_1": [
            ColumnRule {
                when: CompiledCondition {parsed, compiled},
                then: CompiledCondition {parsed, compiled},
            },
            ColumnRule {
                when: CompiledCondition {parsed, compiled},
                then: CompiledCondition {parsed, compiled},
            },
            ...
        ],
    },
    ...
}
*/

pub struct ParsedStructure {
    // TODO: Remove the underscores from these variable names. Since the code currently never
    // accesses these fields we will get a compiler warning unless they are underscored.
    // Presumably, though, when the implementation is complete they will be accessed at some point
    // and we can remove the underscores here.
    _original: String,
    _parsed: Expression,
}

pub struct CompiledCondition {
    original: String,
    parsed: Expression,
    compiled: Arc<dyn Fn(&str) -> bool + Sync + Send>,
}

impl std::fmt::Debug for CompiledCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("CompiledCondition")
            .field("original", &self.original)
            .field("parsed", &self.parsed)
            .finish()
    }
}

pub struct ColumnRule {
    when: CompiledCondition,
    then: CompiledCondition,
}

impl std::fmt::Debug for ColumnRule {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("ColumnRule").field("when", &self.when).field("then", &self.then).finish()
    }
}

/// Use this function for "small" TSVs only, since it returns a vector.
fn read_tsv_into_vector(path: &String) -> Vec<ConfigMap> {
    let mut rdr = csv::ReaderBuilder::new().delimiter(b'\t').from_reader(
        File::open(path).unwrap_or_else(|err| {
            panic!("Unable to open '{}': {}", path, err);
        }),
    );

    let rows: Vec<_> = rdr
        .deserialize()
        .map(|result| {
            let row: ConfigMap = result.unwrap();
            row
        })
        .collect();

    if rows.len() < 1 {
        panic!("No rows in {}", path);
    }

    rows
}

fn compile_condition(
    condition_option: Option<&str>,
    parser: &StartParser,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
) -> CompiledCondition {
    let unquoted_re = Regex::new(r#"^['"](?P<unquoted>.*)['"]$"#).unwrap();
    match condition_option {
        // The case of no condition, or a "null" or "not null" condition, will be treated specially
        // later during the validation phase in a way that does not utilise the associated closure.
        // Since we have to assign some closure in these cases, we use a constant closure that
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
                                panic!(
                                    "Programming error: argument: {:?} to function 'in' \
                                     is not a label",
                                    arg
                                );
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
                        // TODO: remove this commented out code later:
                        //original: compiled_datatype_condition.original.to_string(),
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

fn read_config_files(
    table_table_path: &String,
    parser: &StartParser,
) -> (
    ConfigMap,
    ConfigMap,
    ConfigMap,
    ConfigMap,
    HashMap<String, CompiledCondition>,
    HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    HashMap<String, ParsedStructure>,
) {
    let special_table_types = json!({
        "table": {"required": true},
        "column": {"required": true},
        "datatype": {"required": true},
        "rule": {"required": false},
    });
    let special_table_types = special_table_types.as_object().unwrap();

    // Initialize the special table entries in the config map:
    let mut specials_config: ConfigMap = SerdeMap::new();
    for t in special_table_types.keys() {
        specials_config.insert(t.to_string(), SerdeValue::Null);
    }

    let path = table_table_path;
    let rows = read_tsv_into_vector(path);

    // Load table table
    let mut tables_config: ConfigMap = SerdeMap::new();
    for mut row in rows {
        for column in vec!["table", "path", "type"] {
            if !row.contains_key(column) || row.get(column) == None {
                panic!("Missing required column '{}' reading '{}'", column, path);
            }
        }

        for column in vec!["table", "path"] {
            if row.get(column).and_then(|c| c.as_str()).and_then(|c| Some(c.trim())).unwrap() == ""
            {
                panic!("Missing required value for '{}' reading '{}'", column, path);
            }
        }

        for column in vec!["type"] {
            if row.get(column).and_then(|c| c.as_str()).and_then(|c| Some(c.trim())).unwrap() == ""
            {
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

    // Load datatype table
    let mut datatypes_config: ConfigMap = SerdeMap::new();
    let mut compiled_datatype_conditions: HashMap<String, CompiledCondition> = HashMap::new();
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
            if row.get(column).and_then(|c| c.as_str()).and_then(|c| Some(c.trim())).unwrap() == ""
            {
                panic!("Missing required value for '{}' reading '{}'", column, path);
            }
        }

        for column in vec!["parent", "condition", "SQL type"] {
            if row.get(column).and_then(|c| c.as_str()).and_then(|c| Some(c.trim())).unwrap() == ""
            {
                row.remove(&column.to_string());
            }
        }

        let dt_name = row.get("datatype").and_then(|d| d.as_str()).unwrap();
        let condition = row.get("condition").and_then(|c| c.as_str());
        let compiled_condition =
            compile_condition(condition, parser, &compiled_datatype_conditions);
        if let Some(_) = condition {
            compiled_datatype_conditions.insert(dt_name.to_string(), compiled_condition);
        }
        datatypes_config.insert(dt_name.to_string(), SerdeValue::Object(row));
    }

    for dt in vec!["text", "empty", "line", "word"] {
        if !datatypes_config.contains_key(dt) {
            panic!("Missing required datatype: '{}'", dt);
        }
    }

    // Load column table
    let mut parsed_structure_conditions: HashMap<String, ParsedStructure> = HashMap::new();
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
            if row.get(column).and_then(|c| c.as_str()).and_then(|c| Some(c.trim())).unwrap() == ""
            {
                panic!("Missing required value for '{}' reading '{}'", column, path);
            }
        }

        for column in vec!["nulltype"] {
            if row.get(column).and_then(|c| c.as_str()).and_then(|c| Some(c.trim())).unwrap() == ""
            {
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

        let structure = row.get("structure").and_then(|s| s.as_str()).unwrap();
        if structure != "" {
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
                _original: structure.to_string(),
                _parsed: *parsed_structure.clone(),
            };
            parsed_structure_conditions.insert(structure.to_string(), parsed_structure);
        }

        let row_table = row.get("table").and_then(|t| t.as_str()).unwrap();
        let column_name = row.get("column").and_then(|c| c.as_str()).unwrap();
        if let Some(SerdeValue::Object(columns_config)) =
            tables_config.get_mut(row_table).and_then(|t| t.get_mut("column"))
        {
            columns_config.insert(column_name.to_string(), SerdeValue::Object(row));
        } else {
            // TODO: Try and remove this panic! statement by using unwrap() above (or something
            // like that).
            panic!(
                "Programming error: Unable to find column config for column {}.{}",
                row_table, column_name
            );
        }
    }

    // Load rule table if it exists
    let mut rules_config: ConfigMap = SerdeMap::new();
    let mut compiled_rule_conditions: HashMap<String, HashMap<String, Vec<ColumnRule>>> =
        HashMap::new();
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
                if row.get(column).and_then(|c| c.as_str()).and_then(|c| Some(c.trim())).unwrap()
                    == ""
                {
                    panic!("Missing required value for '{}' reading '{}'", column, path);
                }
            }

            let row_table = row.get("table").and_then(|t| t.as_str()).unwrap();
            if !tables_config.contains_key(row_table) {
                panic!("Undefined table '{}' reading '{}'", row_table, path);
            }

            // Compile and collect the when and then conditions.
            let mut column_rule_key = None;
            for column in vec!["when column", "then column"] {
                let row_column = row.get(column).and_then(|c| c.as_str()).unwrap();
                if column == "when column" {
                    column_rule_key = Some(row_column.to_string());
                }
                if !tables_config
                    .get(row_table)
                    .and_then(|t| t.get("column"))
                    .and_then(|c| c.as_object())
                    .and_then(|c| Some(c.contains_key(row_column)))
                    .unwrap()
                {
                    panic!("Undefined column '{}.{}' reading '{}'", row_table, row_column, path);
                }
            }
            let column_rule_key = column_rule_key.unwrap();

            let mut when_compiled = None;
            let mut then_compiled = None;
            for column in vec!["when condition", "then condition"] {
                let condition_option = row.get(column).and_then(|c| c.as_str());
                if let Some(_) = condition_option {
                    let compiled_condition =
                        compile_condition(condition_option, parser, &compiled_datatype_conditions);
                    if column == "when condition" {
                        when_compiled = Some(compiled_condition);
                    } else if column == "then condition" {
                        then_compiled = Some(compiled_condition);
                    }
                }
            }

            if let (Some(when_compiled), Some(then_compiled)) = (when_compiled, then_compiled) {
                if !compiled_rule_conditions.contains_key(row_table) {
                    let table_rules: HashMap<String, Vec<ColumnRule>> = HashMap::new();
                    compiled_rule_conditions.insert(row_table.to_string(), table_rules);
                }
                let table_rules = compiled_rule_conditions.get_mut(row_table).unwrap();
                if !table_rules.contains_key(&column_rule_key) {
                    table_rules.insert(column_rule_key.to_string(), vec![]);
                }
                let column_rules = table_rules.get_mut(&column_rule_key).unwrap();
                column_rules.push(ColumnRule { when: when_compiled, then: then_compiled });
            }

            // Add the rule specified in the given row to the list of rules associated with the
            // value of the when column:
            let row_when_column = row.get("when column").and_then(|c| c.as_str()).unwrap();
            if !rules_config.contains_key(row_table) {
                rules_config.insert(String::from(row_table), SerdeValue::Object(SerdeMap::new()));
            }

            if let Some(SerdeValue::Object(table_rule_config)) = rules_config.get_mut(row_table) {
                if !table_rule_config.contains_key(row_when_column) {
                    table_rule_config
                        .insert(String::from(row_when_column), SerdeValue::Array(vec![]));
                }
                if let Some(SerdeValue::Array(column_rule_config)) =
                    table_rule_config.get_mut(&row_when_column.to_string())
                {
                    column_rule_config.push(SerdeValue::Object(row));
                } else {
                    panic!(
                        "Programming error: No '{}' key in rule config for table '{}'",
                        row_when_column, row_table
                    );
                }
            } else {
                panic!("Programming error: No 'when column' key in rule config.")
            }
        }
    }

    (
        specials_config,
        tables_config,
        datatypes_config,
        rules_config,
        compiled_datatype_conditions,
        compiled_rule_conditions,
        parsed_structure_conditions,
    )
}

async fn configure_db(
    tables_config: &mut ConfigMap,
    datatypes_config: &mut ConfigMap,
    pool: &SqlitePool,
    parser: &StartParser,
    write_sql_to_stdout: Option<bool>,
    write_to_db: Option<bool>,
) -> Result<ConfigMap, sqlx::Error> {
    // If the optional arguments have not been supplied, give them default values:
    let write_sql_to_stdout = write_sql_to_stdout.unwrap_or(false);
    let write_to_db = write_to_db.unwrap_or(false);

    // This is what we will return:
    let mut constraints_config: ConfigMap = SerdeMap::new();
    constraints_config.insert(String::from("foreign"), SerdeValue::Object(SerdeMap::new()));
    constraints_config.insert(String::from("unique"), SerdeValue::Object(SerdeMap::new()));
    constraints_config.insert(String::from("primary"), SerdeValue::Object(SerdeMap::new()));
    constraints_config.insert(String::from("tree"), SerdeValue::Object(SerdeMap::new()));
    constraints_config.insert(String::from("under"), SerdeValue::Object(SerdeMap::new()));

    let table_names: Vec<String> = tables_config.keys().cloned().collect();
    for table_name in table_names {
        let mut path = tables_config
            .get(&table_name)
            .and_then(|r| r.get("path"))
            .and_then(|p| Some(p.to_string()))
            .unwrap();
        // Remove enclosing quotes that are added as a side-effect of the to_string() conversion
        // TODO: See if we can avoid having to do this here and elsewhere.
        path = path.split_off(1);
        path.truncate(path.len() - 1);
        // Note that we set has_headers to false (even though the files have header rows) in order
        // to explicitly read the headers
        let mut rdr = csv::ReaderBuilder::new().has_headers(false).delimiter(b'\t').from_reader(
            File::open(path.clone()).unwrap_or_else(|err| {
                panic!("Unable to open '{}': {}", path.clone(), err);
            }),
        );

        // Get the columns that have been previously configured:
        let defined_columns: Vec<String> = tables_config
            .get(&table_name)
            .and_then(|r| r.get("column"))
            .and_then(|v| v.as_object())
            .and_then(|o| Some(o.keys()))
            .and_then(|k| Some(k.cloned()))
            .and_then(|k| Some(k.collect()))
            .unwrap();

        // Get the actual columns from the data itself:
        let mut iter = rdr.records();
        let actual_columns;
        if let Some(result) = iter.next() {
            actual_columns = result.unwrap();
        } else {
            panic!("'{}' is empty", path);
        }
        // Get the first row of data (just to verify that there is data in the file):
        if let None = iter.next() {
            panic!("No rows in '{}'", path);
        }

        let mut all_columns: ConfigMap = ConfigMap::new();
        for column_name in &actual_columns {
            let column;
            if !defined_columns.contains(&column_name.to_string()) {
                let mut cmap: ConfigMap = SerdeMap::new();
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
            all_columns.insert(column_name.to_string(), column);
        }

        tables_config
            .get_mut(&table_name)
            .and_then(|t| t.as_object_mut())
            .and_then(|o| o.insert(String::from("column"), SerdeValue::Object(all_columns)));

        for table in vec![table_name.to_string(), format!("{}_conflict", table_name)] {
            let (table_sql, table_constraints) =
                create_table(tables_config, datatypes_config, parser, &table);
            if !table.ends_with("_conflict") {
                for constraint_type in vec!["foreign", "unique", "primary", "tree", "under"] {
                    let table_constraints = table_constraints.get(constraint_type).unwrap().clone();
                    constraints_config
                        .get_mut(constraint_type)
                        .and_then(|o| o.as_object_mut())
                        .and_then(|o| o.insert(table_name.to_string(), table_constraints));
                }
            }
            if write_to_db {
                query(&table_sql)
                    .execute(pool)
                    .await
                    .expect(format!("The SQL statement: {} returned an error", table_sql).as_str());
            }
            if write_sql_to_stdout {
                println!("{}\n", table_sql);
            }
        }

        let drop_view_sql = format!(r#"DROP VIEW IF EXISTS "{}_view";"#, table_name);
        let create_view_sql = format!(
            r#"CREATE VIEW "{t}_view" AS SELECT * FROM "{t}" UNION SELECT * FROM "{t}_conflict";"#,
            t = table_name,
        );
        let sql = format!("{}\n{}\n", drop_view_sql, create_view_sql);

        if write_sql_to_stdout {
            println!("{}", sql);
        }
        if write_to_db {
            query(&sql)
                .execute(pool)
                .await
                .expect(format!("The SQL statement: {} returned an error", sql).as_str());
        }
    }

    return Ok(constraints_config);
}

async fn load_db(
    config: &ConfigMap,
    pool: &SqlitePool,
    parser: &StartParser,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    parsed_structure_conditions: &HashMap<String, ParsedStructure>,
) -> Result<(), sqlx::Error> {
    let table_list: Vec<String> = config
        .get("table")
        .and_then(|t| t.as_object())
        .and_then(|o| Some(o.keys()))
        .and_then(|k| Some(k.cloned()))
        .and_then(|k| Some(k.collect()))
        .and_then(|v| {
            Some(verify_table_deps_and_sort(
                &v,
                config.get("constraints").and_then(|t| t.as_object()).unwrap(),
            ))
        })
        .unwrap();

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

        let chunks = records.chunks(CHUNK_SIZE);
        validate_and_insert_chunks(
            config,
            pool,
            parser,
            compiled_datatype_conditions,
            compiled_rule_conditions,
            parsed_structure_conditions,
            &table_name,
            &chunks,
            &headers,
        )
        .await?;

        // TODO: Call validate_tree_foreign_keys() and then use the returned records to update
        // the database.
    }

    Ok(())
}

async fn validate_and_insert_chunks(
    config: &ConfigMap,
    pool: &SqlitePool,
    parser: &StartParser,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    parsed_structure_conditions: &HashMap<String, ParsedStructure>,
    table_name: &String,
    chunks: &IntoChunks<csv::StringRecordsIter<'_, std::fs::File>>,
    headers: &csv::StringRecord,
) -> Result<(), sqlx::Error> {
    if !MULTI_THREADED {
        for (chunk_number, chunk) in chunks.into_iter().enumerate() {
            // enumerate() begins at 0 by default but we need to begin with 1:
            let chunk_number = chunk_number + 1;
            let mut rows: Vec<_> = chunk.collect();
            let mut intra_validated_rows = validate_rows_intra(
                config,
                pool,
                parser,
                compiled_datatype_conditions,
                compiled_rule_conditions,
                parsed_structure_conditions,
                table_name,
                headers,
                &mut rows,
            );
            validate_rows_inter_and_insert(
                config,
                pool,
                parser,
                compiled_datatype_conditions,
                compiled_rule_conditions,
                parsed_structure_conditions,
                table_name,
                headers,
                &mut intra_validated_rows,
                chunk_number,
            )
            .await?;
        }
        Ok(())
    } else {
        panic!("Multiprocessing temporariy disabled because of async");
        /*
        crossbeam::scope(|scope| {
            fn wait_for_and_process_results(
                config: &ConfigMap,
                pool: &SqlitePool,
                parser: &StartParser,
                compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
                compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
                parsed_structure_conditions: &HashMap<String, ParsedStructure>,
                table_name: &String,
                headers: &csv::StringRecord,
                mut chunk_number: usize,
                handles: &mut Vec<ScopedJoinHandle<Vec<ResultRow>>>,
            ) {
                while let Some(handle) = handles.pop() {
                    let mut intra_validated_rows = handle.join().unwrap();
                    validate_rows_inter_and_insert(
                        config,
                        pool,
                        parser,
                        compiled_datatype_conditions,
                        compiled_rule_conditions,
                        parsed_structure_conditions,
                        table_name,
                        headers,
                        &mut intra_validated_rows,
                        chunk_number
                    );
                    chunk_number += 1;
                }
            }

            let num_cpus = num_cpus::get();
            let mut handles = vec![];
            let mut last_chunk_number = 0;
            for (chunk_number, chunk) in chunks.into_iter().enumerate() {
                // enumerate() begins at 0 by default but we need to begin with 1:
                let chunk_number = chunk_number + 1;
                let mut rows: Vec<_> = chunk.collect();
                handles.push(scope.spawn(move |_| {
                    validate_rows_intra(
                        config,
                        pool,
                        parser,
                        compiled_datatype_conditions,
                        compiled_rule_conditions,
                        parsed_structure_conditions,
                        table_name,
                        headers,
                        &mut rows,
                    )
                }));
                if chunk_number % num_cpus == 0 {
                    wait_for_and_process_results(
                        config,
                        pool,
                        parser,
                        compiled_datatype_conditions,
                        compiled_rule_conditions,
                        parsed_structure_conditions,
                        table_name,
                        headers,
                        chunk_number - num_cpus + 1,
                        &mut handles
                    );
                }
                last_chunk_number = chunk_number;
            }
            let left_to_handle = last_chunk_number % num_cpus;
            wait_for_and_process_results(
                config,
                pool,
                parser,
                compiled_datatype_conditions,
                compiled_rule_conditions,
                parsed_structure_conditions,
                table_name,
                headers,
                last_chunk_number - left_to_handle + 1,
                &mut handles
            );
        })
        .expect("A child thread panicked");
        */
    }
}

async fn validate_rows_inter_and_insert(
    config: &ConfigMap,
    pool: &SqlitePool,
    parser: &StartParser,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    parsed_structure_conditions: &HashMap<String, ParsedStructure>,
    table_name: &String,
    headers: &csv::StringRecord,
    intra_validated_rows: &mut Vec<ResultRow>,
    // TODO: Remove this underscore later
    _chunk_number: usize,
) -> Result<(), sqlx::Error> {
    //eprintln!("ROWS BEFORE: {:#?}", intra_validated_rows);
    validate_rows_trees(
        config,
        pool,
        parser,
        compiled_datatype_conditions,
        compiled_rule_conditions,
        parsed_structure_conditions,
        table_name,
        headers,
        intra_validated_rows,
    )
    .await?;
    //eprintln!("ROWS AFTER: {:#?}", intra_validated_rows);

    // TODO: Implement the rest.

    Ok(())
}

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
                let sorted = sorted.iter().map(|&item| item.to_string()).collect::<Vec<_>>();
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
        Ok(table_list) => {
            return table_list;
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

fn get_sql_type(dt_config: &ConfigMap, datatype: &String) -> Option<String> {
    if !dt_config.contains_key(datatype) {
        return None;
    }

    if let Some(sql_type) = dt_config.get(datatype).and_then(|d| d.get("SQL type")) {
        return Some(sql_type.to_string());
    }

    let mut parent_datatype = dt_config
        .get(datatype)
        .and_then(|d| d.get("parent"))
        .and_then(|s| Some(s.to_string()))
        .unwrap();

    // Remove enclosing quotes that are added as a side-effect of the to_string() conversion
    parent_datatype = parent_datatype.split_off(1);
    parent_datatype.truncate(parent_datatype.len() - 1);
    return get_sql_type(dt_config, &parent_datatype);
}

fn create_table(
    tables_config: &mut ConfigMap,
    datatypes_config: &mut ConfigMap,
    parser: &StartParser,
    table_name: &String,
) -> (String, SerdeValue) {
    let mut output = vec![
        format!(r#"DROP TABLE IF EXISTS "{}";"#, table_name),
        format!(r#"CREATE TABLE "{}" ("#, table_name),
        String::from(r#"  "row_number" INTEGER,"#),
    ];

    let normal_table_name;
    if let Some(s) = table_name.strip_suffix("_conflict") {
        normal_table_name = String::from(s);
    } else {
        normal_table_name = table_name.to_string();
    }

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

    let colvals = columns.values();
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
        )
        .and_then(|mut s| {
            // Remove enclosing quotes that are added as a side-effect of the to_string()
            // conversion
            // TODO: See if we can avoid having to do this here and elsewhere.
            // See the code in the cyclic dependency check, which seems to successfully avoid
            // this issue.
            s = s.split_off(1);
            s.truncate(s.len() - 1);
            Some(s)
        });

        if let None = sql_type {
            panic!("Missing SQL type for {}", row.get("datatype").unwrap());
        }
        let sql_type = sql_type.unwrap();
        if !SQLITE_TYPES.contains(&sql_type.to_lowercase().as_str()) {
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
                                            .and_then(|mut s| {
                                                // Remove enclosing quotes that are added as a
                                                // side-effect of the to_string() conversion
                                                // TODO: See if we can avoid having to do this here
                                                // and elsewhere. See the code in the cyclic
                                                // dependency check, which seems to successfully
                                                // avoid this issue.
                                                s = s.split_off(1);
                                                s.truncate(s.len() - 1);
                                                Some(s)
                                            })
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
        line.push_str(",");
        output.push(line);
        let metacol = format!("{}_meta", column_name);
        let mut line = format!(r#"  "{}" TEXT"#, metacol);
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
        output.push(line);
    }

    let foreign_keys = table_constraints.get("foreign").and_then(|v| v.as_array()).unwrap();
    let num_fkeys = foreign_keys.len();
    for (i, fkey) in foreign_keys.iter().enumerate() {
        output.push(format!(
            r#"  FOREIGN KEY ("{}") REFERENCES "{}"("{}"){}"#,
            fkey.get("column")
                .and_then(|s| Some(s.to_string()))
                .and_then(|mut s| {
                    // Remove enclosing quotes that are added as a side-effect of the to_string()
                    // conversion
                    s = s.split_off(1);
                    s.truncate(s.len() - 1);
                    Some(s)
                })
                .unwrap(),
            fkey.get("ftable")
                .and_then(|s| Some(s.to_string()))
                .and_then(|mut s| {
                    // Remove enclosing quotes that are added as a side-effect of the to_string()
                    // conversion
                    s = s.split_off(1);
                    s.truncate(s.len() - 1);
                    Some(s)
                })
                .unwrap(),
            fkey.get("fcolumn")
                .and_then(|s| Some(s.to_string()))
                .and_then(|mut s| {
                    // Remove enclosing quotes that are added as a side-effect of the to_string()
                    // conversion
                    s = s.split_off(1);
                    s.truncate(s.len() - 1);
                    Some(s)
                })
                .unwrap(),
            if i < (num_fkeys - 1) { "," } else { "" }
        ));
    }
    output.push(String::from(");"));

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
            output.push(format!(
                r#"CREATE UNIQUE INDEX "{}_{}_idx" ON "{}"("{}");"#,
                table_name, tree_child, table_name, tree_child
            ));
        }
    }

    // Finally, create a further unique index on row_number:
    output.push(format!(
        r#"CREATE UNIQUE INDEX "{}_row_number_idx" ON "{}"("row_number");"#,
        table_name, table_name
    ));

    let output = String::from(output.join("\n"));
    return (output, table_constraints);
}

async fn configure_and_load_db(
    specials_config: &mut ConfigMap,
    tables_config: &mut ConfigMap,
    datatypes_config: &mut ConfigMap,
    rules_config: &mut ConfigMap,
    pool: &SqlitePool,
    parser: &StartParser,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    parsed_structure_conditions: &HashMap<String, ParsedStructure>,
) -> Result<(), sqlx::Error> {
    let constraints_config =
        // TODO: set both of these flags to true
        configure_db(tables_config, datatypes_config, pool, parser, Some(false), Some(true)).await?;

    // TODO: Try (again) to do this combination step at the end of `configure_db()`.
    // Combine the individual configuration maps into one:
    let mut config: ConfigMap = SerdeMap::new();
    config.insert(String::from("special"), SerdeValue::Object(specials_config.clone()));
    config.insert(String::from("table"), SerdeValue::Object(tables_config.clone()));
    config.insert(String::from("datatype"), SerdeValue::Object(datatypes_config.clone()));
    config.insert(String::from("rule"), SerdeValue::Object(rules_config.clone()));
    config.insert(String::from("constraints"), SerdeValue::Object(constraints_config.clone()));

    load_db(
        &config,
        pool,
        parser,
        compiled_datatype_conditions,
        compiled_rule_conditions,
        parsed_structure_conditions,
    )
    .await?;

    Ok(())
}

#[async_std::main]
async fn main() -> Result<(), sqlx::Error> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: cmi-pb-terminology-rs table db_dir");
        process::exit(1);
    }
    let table = &args[1];
    let db_dir = &args[2];
    let parser = StartParser::new();

    let (
        mut specials_config,
        mut tables_config,
        mut datatypes_config,
        mut rules_config,
        compiled_datatype_conditions,
        compiled_rule_conditions,
        parsed_structure_conditions,
    ) = read_config_files(table, &parser);

    let connection_options =
        SqliteConnectOptions::from_str(format!("sqlite://{}/cmi-pb.db", db_dir).as_str())?
            .create_if_missing(true);

    let pool = SqlitePoolOptions::new().max_connections(5).connect_with(connection_options).await?;
    query("PRAGMA foreign_keys = ON").execute(&pool).await?;

    configure_and_load_db(
        &mut specials_config,
        &mut tables_config,
        &mut datatypes_config,
        &mut rules_config,
        &pool,
        &parser,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
        &parsed_structure_conditions,
    )
    .await
    .unwrap();

    Ok(())
}
