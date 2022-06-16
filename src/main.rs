#[macro_use]
extern crate lalrpop_util;

mod ast;
mod validate;

lalrpop_mod!(pub cmi_pb_grammar);

pub use crate::validate::{
    get_matching_values, validate_row, validate_rows_constraints, validate_rows_intra,
    validate_rows_trees, validate_tree_foreign_keys, validate_under, ResultCell, ResultRow,
};
use crate::{ast::Expression, cmi_pb_grammar::StartParser};
use crossbeam;
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
    any::{AnyConnectOptions, AnyPool, AnyPoolOptions},
    query as sqlx_query, Row, ValueRef,
};
use std::{
    collections::{BTreeMap, HashMap},
    env,
    fs::File,
    process,
    str::FromStr,
    sync::Arc,
};

// Note: serde_json::Map is backed by a BTreeMap by default
// (see https://docs.serde.rs/serde_json/map/index.html)
pub type ConfigMap = serde_json::Map<String, SerdeValue>;

lazy_static! {
    // TODO: include synonyms?
    static ref SQL_TYPES: Vec<&'static str> = vec!["text", "integer", "real", "blob"];
}

static CHUNK_SIZE: usize = 2500;
pub static MULTI_THREADED: bool = true;

/// Represents a structure such as those found in the `structure` column of the `column` table in
/// both its parsed format (i.e., as an Expression) as well as in its original format (i.e., as a
/// plain string).
#[derive(Clone, Debug)]
pub struct ParsedStructure {
    original: String,
    parsed: Expression,
}

/// Represents a condition in three different ways: in String format, as a parsed Expression,
/// and as a pre-compiled regular expression.
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

/// Represents a 'when-then' condition, as found in the `rule` table, as CompiledConditions
/// corresponding to the when and then parts of the given rule.
pub struct ColumnRule {
    when: CompiledCondition,
    then: CompiledCondition,
}

impl std::fmt::Debug for ColumnRule {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("ColumnRule").field("when", &self.when).field("then", &self.then).finish()
    }
}

/// Given a path, read a TSV file and return a vector of rows represented as ConfigMaps.
/// Note: Use this function to read "small" TSVs only. In particular, use this for the special
/// configuration tables.
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

/// Given a condition on a datatype, if the condition is a Function, then parse it using
/// StartParser, create a corresponding CompiledCondition, and return it. If the condition is a
/// Label, then look for the CompiledCondition corresponding to it in compiled_datatype_conditions
// and return it.
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

/// Given the path to a table TSV file, load and check the 'table', 'column', and 'datatype'
/// tables, and return ConfigMaps corresponding to specials, tables, datatypes, and rules, as well
/// as HashMaps for compiled datatype and rule conditions and for parsed structure conditions.
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

    // Initialize the special table entries in the specials config map:
    let mut specials_config = ConfigMap::new();
    for t in special_table_types.keys() {
        specials_config.insert(t.to_string(), SerdeValue::Null);
    }

    let path = table_table_path;
    let rows = read_tsv_into_vector(path);

    // Load table table
    let mut tables_config = ConfigMap::new();
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
    let mut parsed_structure_conditions = HashMap::new();
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
                original: structure.to_string(),
                parsed: *parsed_structure.clone(),
            };
            parsed_structure_conditions.insert(structure.to_string(), parsed_structure);
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
    let mut compiled_rule_conditions = HashMap::new();
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
                    let table_rules = HashMap::new();
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

/// Given config maps for tables and datatypes, a database connection pool, and a StartParser,
/// read in the TSV files corresponding to the tables defined in the tables config, and use that
/// information to fill in constraints information into a new config map that is then returned. If
/// the flag `write_sql_to_stdout` is set to true, emit SQL to create the database schema to STDOUT.
/// If the flag `write_to_db` is set to true, execute the SQL in the database using the given
/// connection pool.
async fn configure_db(
    tables_config: &mut ConfigMap,
    datatypes_config: &mut ConfigMap,
    pool: &AnyPool,
    parser: &StartParser,
    write_sql_to_stdout: Option<bool>,
    write_to_db: Option<bool>,
) -> Result<ConfigMap, sqlx::Error> {
    // If the optional arguments are set to None, give them default values:
    let write_sql_to_stdout = write_sql_to_stdout.unwrap_or(false);
    let write_to_db = write_to_db.unwrap_or(false);

    // This is what we will return:
    let mut constraints_config = ConfigMap::new();
    constraints_config.insert(String::from("foreign"), SerdeValue::Object(ConfigMap::new()));
    constraints_config.insert(String::from("unique"), SerdeValue::Object(ConfigMap::new()));
    constraints_config.insert(String::from("primary"), SerdeValue::Object(ConfigMap::new()));
    constraints_config.insert(String::from("tree"), SerdeValue::Object(ConfigMap::new()));
    constraints_config.insert(String::from("under"), SerdeValue::Object(ConfigMap::new()));

    // Begin by reading in the TSV files corresponding to the tables defined in tables_config, and
    // use that information to create the associated database tables, while saving constraint
    // information to constrains_config.
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
        // Get the first row of data (just to verify that there is data in the file):
        if let None = iter.next() {
            panic!("No rows in '{}'", path);
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
                sqlx_query(&table_sql)
                    .execute(pool)
                    .await
                    .expect(format!("The SQL statement: {} returned an error", table_sql).as_str());
            }
            if write_sql_to_stdout {
                println!("{}\n", table_sql);
            }
        }

        // Create a view as the union of the regular and conflict versions of the table:
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
            sqlx_query(&sql)
                .execute(pool)
                .await
                .expect(format!("The SQL statement: {} returned an error", sql).as_str());
        }
    }

    return Ok(constraints_config);
}

/// Given a configuration map, a database connection pool, a parser, HashMaps representing
/// compiled datatype and rule conditions, and a HashMap representing parsed structure conditions,
/// read in the data TSV files corresponding to each configured table, then validate and load all of
/// the corresponding data rows.
async fn load_db(
    config: &ConfigMap,
    pool: &AnyPool,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
) -> Result<(), sqlx::Error> {
    // Sort the tables according to their foreign key dependencies so that tables are always loaded
    // after the tables they depend on:
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

    // Now load the rows:
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
            let column_name = record.get("column").and_then(|c| c.as_str()).unwrap();
            let meta_name = format!("{}_meta", column_name);
            let row_number = record.get("row_number").unwrap();
            let meta = format!("{}", record.get("meta").unwrap());
            let sql = format!(
                r#"UPDATE "{}" SET "{}" = NULL, "{}" = JSON(?) WHERE "row_number" = {}"#,
                table_name, column_name, meta_name, row_number
            );
            let query = sqlx_query(&sql).bind(meta);
            query.execute(pool).await?;
        }
    }

    Ok(())
}

/// Given a configuration map, a database connection pool, maps for compiled datatype and rule
/// conditions, a table name, a number of chunks of rows to insert into the table in the database,
/// and the headers of the rows to be inserted, validate each chunk and insert the validated rows
/// to the table.
async fn validate_and_insert_chunks(
    config: &ConfigMap,
    pool: &AnyPool,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    table_name: &String,
    chunks: &IntoChunks<csv::StringRecordsIter<'_, std::fs::File>>,
    headers: &csv::StringRecord,
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
                )
                .await?;
            }
        }

        Ok(())
    }
}

/// Given a configuration map, a database connection pool, a table name, some rows to validate,
/// and the chunk number corresponding to the rows, do inter-row validation on the rows and insert
/// them to the table.
async fn validate_rows_inter_and_insert(
    config: &ConfigMap,
    pool: &AnyPool,
    table_name: &String,
    rows: &mut Vec<ResultRow>,
    chunk_number: usize,
) -> Result<(), sqlx::Error> {
    // First, do the tree validation:
    validate_rows_trees(config, pool, table_name, rows).await?;

    // Try to insert the rows to the db first without validating unique and foreign constraints.
    // If there are constraint violations this will cause a database error, in which case we then
    // explicitly do the constraint validation and insert the resulting rows:
    let ((main_sql, main_params), (_, _)) =
        make_inserts(config, table_name, rows, chunk_number).await?;

    let mut main_query = sqlx_query(&main_sql);
    for param in &main_params {
        main_query = main_query.bind(param);
    }
    let main_result = main_query.execute(pool).await;
    match main_result {
        Ok(_) => (),
        Err(_) => {
            validate_rows_constraints(config, pool, table_name, rows).await?;

            let ((main_sql, main_params), (conflict_sql, conflict_params)) =
                make_inserts(config, table_name, rows, chunk_number).await?;

            let mut main_query = sqlx_query(&main_sql);
            for param in &main_params {
                main_query = main_query.bind(param);
            }
            main_query.execute(pool).await?;

            let mut conflict_query = sqlx_query(&conflict_sql);
            for param in &conflict_params {
                conflict_query = conflict_query.bind(param);
            }
            conflict_query.execute(pool).await?;
            println!("{}\n", main_sql);
            println!("{}\n", conflict_sql);
        }
    };

    Ok(())
}

/// Given a configuration map, a table name, a number of rows, and their corresponding chunk number,
/// return a two-place tuple containing SQL strings and params for INSERT statements with VALUES for
/// all the rows in the normal and conflict versions of the table, respectively.
async fn make_inserts(
    config: &ConfigMap,
    table_name: &String,
    rows: &mut Vec<ResultRow>,
    chunk_number: usize,
) -> Result<((String, Vec<String>), (String, Vec<String>)), sqlx::Error> {
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
        table_name: &String,
        column_names: &Vec<String>,
        rows: &Vec<ResultRow>,
    ) -> (String, Vec<String>) {
        let mut lines = vec![];
        let mut params = vec![];
        for row in rows.iter() {
            let mut values = vec![format!("{}", row.row_number.unwrap())];
            for column in column_names {
                let cell = row.contents.get(column).unwrap();
                // Insert the value of the cell into the column unless it is invalid, in which case
                // insert NULL:
                if cell.nulltype == None && cell.valid {
                    values.push(String::from("?"));
                    params.push(cell.value.clone());
                } else {
                    values.push(String::from("NULL"));
                }
                // If the cell value is valid and there is no extra information (e.g., nulltype),
                // then just set the metadata to None, which can be taken to represent a "plain"
                // valid cell. Note that the only possible "extra information" is nulltype, but it's
                // possible that in the future we may add further fields to ResultCell, so it's best
                // to keep this if/else clause separate rather than merge it with the one above.
                if cell.valid && cell.nulltype == None {
                    values.push(String::from("NULL"));
                } else {
                    // TODO: This will have to handled differently for postgres:
                    values.push(String::from("JSON(?)"));
                    let mut param = json!({
                        "value": cell.value.clone(),
                        "valid": cell.valid.clone(),
                        "messages": cell.messages.clone(),
                    });
                    if cell.nulltype != None {
                        param.as_object_mut().unwrap().insert(
                            String::from("nulltype"),
                            SerdeValue::String(cell.nulltype.clone().unwrap()),
                        );
                    }
                    params.push(param.to_string());
                }
            }
            let line = values.join(", ");
            let line = format!("({})", line);
            lines.push(line);
        }
        let mut output = String::from("");
        if !lines.is_empty() {
            output.push_str(&format!(
                r#"INSERT INTO "{}" ("row_number", {}) VALUES"#,
                table_name,
                {
                    let mut all_columns = vec![];
                    for column_name in column_names {
                        let quoted_column_name = format!(r#""{}""#, column_name);
                        let quoted_meta_column_name = format!(r#""{}_meta""#, column_name);
                        all_columns.push(quoted_column_name);
                        all_columns.push(quoted_meta_column_name);
                    }
                    all_columns.join(", ")
                }
            ));
            output.push_str("\n");
            output.push_str(&lines.join(",\n"));
            output.push_str(";");
        }

        (output, params)
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

    let (main_sql, main_params) = generate_sql(&table_name, &column_names, &main_rows);
    let (conflict_sql, conflict_params) =
        generate_sql(&format!("{}_conflict", table_name), &column_names, &conflict_rows);

    Ok(((main_sql, main_params), (conflict_sql, conflict_params)))
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

/// Given the config maps for tables and datatypes, and a table name, generate a SQL schema string,
/// including each column C and its matching C_meta column, then return the schema string as well as
/// a list of the table's constraints.
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
        let column = tables_config
            .get(normal_table_name.as_str())
            .and_then(|c| c.as_object())
            .and_then(|o| o.get("column"))
            .and_then(|c| c.as_object())
            .and_then(|c| c.get(column_name))
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
            fkey.get("column").and_then(|s| s.as_str()).unwrap(),
            fkey.get("ftable").and_then(|s| s.as_str()).unwrap(),
            fkey.get("fcolumn").and_then(|s| s.as_str()).unwrap(),
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

/// Given configuration maps for specials, tables, datatypes, and rules, a database connection pool,
/// a grammar parser, compiled datatype and rule conditions, and parsed structure conditions, read
/// the TSVs corresponding to the various defined tables, then create a database containing those
/// tables and write the data from the TSVs to them, all the while writing the SQL strings used to
/// generate the database to STDOUT.
async fn configure_and_load_db(
    specials_config: &mut ConfigMap,
    tables_config: &mut ConfigMap,
    datatypes_config: &mut ConfigMap,
    rules_config: &mut ConfigMap,
    pool: &AnyPool,
    parser: &StartParser,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
) -> Result<ConfigMap, sqlx::Error> {
    let constraints_config =
        configure_db(tables_config, datatypes_config, pool, parser, Some(true), Some(true)).await?;

    // TODO: Try (again) to do this combination step at the end of `configure_db()`.
    // Combine the individual configuration maps into one:
    let mut config = ConfigMap::new();
    config.insert(String::from("special"), SerdeValue::Object(specials_config.clone()));
    config.insert(String::from("table"), SerdeValue::Object(tables_config.clone()));
    config.insert(String::from("datatype"), SerdeValue::Object(datatypes_config.clone()));
    config.insert(String::from("rule"), SerdeValue::Object(rules_config.clone()));
    config.insert(String::from("constraints"), SerdeValue::Object(constraints_config.clone()));

    load_db(&config, pool, compiled_datatype_conditions, compiled_rule_conditions).await?;

    Ok(config)
}

/// Given a database connection pool, a table name, and a row, assign a new row number to the row
/// and insert it to the database, then return the new row number.
pub async fn insert_new_row(
    pool: &AnyPool,
    table_name: &str,
    row: &ConfigMap,
) -> Result<u32, sqlx::Error> {
    let sql = format!(r#"SELECT MAX("row_number") AS "row_number" FROM "{}""#, table_name);
    let query = sqlx_query(&sql);
    let result_row = query.fetch_one(pool).await?;
    let result = result_row.try_get_raw("row_number").unwrap();
    let new_row_number: f32;
    if result.is_null() {
        new_row_number = 1.0;
    } else {
        new_row_number = result_row.get_unchecked("row_number");
    }
    let new_row_number = new_row_number as u32 + 1;

    let mut insert_columns = vec![];
    let mut insert_values = vec![];
    let mut insert_params = vec![];
    for (column, cell) in row.iter() {
        let cell = cell.as_object().unwrap();
        let cell_valid = cell.get("valid").and_then(|v| v.as_bool()).unwrap();
        let mut cell_for_insert = cell.clone();
        insert_columns
            .append(&mut vec![format!(r#""{}""#, column), format!(r#""{}_meta""#, column)]);

        // Normal column:
        if cell_valid {
            let value = cell.get("value").and_then(|v| v.as_str()).unwrap();
            cell_for_insert.remove("value");
            insert_values.push(String::from("?"));
            insert_params.push(String::from(value));
        } else {
            insert_values.push(String::from("NULL"));
        }

        // Meta column:
        if cell_valid && cell.keys().collect::<Vec<_>>() == vec!["messages", "valid", "value"] {
            insert_values.push(String::from("NULL"));
        } else {
            insert_values.push(String::from("JSON(?)"));
            let cell_for_insert = SerdeValue::Object(cell_for_insert.clone());
            insert_params.push(format!("{}", cell_for_insert));
        }
    }

    let insert_stmt = format!(
        r#"INSERT INTO "{}" ("row_number", {}) VALUES ({}, {})"#,
        table_name,
        insert_columns.join(", "),
        new_row_number,
        insert_values.join(", "),
    );

    let mut query = sqlx_query(&insert_stmt);
    for param in &insert_params {
        query = query.bind(param);
    }
    query.execute(pool).await?;

    Ok(new_row_number)
}

/// Given a database connection pool, a table name, a row, and the row number to update, update the
/// corresponding row in the database with new values as specified by `row`.
pub async fn update_row(
    pool: &AnyPool,
    table_name: &str,
    row: &ConfigMap,
    row_number: u32,
) -> Result<(), sqlx::Error> {
    let mut assignments = vec![];
    let mut params = vec![];
    for (column, cell) in row.iter() {
        let cell = cell.as_object().unwrap();
        let cell_valid = cell.get("valid").and_then(|v| v.as_bool()).unwrap();
        let mut cell_for_insert = cell.clone();
        if cell_valid {
            let value = cell.get("value").and_then(|v| v.as_str()).unwrap();
            cell_for_insert.remove("value");
            assignments.push(format!(r#""{}" = ?"#, column));
            params.push(String::from(value));
        } else {
            assignments.push(format!(r#""{}" = NULL"#, column));
        }

        if cell_valid && cell.keys().collect::<Vec<_>>() == vec!["messages", "valid", "value"] {
            assignments.push(format!(r#""{}_meta" = NULL"#, column));
        } else {
            assignments.push(format!(r#""{}_meta" = JSON(?)"#, column));
            let cell_for_insert = SerdeValue::Object(cell_for_insert.clone());
            params.push(format!("{}", cell_for_insert));
        }
    }

    let mut update_stmt = format!(r#"UPDATE "{}" SET "#, table_name);
    update_stmt.push_str(&assignments.join(", "));
    update_stmt.push_str(&format!(r#" WHERE "row_number" = {}"#, row_number));

    let mut query = sqlx_query(&update_stmt);
    for param in &params {
        query = query.bind(param);
    }
    query.execute(pool).await?;

    Ok(())
}

async fn run_tests(
    config: &ConfigMap,
    compiled_datatype_conditions: &HashMap<String, CompiledCondition>,
    compiled_rule_conditions: &HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    parsed_structure_conditions: &HashMap<String, ParsedStructure>,
    pool: &AnyPool,
) -> Result<(), sqlx::Error> {
    let matching_values = get_matching_values(
        config,
        compiled_datatype_conditions,
        parsed_structure_conditions,
        pool,
        "foobar",
        "child",
        None,
    )
    .await?;
    assert_eq!(
        matching_values,
        json!([
            {"id":"a","label":"a","order":1},
            {"id":"b","label":"b","order":2},
            {"id":"c","label":"c","order":3},
            {"id":"d","label":"d","order":4},
            {"id":"e","label":"e","order":5},
            {"id":"f","label":"f","order":6},
            {"id":"g","label":"g","order":7},
            {"id":"h","label":"h","order":8}
        ])
    );

    // NOTE: No validation of the validate/insert/update functions is done below. You must use an
    // external script to fetch the data from the database and run a diff against a known good
    // sample.
    let row = json!({
        "child": {"messages": [], "valid": true, "value": "b"},
        "parent": {"messages": [], "valid": true, "value": "f"},
        "xyzzy": {"messages": [], "valid": true, "value": "w"},
        "foo": {"messages": [], "valid": true, "value": "A"},
        "bar": {
            "messages": [
                {"level": "error", "message": "An unrelated error", "rule": "custom:unrelated"}
            ],
            "valid": false,
            "value": "B",
        },
    });

    let result_row = validate_row(
        config,
        compiled_datatype_conditions,
        compiled_rule_conditions,
        pool,
        "foobar",
        row.as_object().unwrap(),
        true,
        Some(1),
    )
    .await?;
    update_row(pool, "foobar", &result_row, 1).await?;

    let row = json!({
        "id": {"messages": [], "valid": true, "value": "BFO:0000027"},
        "label": {"messages": [], "valid": true, "value": "car"},
        "parent": {
            "messages": [
                {"level": "error", "message": "An unrelated error", "rule": "custom:unrelated"}
            ],
            "valid": false,
            "value": "barrie",
        },
        "source": {"messages": [], "valid": true, "value": "BFOBBER"},
        "type": {"messages": [], "valid": true, "value": "owl:Class"},
    });

    let result_row = validate_row(
        config,
        compiled_datatype_conditions,
        compiled_rule_conditions,
        pool,
        "import",
        row.as_object().unwrap(),
        false,
        None,
    )
    .await?;
    let _new_row_num = insert_new_row(pool, "import", &result_row).await?;

    Ok(())
}

#[async_std::main]
async fn main() -> Result<(), sqlx::Error> {
    let args: Vec<String> = env::args().collect();
    let test;
    let table;
    let db_dir;
    if args.len() == 3 {
        test = false;
        table = &args[1];
        db_dir = &args[2];
    } else if args.len() == 4 && &args[1] == "--test" {
        test = true;
        table = &args[2];
        db_dir = &args[3];
    } else {
        eprintln!("Usage: cmi-pb-terminology-rs [--test] table db_dir");
        process::exit(1);
    }
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
        AnyConnectOptions::from_str(format!("sqlite://{}/cmi-pb.db?mode=rwc", db_dir).as_str())?;
    let pool = AnyPoolOptions::new().max_connections(5).connect_with(connection_options).await?;
    sqlx_query("PRAGMA foreign_keys = ON").execute(&pool).await?;

    // To connect to a postgresql database listening to a unix domain socket:
    // ----------------------------------------------------------------------
    // let connection_options =
    //     AnyConnectOptions::from_str("postgres:///testdb?host=/var/run/postgresql")?;
    //
    // To query the connection type at runtime via the pool:
    // -----------------------------------------------------
    // let db_type = pool.any_kind();

    let config = configure_and_load_db(
        &mut specials_config,
        &mut tables_config,
        &mut datatypes_config,
        &mut rules_config,
        &pool,
        &parser,
        &compiled_datatype_conditions,
        &compiled_rule_conditions,
    )
    .await
    .unwrap();

    if test {
        run_tests(
            &config,
            &compiled_datatype_conditions,
            &compiled_rule_conditions,
            &parsed_structure_conditions,
            &pool,
        )
        .await?;
    }

    Ok(())
}
