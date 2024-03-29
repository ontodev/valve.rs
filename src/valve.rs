//! The Valve API

use crate::{
    ast::Expression,
    toolkit::{
        add_message_counts, cast_column_sql_to_text, convert_undo_or_redo_record_to_change,
        delete_row_tx, get_column_value, get_compiled_datatype_conditions,
        get_compiled_rule_conditions, get_json_from_row, get_parsed_structure_conditions,
        get_pool_from_connection_string, get_record_to_redo, get_record_to_undo, get_row_from_db,
        get_sql_for_standard_view, get_sql_for_text_view, get_sql_type,
        get_sql_type_from_global_config, get_table_ddl, insert_chunks, insert_new_row_tx,
        local_sql_syntax, read_config_files, record_row_change, switch_undone_state, update_row_tx,
        verify_table_deps_and_sort, ColumnRule, CompiledCondition, ParsedStructure,
    },
    validate::{validate_row_tx, validate_tree_foreign_keys, validate_under, with_tree_sql},
    valve_grammar::StartParser,
    CHUNK_SIZE, SQL_PARAM,
};
use anyhow::Result;
use csv::{QuoteStyle, ReaderBuilder, WriterBuilder};
use enquote::unquote;
use futures::{executor::block_on, TryStreamExt};
use indexmap::IndexMap;
use indoc::indoc;
use itertools::Itertools;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as SerdeValue};
use sqlx::{
    any::{AnyKind, AnyPool},
    query as sqlx_query, Row, ValueRef,
};
use std::{collections::HashMap, error::Error, fmt, fs::File, path::Path};

/// Alias for [Map](serde_json::map)<[String], [Value](serde_json::value)>.
pub type JsonRow = serde_json::Map<String, SerdeValue>;

/// Given a row, represented as a JSON object in the following format:
/// ```
/// {
///     "column_1": value1,
///     "column_2": value2,
///     "column_3": value3,
///     ...
/// },
/// ```
/// if a given value happens to be a string, attempt to parse it as a complex type (i.e., as an
/// [Array](SerdeValue::Array) or [Object](SerdeValue::Object)) and replace the field with the
/// parsed value if the parsing has been successful. For instance, supppose `value1` above happens
/// to be a [String](SerdeValue::String) that happens to be parsable as an
/// [Object](SerdeValue::Object) with the fields "column", "value", "level", "rule",
/// and "message", suppose `value2` happens to be an array, and suppose `value3` is a number. Then
/// the JSON object returned from this function will look like:
/// ```
/// {
///     "column_1": {
///         "column": value1_column,
///         "value": value1_value,
///         "level": value1_level,
///         "rule": value1_rule,
///         "message": value1_message,
///     },
///     "column_2": [value2_a, value2_b, ...],
///     "column_3": numeric_value_3,
///     ...
/// }
/// ```
pub fn unfold_json_row(json_row: &JsonRow) -> Result<JsonRow> {
    let mut rich_row = JsonRow::new();
    for (column, value) in json_row.iter() {
        let value = match value {
            SerdeValue::String(strvalue) => match serde_json::from_str::<SerdeValue>(strvalue) {
                Ok(SerdeValue::Array(v)) => json!(v),
                Ok(SerdeValue::Object(v)) => json!(v),
                Err(_) | Ok(_) => value.clone(),
            },
            _ => value.clone(),
        };
        rich_row.insert(column.to_string(), value);
    }
    Ok(rich_row)
}

/// Represents a particular row of data with validation results.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ValveRow {
    /// The row number of the row.
    pub row_number: Option<u32>,
    /// A map from column names to the cells corresponding to each column in the row.
    pub contents: IndexMap<String, ValveCell>,
}

impl ValveRow {
    /// Given a row number and a row represented as a JSON object in the following ('simple')
    /// format:
    /// ```
    /// {
    ///     "column_1": value1,
    ///     "column_2": value2,
    ///     ...
    /// },
    /// ```
    /// convert it to a [ValveRow] and return it.
    pub fn from_simple_json(row: &JsonRow, row_number: Option<u32>) -> Result<Self> {
        let mut valve_cells = IndexMap::new();
        for (column, value) in row.iter() {
            valve_cells.insert(column.to_string(), ValveCell::new(value));
        }
        Ok(Self {
            row_number: row_number,
            contents: valve_cells,
        })
    }

    /// Given a row, with the given row number, represented as a JSON object in the following
    /// ('rich') format:
    /// ```
    /// {
    ///     "column_1": {
    ///         "nulltype": nulltype, // Optional
    ///         "valid": <true|false>,
    ///         "messages": [{"level": level, "rule": rule, "message": message}, ...],
    ///         "value": value1
    ///     },
    ///     "column_2": {
    ///         "nulltype": nulltype, // Optional
    ///         "valid": <true|false>,
    ///         "messages": [{"level": level, "rule": rule, "message": message}, ...],
    ///         "value": value2
    ///     },
    ///     ...
    /// },
    /// ```
    /// convert it into a [ValveRow] and return it.
    pub fn from_rich_json(row_number: Option<u32>, row: &JsonRow) -> Result<Self> {
        Ok(serde_json::from_str(
            &json!({
                "row_number": row_number,
                "contents": row,
            })
            .to_string(),
        )?)
    }

    /// Given a [ValveRow], convert it to a [JsonRow] using [serde_json::to_value()] and return it.
    pub fn to_rich_json(&self) -> Result<JsonRow> {
        let value = serde_json::to_value(self)?;
        value
            .as_object()
            .ok_or(
                ValveError::InputError(format!(
                    "Could not convert {:?} to a rich JSON object",
                    value
                ))
                .into(),
            )
            .cloned()
    }

    /// Given a [ValveRow], convert the `contents` field of the row to a [JsonRow] using
    /// [serde_json::to_value()] and return it.
    pub fn contents_to_rich_json(&self) -> Result<JsonRow> {
        let value = serde_json::to_value(self.contents.clone())?;
        value
            .as_object()
            .ok_or(
                ValveError::InputError(format!(
                    "Could not convert {:?} to a rich JSON object",
                    value
                ))
                .into(),
            )
            .cloned()
    }
}

/// Represents a particular cell in a particular row of data with vaildation results.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ValveCell {
    /// If present, indicates that the value of the cell is considered to be a null value of
    /// the given type. If not present, indicates that the contents of the cell are not empty.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nulltype: Option<String>,
    /// The value of the cell.
    pub value: SerdeValue,
    /// Whether the value of the cell is valid or invalid.
    pub valid: bool,
    /// Any messages associated with the cell value.
    pub messages: Vec<ValveCellMessage>,
}

impl ValveCell {
    /// Initializes a new [ValveCell] based on the given [SerdeValue] and returns it. Note that
    /// any value that is not a [String](SerdeValue::String), [Number](SerdeValue::Number), or
    /// [Bool](SerdeValue::Bool) is converted to a [String](SerdeValue::String).
    pub fn new(value: &SerdeValue) -> Self {
        let value = match value {
            SerdeValue::String(_) | SerdeValue::Number(_) | SerdeValue::Bool(_) => value.clone(),
            _ => json!(value.to_string()),
        };
        Self {
            nulltype: None,
            value: value,
            valid: true,
            messages: vec![],
        }
    }

    /// Returns the value of the cell as a String.
    pub fn strvalue(&self) -> String {
        match &self.value {
            SerdeValue::String(s) => s.to_string(),
            _ => self.value.to_string(),
        }
    }
}

/// Represents one of the messages in a [ValveCell]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ValveCellMessage {
    /// The severity of the message.
    pub level: String,
    /// The rule violation that the message is about.
    pub rule: String,
    /// The contents of the message.
    pub message: String,
}

/// Generic enum representing various error types returned by Valve methods
#[derive(Debug)]
pub enum ValveError {
    /// An error in the Valve configuration:
    ConfigError(String),
    /// An error that occurred while reading or writing to a CSV/TSV:
    CsvError(csv::Error),
    /// An error involving the data:
    DataError(String),
    /// An error generated by the underlying database:
    DatabaseError(sqlx::Error),
    /// An error in the inputs to a function:
    InputError(String),
    /// An error that occurred while reading/writing to stdio:
    IOError(std::io::Error),
    /// An error that occurred while serialising or deserialising to/from JSON:
    SerdeJsonError(serde_json::Error),
    /// An error that occurred while parsing a regex:
    RegexError(regex::Error),
    /// An error that occurred because of a user's action
    UserError(String),
}

impl std::fmt::Display for ValveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for ValveError {}

/// Represents a message associated with a particular value of a particular column.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ValveMessage {
    /// The name of the column
    pub column: String,
    /// The value of the column
    pub value: String,
    /// The rule violated by the value
    pub rule: String,
    /// The severity of the violation
    pub level: String,
    /// A description of the violation
    pub message: String,
}

/// Represents a change to a row in a database table.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ValveRowChange {
    /// The name of the table that the change is from
    pub table: String,
    /// The row number of the changed row
    pub row: u32,
    /// A summary of the change to the row
    pub message: String,
    /// The changes to each cell
    pub changes: Vec<ValveChange>,
}

/// Represents a change to a value in a row of a database table.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ValveChange {
    /// The name of the column that the value is from
    pub column: String,
    /// The kind of change (update, insert, delete)
    pub level: String,
    /// The previous contents of this column value
    pub old_value: String,
    /// The new contents of this column value
    pub value: String,
    /// A description of the change
    pub message: String,
}

/// Configuration information specific to Valve's special tables
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ValveSpecialConfig {
    /// The name of the table table
    pub table: String,
    /// The name of the column table
    pub column: String,
    /// The name of the datatype table
    pub datatype: String,
    /// The name of the rule table, or an empty string if there isn't any
    pub rule: String,
}

/// Configuration information for a particular table.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ValveTableConfig {
    /// The name of a table
    pub table: String,
    /// The table's type
    pub table_type: String,
    /// A description of the table
    pub description: String,
    /// The location of the TSV file representing the table in the filesystem
    pub path: String,
    /// The table's column configuration
    pub column: HashMap<String, ValveColumnConfig>,
    /// The order in which the table's columns should appear in the database and in TSV files.
    pub column_order: Vec<String>,
}

/// Configuration information for a particular column of a particular table
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ValveColumnConfig {
    /// The table that the column belongs to
    pub table: String,
    /// The column's name
    pub column: String,
    /// The column's datatype
    pub datatype: String,
    /// The column's description
    pub description: String,
    /// The column's label
    pub label: String,
    /// The structural constraint that should be satisfied by all of the column's values
    pub structure: String,
    /// The datatype of the column's nulltype (or the empty string if the column has none)
    pub nulltype: String,
}

/// Configuration information for a particular datatype
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ValveDatatypeConfig {
    /// The datatype's corresponding HTML type
    pub html_type: String,
    /// The datatype's corresponding SQL type
    pub sql_type: String,
    /// The regular expression which all data values of this type must match
    pub condition: String,
    /// The name of the datatype
    pub datatype: String,
    /// The datatype's description
    pub description: String,
    /// The parent datatype of the datatype
    pub parent: String,
}

/// Configuration information for a particular table rule
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ValveRuleConfig {
    /// The description of the rule
    pub description: String,
    /// The level of the rule (error, warning, info)
    pub level: String,
    /// The table with which the rule is associated
    pub table: String,
    /// The column on which the antecedent of the rule is based
    pub when_column: String,
    /// The condition that the antecedent column must satisfy for the rule to apply
    pub when_condition: String,
    /// The column to which the rule applies whenever the antecedent condition is satisfied
    pub then_column: String,
    /// The condition that the then_column must satisfy whenever the antecedent condition applies
    pub then_condition: String,
}

/// Configuration information for a particular 'tree' constraint
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ValveTreeConstraint {
    /// The child node associated with this tree
    pub child: String,
    /// The child's parent
    pub parent: String,
}

/// Configuration information for a particular 'under' constraint
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ValveUnderConstraint {
    /// The column whose values should be 'under' the given value in the given tree
    pub column: String,
    /// The table to which the reference tree belongs
    pub ttable: String,
    /// The column (i.e., the 'child') on which the reference tree is based
    pub tcolumn: String,
    /// The value that the values of `column` should be under with respect to the given tree
    pub value: SerdeValue,
}

/// Configuration information for a particular foreign key constraint
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ValveForeignConstraint {
    /// The table to which the column constrained by the key belongs
    pub table: String,
    /// The column constrained by the key
    pub column: String,
    /// The table referenced by the foreign key
    pub ftable: String,
    /// The column referenced by the foreign key
    pub fcolumn: String,
}

/// Configuration information for the constraints enforced by a particular Valve instance
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ValveConstraintConfig {
    /// A map from table names to that table's primary key constraints
    // TODO: primary would be better as HashMap<String, String>, since it is not possible to
    // have more than one primary key per table, but the below reflects the current implementation
    // which in principle allows for more than one.
    pub primary: HashMap<String, Vec<String>>,
    /// A map from table names to each given table's unique key constraints
    pub unique: HashMap<String, Vec<String>>,
    /// A map from table names to each given table's foreign key constraints
    pub foreign: HashMap<String, Vec<ValveForeignConstraint>>,
    /// A map from table names to each given table's tree constraints
    pub tree: HashMap<String, Vec<ValveTreeConstraint>>,
    /// A map from table names to each given table's under constraints
    pub under: HashMap<String, Vec<ValveUnderConstraint>>,
}

/// Configuration information for a particular Valve instance
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ValveConfig {
    /// Configuration specific to Valve's special tables
    pub special: ValveSpecialConfig,
    /// A map from table names to the configuration information for that table
    pub table: HashMap<String, ValveTableConfig>,
    /// A map from datatype names to the configuration information for that datatype
    pub datatype: HashMap<String, ValveDatatypeConfig>,
    /// A map from table names to a further map, for each column in the given table, to the
    /// conditional 'when-then' rules associated with that column. Note that 'associated with'
    /// means that the given column is the when-column of some rule defined on the table.
    pub rule: HashMap<String, HashMap<String, Vec<ValveRuleConfig>>>,
    /// Configuration specific to Valve's database and tree/under constraints
    pub constraint: ValveConstraintConfig,
}

impl fmt::Display for ValveConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let json = serde_json::to_string(&self);
        match json {
            Ok(json) => {
                write!(f, "{}", json)
            }
            Err(e) => {
                log::error!(
                    "Unable to render Valve configuration: {:?} as JSON: '{}'",
                    self,
                    e
                );
                Err(fmt::Error)
            }
        }
    }
}

/// Main entrypoint for the Valve API.
#[derive(Clone, Debug)]
pub struct Valve {
    /// The valve configuration map.
    pub config: ValveConfig,
    /// The full list of tables managed by valve, in dependency order.
    pub sorted_table_list: Vec<String>,
    /// Lists of tables that depend on a given table, indexed by table.
    pub table_dependencies_in: HashMap<String, Vec<String>>,
    /// Lists of tables that a given table depends on, indexed by table.
    pub table_dependencies_out: HashMap<String, Vec<String>>,
    /// A map from string representations of datatype conditions to the pre-compiled regexes
    /// associated with them.
    pub datatype_conditions: HashMap<String, CompiledCondition>,
    /// A map from string representations of rule conditions to the pre-compiled regexes
    /// associated with them.
    pub rule_conditions: HashMap<String, HashMap<String, Vec<ColumnRule>>>,
    /// A map from string representations of structure conditions to the parsed versions
    /// associated with them.
    pub structure_conditions: HashMap<String, ParsedStructure>,
    /// The database connection pool.
    pub pool: AnyPool,
    /// The user associated with this valve instance.
    pub user: String,
    /// When set to true, the valve instance produces more logging output.
    pub verbose: bool,
    /// When set to true, valve will ask for confirmation before automatically dropping/truncating
    /// database tables in order to satisfy a dependency.
    pub interactive: bool,
    /// Activates optimizations used for the initial loading of data.
    pub initial_load: bool,
}

impl Valve {
    /// Given a path to a table table, a path to a database, and a flag indicating whether the
    /// database should be configured for initial loading: Set up a database connection, configure
    /// VALVE, and return a new Valve struct.
    pub async fn build(table_path: &str, database: &str) -> Result<Self> {
        env_logger::init();
        let pool = get_pool_from_connection_string(database).await?;
        if pool.any_kind() == AnyKind::Sqlite {
            sqlx_query("PRAGMA foreign_keys = ON")
                .execute(&pool)
                .await?;
        }

        let parser = StartParser::new();
        let (
            specials_config,
            tables_config,
            datatypes_config,
            rules_config,
            constraints_config,
            sorted_table_list,
            table_dependencies_in,
            table_dependencies_out,
        ) = read_config_files(table_path, &parser, &pool)?;

        let config = ValveConfig {
            special: specials_config,
            table: tables_config,
            datatype: datatypes_config,
            rule: rules_config,
            constraint: constraints_config,
        };

        let datatype_conditions = get_compiled_datatype_conditions(&config, &parser)?;
        let rule_conditions = get_compiled_rule_conditions(&config, &datatype_conditions, &parser)?;
        let structure_conditions = get_parsed_structure_conditions(&config, &parser)?;

        Ok(Self {
            config: config,
            sorted_table_list: sorted_table_list.clone(),
            table_dependencies_in: table_dependencies_in,
            table_dependencies_out: table_dependencies_out,
            datatype_conditions: datatype_conditions,
            rule_conditions: rule_conditions,
            structure_conditions: structure_conditions,
            pool: pool,
            user: String::from("VALVE"),
            verbose: false,
            interactive: false,
            initial_load: false,
        })
    }

    /// Configures a SQLite database for initial loading by setting a number of unsafe PRAGMAs
    /// that are unsafe in general but suitable when setting up a Valve database for the first
    /// time. Note that if Valve's managed database is not a SQLite database, calling this function
    /// has no effect.
    pub async fn configure_for_initial_load(&mut self) -> Result<&mut Self> {
        if self.initial_load {
            Ok(self)
        } else {
            self.initial_load = true;
            if self.pool.any_kind() == AnyKind::Sqlite {
                // These pragmas are unsafe but they are used during initial loading since data
                // integrity is not a priority in this case.
                self.execute_sql("PRAGMA journal_mode = OFF").await?;
                self.execute_sql("PRAGMA synchronous = 0").await?;
                self.execute_sql("PRAGMA cache_size = 1000000").await?;
                self.execute_sql("PRAGMA temp_store = MEMORY").await?;
            }
            Ok(self)
        }
    }

    /// Convenience function to retrieve the path to Valve's "table table", the main entrypoint
    /// to Valve's configuration.
    pub fn get_path(&self) -> Result<String> {
        Ok(self
            .config
            .table
            .get("table")
            .and_then(|t| Some(t.path.as_str()))
            .ok_or(ValveError::ConfigError(
                "Table table is undefined".to_string(),
            ))?
            .to_string())
    }

    /// The maximum length of a username.
    pub const USERNAME_MAX_LEN: usize = 20;

    /// Sets the user name, which must be a short (see [USERNAME_MAX_LEN](Self::USERNAME_MAX_LEN)),
    /// trimmed, string without newlines, for this Valve instance.
    pub fn set_user(&mut self, user: &str) -> Result<&mut Self> {
        if user.len() > Self::USERNAME_MAX_LEN {
            return Err(ValveError::InputError(format!(
                "Username '{}' is longer than {} characters.",
                user,
                Self::USERNAME_MAX_LEN
            ))
            .into());
        } else {
            let user_regex = Regex::new(r#"^\S([^\n]*\S)*$"#)?;
            if !user_regex.is_match(user) {
                return Err(ValveError::InputError(format!(
                    "Username '{}' is not a short, trimmed, string without newlines.",
                    user,
                ))
                .into());
            }
        }
        self.user = user.to_string();
        Ok(self)
    }

    /// Configure verbosity
    pub fn set_verbose(&mut self, verbose: bool) -> &mut Self {
        self.verbose = verbose;
        self
    }

    /// Configure interactive mode
    pub fn set_interactive(&mut self, interactive: bool) -> &mut Self {
        self.interactive = interactive;
        self
    }

    /// (Private function.) Given a SQL string, execute it using the connection pool associated
    /// with the Valve instance.
    async fn execute_sql(&self, sql: &str) -> Result<()> {
        sqlx_query(&sql).execute(&self.pool).await?;
        Ok(())
    }

    /// Return the list of configured tables in sorted order, or reverse sorted order if the
    /// reverse flag is set.
    pub fn get_sorted_table_list(&self, reverse: bool) -> Vec<&str> {
        let mut sorted_tables = self
            .sorted_table_list
            .iter()
            .map(|i| i.as_str())
            .collect::<Vec<_>>();
        if reverse {
            sorted_tables.reverse();
        }
        sorted_tables
    }

    /// Given the name of a table, determine whether its current instantiation in the database
    /// differs from the way it has been configured. The answer to this question is yes whenever
    /// (1) the number of columns or any of their names differs from their configured values, or
    /// the order of database columns differs from the configured order; (2) The SQL type of one or
    /// more columns does not match the configured SQL type for that column; (3) Some column with a
    /// 'unique', 'primary', or 'from(table, column)' in its column configuration fails to be
    /// associated, in the database, with a unique constraint, primary key, or foreign key,
    /// respectively; or vice versa; (4) The table does not exist in the database.
    pub async fn table_has_changed(&self, table: &str) -> Result<bool> {
        // A clojure that, given a parsed structure condition, a table and column name, and an
        // unsigned integer representing whether the given column, in the case of a SQLite database,
        // is a primary key (in the case of PostgreSQL, the sqlite_pk parameter is ignored):
        // determine whether the structure of the column is properly reflected in the db. E.g., a
        // `from(table.column)` struct should be associated with a foreign key, `primary` with a
        // primary key, `unique` with a unique constraint.
        let structure_has_changed = |pstruct: &Expression,
                                     table: &str,
                                     column: &str,
                                     sqlite_pk: &u32|
         -> Result<bool> {
            // A clojure to determine whether the given column has the given constraint type, which
            // can be one of 'UNIQUE', 'PRIMARY KEY', 'FOREIGN KEY':
            let column_has_constraint_type = |constraint_type: &str| -> Result<bool> {
                if self.pool.any_kind() == AnyKind::Postgres {
                    let sql = format!(
                        r#"SELECT 1
                       FROM information_schema.table_constraints tco
                       JOIN information_schema.key_column_usage kcu
                         ON kcu.constraint_name = tco.constraint_name
                            AND kcu.constraint_schema = tco.constraint_schema
                            AND kcu.table_name = '{}'
                       WHERE tco.constraint_type = '{}'
                         AND kcu.column_name = '{}'"#,
                        table, constraint_type, column
                    );
                    let rows = block_on(sqlx_query(&sql).fetch_all(&self.pool))?;
                    if rows.len() > 1 {
                        unreachable!();
                    }
                    Ok(rows.len() == 1)
                } else {
                    if constraint_type == "PRIMARY KEY" {
                        return Ok(*sqlite_pk == 1);
                    } else if constraint_type == "UNIQUE" {
                        let sql = format!(r#"PRAGMA INDEX_LIST("{}")"#, table);
                        for row in block_on(sqlx_query(&sql).fetch_all(&self.pool))? {
                            let idx_name = row.get::<String, _>("name");
                            let unique = row.get::<i16, _>("unique") as u8;
                            if unique == 1 {
                                let sql = format!(r#"PRAGMA INDEX_INFO("{}")"#, idx_name);
                                let rows = block_on(sqlx_query(&sql).fetch_all(&self.pool))?;
                                if rows.len() == 1 {
                                    let cname = rows[0].get::<String, _>("name");
                                    if cname == column {
                                        return Ok(true);
                                    }
                                }
                            }
                        }
                        Ok(false)
                    } else if constraint_type == "FOREIGN KEY" {
                        let sql = format!(r#"PRAGMA FOREIGN_KEY_LIST("{}")"#, table);
                        for row in block_on(sqlx_query(&sql).fetch_all(&self.pool))? {
                            let cname = row.get::<String, _>("from");
                            if cname == column {
                                return Ok(true);
                            }
                        }
                        Ok(false)
                    } else {
                        return Err(ValveError::InputError(
                            format!("Unrecognized constraint type: '{}'", constraint_type).into(),
                        )
                        .into());
                    }
                }
            };

            // Check if there is a change to whether this column is a primary/unique key:
            let is_primary = match pstruct {
                Expression::Label(label) if label == "primary" => true,
                _ => false,
            };
            if is_primary != column_has_constraint_type("PRIMARY KEY")? {
                return Ok(true);
            } else if !is_primary {
                let is_unique = match pstruct {
                    Expression::Label(label) if label == "unique" => true,
                    _ => false,
                };
                let unique_in_db = column_has_constraint_type("UNIQUE")?;
                if is_unique != unique_in_db {
                    // A child of a tree constraint implies a unique db constraint, so if there is a
                    // unique constraint in the db that is not configured, that is the explanation,
                    // and in that case we do not count this as a change to the column.
                    if !unique_in_db {
                        return Ok(true);
                    } else {
                        let trees = self
                            .config
                            .constraint
                            .tree
                            .get(table)
                            .and_then(|v| Some(v.iter().map(|t| t.child.to_string())))
                            .ok_or(ValveError::ConfigError(format!(
                                "Could not determine trees for table '{}'",
                                table
                            )))?
                            .collect::<Vec<_>>();
                        if !trees.contains(&column.to_string()) {
                            return Ok(true);
                        }
                    }
                }
            }

            match pstruct {
                Expression::Function(name, args) if name == "from" => {
                    match &*args[0] {
                        Expression::Field(cfg_ftable, cfg_fcolumn) => {
                            if self.pool.any_kind() == AnyKind::Sqlite {
                                let sql = format!(r#"PRAGMA FOREIGN_KEY_LIST("{}")"#, table);
                                for row in block_on(sqlx_query(&sql).fetch_all(&self.pool))? {
                                    let from = row.get::<String, _>("from");
                                    if from == column {
                                        let db_ftable = row.get::<String, _>("table");
                                        let db_fcolumn = row.get::<String, _>("to");
                                        if *cfg_ftable != db_ftable || *cfg_fcolumn != db_fcolumn {
                                            return Ok(true);
                                        }
                                    }
                                }
                            } else {
                                let sql = format!(
                                    r#"SELECT
                                       ccu.table_name AS foreign_table_name,
                                       ccu.column_name AS foreign_column_name
                                   FROM information_schema.table_constraints AS tc
                                   JOIN information_schema.key_column_usage AS kcu
                                     ON tc.constraint_name = kcu.constraint_name
                                        AND tc.table_schema = kcu.table_schema
                                   JOIN information_schema.constraint_column_usage AS ccu
                                     ON ccu.constraint_name = tc.constraint_name
                                   WHERE tc.constraint_type = 'FOREIGN KEY'
                                     AND tc.table_name = '{}'
                                     AND kcu.column_name = '{}'"#,
                                    table, column
                                );
                                let rows = block_on(sqlx_query(&sql).fetch_all(&self.pool))?;
                                if rows.len() == 0 {
                                    // If the table doesn't even exist return true.
                                    return Ok(true);
                                } else if rows.len() > 1 {
                                    // This seems impossible given how PostgreSQL works:
                                    unreachable!();
                                } else {
                                    let row = &rows[0];
                                    let db_ftable = row.get::<String, _>("foreign_table_name");
                                    let db_fcolumn = row.get::<String, _>("foreign_column_name");
                                    if *cfg_ftable != db_ftable || *cfg_fcolumn != db_fcolumn {
                                        return Ok(true);
                                    }
                                }
                            }
                        }
                        _ => {
                            return Err(ValveError::InputError(
                                format!("Unrecognized structure: {:?}", pstruct).into(),
                            )
                            .into());
                        }
                    };
                }
                _ => (),
            };

            Ok(false)
        };

        let (columns_config, configured_column_order) = {
            let table_config =
                self.config
                    .table
                    .get(table)
                    .ok_or(ValveError::ConfigError(format!(
                        "Undefined table '{}'",
                        table
                    )))?;
            let columns_config = &table_config.column;
            let configured_column_order = {
                let mut configured_column_order = {
                    // These special identifier columns must go first:
                    if table == "message" {
                        vec!["message_id".to_string()]
                    } else if table == "history" {
                        vec!["history_id".to_string()]
                    } else {
                        vec!["row_number".to_string()]
                    }
                };
                configured_column_order.append(&mut table_config.column_order.clone());
                configured_column_order
            };

            (columns_config, configured_column_order)
        };

        let db_columns_in_order = {
            if self.pool.any_kind() == AnyKind::Sqlite {
                let sql = format!(
                    r#"SELECT 1 FROM sqlite_master WHERE "type" = 'table' AND "name" = '{}'"#,
                    table
                );
                let rows = sqlx_query(&sql).fetch_all(&self.pool).await?;
                if rows.len() == 0 {
                    if self.verbose {
                        println!(
                            "The table '{}' will be created as it does not exist in the \
                             database.",
                            table
                        );
                    }
                    return Ok(true);
                } else if rows.len() == 1 {
                    // Otherwise send another query to the db to get the column info:
                    let sql = format!(r#"PRAGMA TABLE_INFO("{}")"#, table);
                    let rows = block_on(sqlx_query(&sql).fetch_all(&self.pool))?;
                    rows.iter()
                        .map(|r| {
                            (
                                r.get::<String, _>("name"),
                                r.get::<String, _>("type"),
                                r.get::<i64, _>("pk") as u32,
                            )
                        })
                        .collect::<Vec<_>>()
                } else {
                    unreachable!();
                }
            } else {
                let sql = format!(
                    r#"SELECT "column_name", "data_type"
                   FROM "information_schema"."columns"
                   WHERE "table_name" = '{}'
                   ORDER BY "ordinal_position""#,
                    table,
                );
                let rows = sqlx_query(&sql).fetch_all(&self.pool).await?;
                if rows.len() == 0 {
                    if self.verbose {
                        println!(
                            "The table '{}' will be created as it does not exist in the \
                             database.",
                            table
                        );
                    }
                    return Ok(true);
                }
                // Otherwise we get the column name:
                rows.iter()
                    .map(|r| {
                        (
                            r.get::<String, _>("column_name"),
                            r.get::<String, _>("data_type"),
                            // The third entry is just a dummy so that the datatypes in the two
                            // wings of this if/else block match.
                            0,
                        )
                    })
                    .collect::<Vec<_>>()
            }
        };

        // Check if the order of the configured columns matches the order of the columns in the
        // database:
        let db_column_order = db_columns_in_order
            .iter()
            .map(|c| c.0.clone())
            .collect::<Vec<_>>();
        if db_column_order != configured_column_order {
            if self.verbose || self.interactive {
                print!(
                    "The table '{}' needs to be recreated because the database columns: {:?} \
                     and/or their order do not match the configured columns: {:?}. ",
                    table, db_column_order, configured_column_order
                );
                if self.interactive {
                    print!("Do you want to continue? [y/N] ");
                    if !proceed::proceed() {
                        return Err(
                            ValveError::UserError("Execution aborted by user".to_string()).into(),
                        );
                    }
                } else {
                    println!();
                }
            }
            return Ok(true);
        }

        // Check, for all tables, whether their column configuration matches the contents of the
        // database:
        for (cname, ctype, pk) in &db_columns_in_order {
            // Do not consider these special columns:
            if (table == "message" && cname == "message_id")
                || (table == "message" && cname == "row")
                || (table == "history" && cname == "history_id")
                || (table == "history" && cname == "timestamp")
                || (table == "history" && cname == "row")
                || cname == "row_number"
            {
                continue;
            }
            let column_config =
                columns_config
                    .get(cname)
                    .ok_or(ValveError::ConfigError(format!(
                        "Undefined column '{}'",
                        cname
                    )))?;
            let sql_type = get_sql_type_from_global_config(&self.config, table, &cname, &self.pool);

            // Check the column's SQL type:
            if sql_type.to_lowercase() != ctype.to_lowercase() {
                let s = sql_type.to_lowercase();
                let c = ctype.to_lowercase();
                // CHARACTER VARYING and VARCHAR are synonyms so we ignore this difference.
                if !((s.starts_with("varchar") || s.starts_with("character varying"))
                    && (c.starts_with("varchar") || c.starts_with("character varying")))
                {
                    if self.verbose || self.interactive {
                        print!(
                            "The table '{}' needs to be recreated because the SQL type of column \
                             '{}', {}, does not match the configured value: {}. ",
                            table, cname, ctype, sql_type
                        );
                        if self.interactive {
                            print!("Do you want to continue? [y/N] ");
                            if !proceed::proceed() {
                                return Err(ValveError::UserError(
                                    "Execution aborted by user".to_string(),
                                )
                                .into());
                            }
                        } else {
                            println!();
                        }
                    }
                    return Ok(true);
                }
            }

            // Check the column's structure:
            let structure = &column_config.structure;
            if structure != "" {
                let parsed_structure = self
                    .structure_conditions
                    .get(structure)
                    .and_then(|p| Some(p.parsed.clone()))
                    .ok_or(ValveError::ConfigError(format!(
                        "Undefined structure '{}'",
                        structure
                    )))?;
                if structure_has_changed(&parsed_structure, table, &cname, &pk)? {
                    if self.verbose || self.interactive {
                        print!(
                            "The table '{}' needs to be recreated because the database \
                             constraints for column '{}' do not match the configured \
                             structure, '{}'. ",
                            table, cname, structure
                        );
                        if self.interactive {
                            print!("Do you want to continue? [y/N] ");
                            if !proceed::proceed() {
                                return Err(ValveError::UserError(
                                    "Execution aborted by user".to_string(),
                                )
                                .into());
                            }
                        } else {
                            println!();
                        }
                    }
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    /// Generates and returns the DDL required to setup the database.
    pub async fn get_setup_statements(&self) -> Result<HashMap<String, Vec<String>>> {
        let tables_config = &self.config.table;
        let datatypes_config = &self.config.datatype;
        let parser = StartParser::new();

        // Begin by reading in the TSV files corresponding to the tables defined in tables_config,
        // and use that information to create the associated database tables, while saving
        // constraint information to constrains_config.
        let mut setup_statements = HashMap::new();
        for table_name in tables_config.keys().cloned().collect::<Vec<_>>() {
            // Generate the statements for creating the table and its corresponding conflict table:
            let mut table_statements = vec![];
            for table in vec![table_name.to_string(), format!("{}_conflict", table_name)] {
                let mut statements =
                    get_table_ddl(tables_config, datatypes_config, &parser, &table, &self.pool);
                table_statements.append(&mut statements);
            }

            let create_view_sql = get_sql_for_standard_view(&table_name, &self.pool);
            let create_text_view_sql =
                get_sql_for_text_view(tables_config, &table_name, &self.pool);
            table_statements.push(create_view_sql);
            table_statements.push(create_text_view_sql);

            setup_statements.insert(table_name.to_string(), table_statements);
        }

        let text_type = get_sql_type(datatypes_config, &"text".to_string(), &self.pool);

        // Generate DDL for the history table:
        let mut history_statements = vec![];
        history_statements.push(format!(
            indoc! {r#"
                CREATE TABLE "history" (
                  {history_id}
                  "table" {text_type},
                  "row" BIGINT,
                  "from" {text_type},
                  "to" {text_type},
                  "summary" {text_type},
                  "user" {text_type},
                  "undone_by" {text_type},
                  {timestamp}
                );
              "#},
            history_id = {
                if self.pool.any_kind() == AnyKind::Sqlite {
                    "\"history_id\" INTEGER PRIMARY KEY,"
                } else {
                    "\"history_id\" SERIAL PRIMARY KEY,"
                }
            },
            text_type = text_type,
            timestamp = {
                if self.pool.any_kind() == AnyKind::Sqlite {
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
        message_statements.push(format!(
            indoc! {r#"
                CREATE TABLE "message" (
                  {message_id}
                  "table" {text_type},
                  "row" BIGINT,
                  "column" {text_type},
                  "value" {text_type},
                  "level" {text_type},
                  "rule" {text_type},
                  "message" {text_type}
                );
              "#},
            message_id = {
                if self.pool.any_kind() == AnyKind::Sqlite {
                    "\"message_id\" INTEGER PRIMARY KEY,"
                } else {
                    "\"message_id\" SERIAL PRIMARY KEY,"
                }
            },
            text_type = text_type,
        ));
        message_statements.push(
            r#"CREATE INDEX "message_trc_idx" ON "message"("table", "row", "column");"#.to_string(),
        );
        setup_statements.insert("message".to_string(), message_statements);

        return Ok(setup_statements);
    }

    /// Writes the database schema to stdout.
    pub async fn dump_schema(&self) -> Result<String> {
        let setup_statements = self.get_setup_statements().await?;
        let mut output = String::from("");
        for table in self.get_sorted_table_list(false) {
            let table_statements =
                setup_statements
                    .get(table)
                    .ok_or(ValveError::ConfigError(format!(
                        "Could not find setup statements for {}",
                        table
                    )))?;
            let table_output = String::from(table_statements.join("\n"));
            output.push_str(&table_output);
        }
        Ok(output)
    }

    /// Create all configured database tables and views if they do not already exist as configured.
    pub async fn create_all_tables(&self) -> Result<&Self> {
        let setup_statements = self.get_setup_statements().await?;
        let sorted_table_list = self.get_sorted_table_list(false);
        for table in &sorted_table_list {
            if self.table_has_changed(*table).await? {
                self.drop_tables(&vec![table]).await?;
                let table_statements =
                    setup_statements
                        .get(*table)
                        .ok_or(ValveError::ConfigError(format!(
                            "Could not find setup statements for {}",
                            table
                        )))?;
                for stmt in table_statements {
                    self.execute_sql(stmt).await?;
                }
            }
        }

        Ok(self)
    }

    /// Checks whether the given table exists in the database.
    pub async fn table_exists(&self, table: &str) -> Result<bool> {
        let sql = {
            if self.pool.any_kind() == AnyKind::Sqlite {
                format!(
                    r#"SELECT 1
                       FROM "sqlite_master"
                       WHERE "type" = 'table' AND name = '{}'
                       LIMIT 1"#,
                    table
                )
            } else {
                format!(
                    r#"SELECT 1
                       FROM "information_schema"."tables"
                       WHERE "table_schema" = 'public'
                         AND "table_name" = '{}'"#,
                    table
                )
            }
        };
        let query = sqlx_query(&sql);
        let rows = query.fetch_all(&self.pool).await?;
        return Ok(rows.len() > 0);
    }

    /// Get all the incoming (tables that depend on it) or outgoing (tables it depends on)
    /// dependencies of the given table.
    pub fn get_dependencies(&self, table: &str, incoming: bool) -> Result<Vec<String>> {
        let mut dependent_tables = vec![];
        if table != "message" && table != "history" {
            let direct_deps = {
                if incoming {
                    self.table_dependencies_in
                        .get(table)
                        .ok_or(ValveError::InputError(format!(
                            "Undefined table '{}'",
                            table
                        )))?
                        .to_vec()
                } else {
                    self.table_dependencies_out
                        .get(table)
                        .ok_or(ValveError::InputError(format!(
                            "Undefined table '{}'",
                            table
                        )))?
                        .to_vec()
                }
            };
            for direct_dep in direct_deps {
                let mut indirect_deps = self.get_dependencies(&direct_dep, incoming)?;
                dependent_tables.append(&mut indirect_deps);
                dependent_tables.push(direct_dep);
            }
        }
        Ok(dependent_tables)
    }

    /// Given a list of tables, fill it in with any further tables that are dependent upon tables
    /// in the given list. If deletion_order is true, the tables are sorted as required for
    /// deleting them all sequentially, otherwise they are ordered in reverse.
    pub fn add_dependencies(
        &self,
        tables: &Vec<&str>,
        deletion_order: bool,
    ) -> Result<Vec<String>> {
        let mut with_dups = vec![];
        for table in tables {
            let dependent_tables = self.get_dependencies(table, true)?;
            for dep_table in dependent_tables {
                with_dups.push(dep_table.to_string());
            }
            with_dups.push(table.to_string());
        }
        // The algorithm above gives the tables in the order needed for deletion. But we want
        // this function to return the creation order by default so we reverse it unless
        // the deletion_order flag is set to true.
        if !deletion_order {
            with_dups.reverse();
        }

        // Remove the duplicates from the returned table list:
        let mut tables_in_order = vec![];
        for table in with_dups.iter().unique() {
            tables_in_order.push(table.to_string());
        }
        Ok(tables_in_order)
    }

    /// Given a subset of the configured tables, return them in sorted dependency order, or in
    /// reverse dependency order if `reverse` is set to true.
    pub fn sort_tables(&self, table_subset: &Vec<&str>, reverse: bool) -> Result<Vec<String>> {
        let full_table_list = self.get_sorted_table_list(false);
        if !table_subset
            .iter()
            .all(|item| full_table_list.contains(item))
        {
            return Err(ValveError::InputError(format!(
                "[{}] contains tables that are not in the configured table list: [{}]",
                table_subset.join(", "),
                full_table_list.join(", ")
            ))
            .into());
        }

        // Filter out message and history since they are not represented in the constraints config.
        // They will be added implicitly to the list returned by verify_table_deps_and_sort.
        let filtered_subset = table_subset
            .iter()
            .filter(|m| **m != "history" && **m != "message")
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        let (sorted_subset, _, _) =
            verify_table_deps_and_sort(&filtered_subset, &self.config.constraint);

        // Since the result of verify_table_deps_and_sort() will include dependencies of the tables
        // in its input list, we filter those out here:
        let mut sorted_subset = sorted_subset
            .iter()
            .filter(|m| table_subset.contains(&m.as_str()))
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        if reverse {
            sorted_subset.reverse();
        }
        Ok(sorted_subset)
    }

    /// Returns an IndexMap, indexed by configured table, containing lists of their dependencies.
    /// If incoming is true, the lists are incoming dependencies, else they are outgoing.
    pub fn collect_dependencies(&self, incoming: bool) -> Result<IndexMap<String, Vec<String>>> {
        let tables = self.get_sorted_table_list(false);
        let mut dependencies = IndexMap::new();
        for table in tables {
            dependencies.insert(table.to_string(), self.get_dependencies(table, incoming)?);
        }
        Ok(dependencies)
    }

    /// Drop all configured tables, in reverse dependency order.
    pub async fn drop_all_tables(&self) -> Result<&Self> {
        // Drop all of the database tables in the reverse of their sorted order:
        self.drop_tables(&self.get_sorted_table_list(true)).await?;
        Ok(self)
    }

    /// Given a vector of table names, drop those tables, in the given order, including any tables
    /// that must be dropped implicitly because of a dependency relationship.
    pub async fn drop_tables(&self, tables: &Vec<&str>) -> Result<&Self> {
        let drop_list = self.add_dependencies(tables, true)?;
        for table in &drop_list {
            if *table != "message" && *table != "history" {
                let sql = format!(r#"DROP VIEW IF EXISTS "{}_text_view""#, table);
                self.execute_sql(&sql).await?;
                let sql = format!(r#"DROP VIEW IF EXISTS "{}_view""#, table);
                self.execute_sql(&sql).await?;
                let sql = format!(r#"DROP TABLE IF EXISTS "{}_conflict""#, table);
                self.execute_sql(&sql).await?;
            }
            let sql = format!(r#"DROP TABLE IF EXISTS "{}""#, table);
            self.execute_sql(&sql).await?;
        }

        Ok(self)
    }

    /// Truncate all configured tables, in reverse dependency order.
    pub async fn truncate_all_tables(&self) -> Result<&Self> {
        self.truncate_tables(&self.get_sorted_table_list(true))
            .await?;
        Ok(self)
    }

    /// Given a vector of table names, truncate those tables, in the given order, including any
    /// tables that must be truncated implicitly because of a dependency relationship.
    pub async fn truncate_tables(&self, tables: &Vec<&str>) -> Result<&Self> {
        self.create_all_tables().await?;
        let truncate_list = self.add_dependencies(tables, true)?;

        // We must use CASCADE in the case of PostgreSQL since we cannot truncate a table, T, that
        // depends on another table, T', even in the case where we have previously truncated T'.
        // SQLite does not need this. However SQLite does require that the tables be truncated in
        // deletion order (which means that it must be checking that T' is empty).
        let truncate_sql = |table: &str| -> String {
            if self.pool.any_kind() == AnyKind::Postgres {
                format!(r#"TRUNCATE TABLE "{}" RESTART IDENTITY CASCADE"#, table)
            } else {
                format!(r#"DELETE FROM "{}""#, table)
            }
        };

        for table in &truncate_list {
            let sql = truncate_sql(&table);
            self.execute_sql(&sql).await?;
            if *table != "message" && *table != "history" {
                let sql = truncate_sql(&format!("{}_conflict", table));
                self.execute_sql(&sql).await?;
            }
        }

        Ok(self)
    }

    /// Load all configured tables in dependency order. If `validate` is false, just try to insert
    /// all rows, irrespective of whether they are valid or not or will possibly trigger a db error.
    pub async fn load_all_tables(&self, validate: bool) -> Result<&Self> {
        let table_list = self.get_sorted_table_list(false);
        if self.verbose {
            println!("Processing {} tables.", table_list.len());
        }
        self.load_tables(&table_list, validate).await
    }

    /// Given a vector of table names, truncate each table, as well as any that depend on a given
    /// table via a foreign key dependency, then load the tables in `table_list` in the given order.
    /// If `validate` is false, just try to insert all rows, irrespective of whether they are valid
    /// or not or whether they may trigger a db error, otherwise all rows are validated as they are
    /// inserted to the database.
    pub async fn load_tables(&self, table_list: &Vec<&str>, validate: bool) -> Result<&Self> {
        let list_for_truncation = self.sort_tables(table_list, true)?;
        self.truncate_tables(
            &list_for_truncation
                .iter()
                .map(|i| i.as_str())
                .collect::<Vec<_>>(),
        )
        .await?;

        let num_tables = table_list.len();
        let mut total_errors = 0;
        let mut total_warnings = 0;
        let mut total_infos = 0;
        let mut table_num = 1;
        for table_name in table_list {
            if *table_name == "message" || *table_name == "history" {
                continue;
            }
            let table_name = table_name.to_string();
            let path = String::from(
                &self
                    .config
                    .table
                    .get(&table_name)
                    .ok_or(ValveError::InputError(format!(
                        "Undefined table '{}'",
                        table_name
                    )))?
                    .path,
            );
            let mut rdr = {
                match File::open(path.clone()) {
                    Err(e) => {
                        log::warn!("Unable to open '{}': {}", path.clone(), e);
                        continue;
                    }
                    Ok(table_file) => ReaderBuilder::new()
                        .has_headers(false)
                        .delimiter(b'\t')
                        .from_reader(table_file),
                }
            };
            if self.verbose {
                println!("Loading table {}/{}: {}", table_num, num_tables, table_name);
            }
            table_num += 1;

            // Extract the headers, which we will need later:
            let mut records = rdr.records();
            let headers;
            if let Some(result) = records.next() {
                headers = result.unwrap();
            } else {
                return Err(ValveError::DataError(format!("'{}' is empty", path)).into());
            }

            for header in headers.iter() {
                if header.trim().is_empty() {
                    return Err(ValveError::DataError(format!(
                        "One or more of the header fields is empty for table '{}'",
                        table_name
                    ))
                    .into());
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
            insert_chunks(
                &self.config,
                &self.pool,
                &self.datatype_conditions,
                &self.rule_conditions,
                &table_name,
                &chunks,
                &headers,
                &mut messages_stats,
                self.verbose,
                validate,
            )
            .await?;

            if validate {
                // We need to wait until all of the rows for a table have been loaded before
                // validating the "foreign" constraints on a table's trees, since this checks if the
                // values of one column (the tree's parent) are all contained in another column (the
                // tree's child). We also need to wait before validating a table's "under"
                // constraints. Although the tree associated with such a constraint need not be
                // defined on the same table, it can be.
                let mut recs_to_update =
                    validate_tree_foreign_keys(&self.config, &self.pool, None, &table_name, None)
                        .await?;
                recs_to_update.append(
                    &mut validate_under(&self.config, &self.pool, None, &table_name, None).await?,
                );

                for record in recs_to_update {
                    let row_number = record.get("row_number").unwrap();
                    let column_name = record.get("column").and_then(|s| s.as_str()).unwrap();
                    let value = record.get("value").and_then(|s| s.as_str()).unwrap();
                    let level = record.get("level").and_then(|s| s.as_str()).unwrap();
                    let rule = record.get("rule").and_then(|s| s.as_str()).unwrap();
                    let message = record.get("message").and_then(|s| s.as_str()).unwrap();

                    let sql = local_sql_syntax(
                        &self.pool,
                        &format!(
                            r#"INSERT INTO "message"
                       ("table", "row", "column", "value", "level", "rule", "message")
                       VALUES ({}, {}, {}, {}, {}, {}, {})"#,
                            SQL_PARAM,
                            row_number,
                            SQL_PARAM,
                            SQL_PARAM,
                            SQL_PARAM,
                            SQL_PARAM,
                            SQL_PARAM
                        ),
                    );
                    let mut query = sqlx_query(&sql);
                    query = query.bind(&table_name);
                    query = query.bind(&column_name);
                    query = query.bind(&value);
                    query = query.bind(&level);
                    query = query.bind(&rule);
                    query = query.bind(&message);
                    query.execute(&self.pool).await?;

                    if self.verbose {
                        // Add the generated message to messages_stats:
                        let messages = vec![ValveCellMessage {
                            message: message.to_string(),
                            level: level.to_string(),
                            ..Default::default()
                        }];
                        add_message_counts(&messages, &mut messages_stats);
                    }
                }
            }

            if self.verbose {
                // Output a report on the messages generated to stderr:
                let errors = messages_stats.get("error").unwrap();
                let warnings = messages_stats.get("warning").unwrap();
                let infos = messages_stats.get("info").unwrap();
                let status_message = format!(
                    "{} errors, {} warnings, and {} information messages generated for {}",
                    errors, warnings, infos, table_name
                );
                println!("{}", status_message);
                total_errors += errors;
                total_warnings += warnings;
                total_infos += infos;
            }
        }

        if self.verbose {
            println!(
                "Loading complete with {} errors, {} warnings, and {} information messages",
                total_errors, total_warnings, total_infos
            );
        }
        Ok(self)
    }

    /// Save all configured tables to their configured paths, unless save_dir is specified,
    /// in which case save them there instead.
    pub fn save_all_tables(&self, save_dir: &Option<String>) -> Result<&Self> {
        let tables = self.get_sorted_table_list(false);
        self.save_tables(&tables, save_dir)?;
        Ok(self)
    }

    /// Given a vector of table names, save those tables to their configured path's, unless
    /// save_dir is specified, in which case save them there instead.
    pub fn save_tables(&self, tables: &Vec<&str>, save_dir: &Option<String>) -> Result<&Self> {
        let table_paths: HashMap<String, String> = self
            .config
            .table
            .iter()
            .filter(|(k, v)| {
                !["message", "history"].contains(&k.as_str())
                    && tables.contains(&k.as_str())
                    && v.path != ""
            })
            .map(|(k, v)| (k.to_string(), v.path.to_string()))
            .collect();

        if self.verbose {
            println!(
                "Saving tables: {} ...",
                table_paths
                    .keys()
                    .map(|k| k.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        for (table, path) in table_paths.iter() {
            let columns: Vec<&str> = self
                .config
                .table
                .get(table)
                .and_then(|t| Some(t.column_order.iter().map(|i| i.as_str()).collect()))
                .ok_or(ValveError::InputError(format!(
                    "Undefined table '{}'",
                    table
                )))?;

            let path = match save_dir {
                Some(s) => format!(
                    "{}/{}",
                    s,
                    Path::new(path).file_name().and_then(|n| n.to_str()).ok_or(
                        ValveError::InputError(format!("Unable to save to '{}'", path))
                    )?,
                ),
                None => path.to_string(),
            };
            self.save_table(table, &columns, &path)?;
        }

        Ok(self)
    }

    /// Save the given table with the given columns at the given path as a TSV file.
    pub fn save_table(&self, table: &str, columns: &Vec<&str>, path: &str) -> Result<&Self> {
        let mut quoted_columns = vec!["\"row_number\"".to_string()];
        quoted_columns.append(
            &mut columns
                .iter()
                .map(|v| enquote::enquote('"', v))
                .collect::<Vec<_>>(),
        );
        let text_view = format!("\"{}_text_view\"", table);
        let sql = format!(
            r#"SELECT {} from {} ORDER BY "row_number""#,
            quoted_columns.join(", "),
            text_view
        );

        let mut writer = WriterBuilder::new()
            .delimiter(b'\t')
            .quote_style(QuoteStyle::Never)
            .from_path(path)?;
        writer.write_record(columns)?;
        let mut stream = sqlx_query(&sql).fetch(&self.pool);
        while let Some(row) = block_on(stream.try_next())? {
            let mut record: Vec<&str> = vec![];
            for column in columns.iter() {
                let cell = row.try_get::<&str, &str>(column).ok().unwrap_or_default();
                record.push(cell);
            }
            writer.write_record(record)?;
        }
        writer.flush()?;

        Ok(self)
    }

    /// Given a table name and a row, represented as a JSON object in the following ('simple')
    /// format:
    /// ```
    /// {
    ///     "column_1": value1,
    ///     "column_2": value2,
    ///     ...
    /// },
    /// ```
    /// validate the row and return the results in the form of a [ValveRow].
    pub async fn validate_row(
        &self,
        table_name: &str,
        row: &JsonRow,
        row_number: Option<u32>,
    ) -> Result<ValveRow> {
        let row = ValveRow::from_simple_json(row, row_number)?;
        validate_row_tx(
            &self.config,
            &self.datatype_conditions,
            &self.rule_conditions,
            &self.pool,
            None,
            table_name,
            &row,
            row_number,
            None,
        )
        .await
    }

    /// Given a table name and a row, represented as a JSON object in the following ('simple')
    /// format:
    /// ```
    /// {
    ///     "column_1": value1,
    ///     "column_2": value2,
    ///     ...
    /// },
    /// ```
    /// validate and insert the row to the table and return the row number of the inserted row
    /// and the row itself in the form of a [ValveRow].
    pub async fn insert_row(&self, table_name: &str, row: &JsonRow) -> Result<(u32, ValveRow)> {
        let mut tx = self.pool.begin().await?;
        let row = ValveRow::from_simple_json(row, None)?;
        let row = validate_row_tx(
            &self.config,
            &self.datatype_conditions,
            &self.rule_conditions,
            &self.pool,
            Some(&mut tx),
            table_name,
            &row,
            None,
            None,
        )
        .await?;

        let rn = insert_new_row_tx(
            &self.config,
            &self.datatype_conditions,
            &self.rule_conditions,
            &self.pool,
            &mut tx,
            table_name,
            &row,
            None,
            true,
        )
        .await?;

        let serde_row = row.contents_to_rich_json()?;
        record_row_change(&mut tx, table_name, &rn, None, Some(&serde_row), &self.user).await?;

        tx.commit().await?;
        Ok((rn, row))
    }

    /// Given a table name, a row number, and a row, represented as a JSON object in the following
    /// ('simple') format:
    /// ```
    /// {
    ///     "column_1": value1,
    ///     "column_2": value2,
    ///     ...
    /// },
    /// ```
    /// validate and update the row in the database, and return it in the form of a [ValveRow].
    pub async fn update_row(
        &self,
        table_name: &str,
        row_number: &u32,
        row: &JsonRow,
    ) -> Result<ValveRow> {
        let mut tx = self.pool.begin().await?;

        // Get the old version of the row from the database so that we can later record it to the
        // history table:
        let old_row =
            get_row_from_db(&self.config, &self.pool, &mut tx, table_name, &row_number).await?;

        let row = ValveRow::from_simple_json(row, Some(*row_number))?;
        let row = validate_row_tx(
            &self.config,
            &self.datatype_conditions,
            &self.rule_conditions,
            &self.pool,
            Some(&mut tx),
            table_name,
            &row,
            Some(*row_number),
            None,
        )
        .await?;

        update_row_tx(
            &self.config,
            &self.datatype_conditions,
            &self.rule_conditions,
            &self.pool,
            &mut tx,
            table_name,
            &row,
            row_number,
            true,
            false,
        )
        .await?;

        // Record the row update in the history table:
        let serde_row = row.contents_to_rich_json()?;
        record_row_change(
            &mut tx,
            table_name,
            row_number,
            Some(&old_row),
            Some(&serde_row),
            &self.user,
        )
        .await?;

        tx.commit().await?;
        Ok(row)
    }

    /// Given a table name and a row number, delete that row from the table.
    pub async fn delete_row(&self, table_name: &str, row_number: &u32) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        let row =
            get_row_from_db(&self.config, &self.pool, &mut tx, &table_name, row_number).await?;

        record_row_change(
            &mut tx,
            &table_name,
            row_number,
            Some(&row),
            None,
            &self.user,
        )
        .await?;

        delete_row_tx(
            &self.config,
            &self.datatype_conditions,
            &self.rule_conditions,
            &self.pool,
            &mut tx,
            table_name,
            row_number,
        )
        .await?;

        tx.commit().await?;
        Ok(())
    }

    /// Return the next recorded change to the data that can be undone, or None if there isn't any.
    pub async fn get_change_to_undo(&self) -> Result<Option<ValveRowChange>> {
        match get_record_to_undo(&self.pool).await? {
            None => Ok(None),
            Some(record) => convert_undo_or_redo_record_to_change(&record),
        }
    }

    /// Return the next recorded change to the data that can be redone, or None if there isn't any.
    pub async fn get_change_to_redo(&self) -> Result<Option<ValveRowChange>> {
        match get_record_to_redo(&self.pool).await? {
            None => Ok(None),
            Some(record) => convert_undo_or_redo_record_to_change(&record),
        }
    }

    /// Undo one change and return the change record or None if there was no change to undo.
    pub async fn undo(&self) -> Result<Option<ValveRow>> {
        let last_change = match get_record_to_undo(&self.pool).await? {
            None => {
                log::warn!("Nothing to undo.");
                return Ok(None);
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
                return Err(ValveError::DataError(
                    "Cannot redo unknown operation from None to None".into(),
                )
                .into())
            }
            (None, Some(_)) => {
                // Undo an insert:
                let mut tx = self.pool.begin().await?;

                delete_row_tx(
                    &self.config,
                    &self.datatype_conditions,
                    &self.rule_conditions,
                    &self.pool,
                    &mut tx,
                    table,
                    &row_number,
                )
                .await?;

                switch_undone_state(&self.user, history_id, true, &mut tx, &self.pool).await?;
                tx.commit().await?;
                Ok(None)
            }
            (Some(from), None) => {
                // Undo a delete:
                let mut tx = self.pool.begin().await?;

                let from = ValveRow::from_rich_json(Some(row_number), &from)?;
                insert_new_row_tx(
                    &self.config,
                    &self.datatype_conditions,
                    &self.rule_conditions,
                    &self.pool,
                    &mut tx,
                    table,
                    &from,
                    Some(row_number),
                    false,
                )
                .await?;

                switch_undone_state(&self.user, history_id, true, &mut tx, &self.pool).await?;
                tx.commit().await?;
                Ok(Some(from))
            }
            (Some(from), Some(_)) => {
                // Undo an an update:
                let mut tx = self.pool.begin().await?;

                let from = ValveRow::from_rich_json(Some(row_number), &from)?;
                update_row_tx(
                    &self.config,
                    &self.datatype_conditions,
                    &self.rule_conditions,
                    &self.pool,
                    &mut tx,
                    table,
                    &from,
                    &row_number,
                    false,
                    false,
                )
                .await?;

                switch_undone_state(&self.user, history_id, true, &mut tx, &self.pool).await?;
                tx.commit().await?;
                Ok(Some(from))
            }
        }
    }

    /// Redo one change and return the change record or None if there was no change to redo.
    pub async fn redo(&self) -> Result<Option<ValveRow>> {
        let last_undo = match get_record_to_redo(&self.pool).await? {
            None => {
                log::warn!("Nothing to redo.");
                return Ok(None);
            }
            Some(last_undo) => {
                let undone_by = last_undo.try_get_raw("undone_by")?;
                if undone_by.is_null() {
                    log::warn!("Nothing to redo.");
                    return Ok(None);
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
                return Err(ValveError::DataError(
                    "Cannot redo unknown operation from None to None".into(),
                )
                .into())
            }
            (None, Some(to)) => {
                // Redo an insert:
                let mut tx = self.pool.begin().await?;

                let to = ValveRow::from_rich_json(Some(row_number), &to)?;
                insert_new_row_tx(
                    &self.config,
                    &self.datatype_conditions,
                    &self.rule_conditions,
                    &self.pool,
                    &mut tx,
                    table,
                    &to,
                    Some(row_number),
                    false,
                )
                .await?;

                switch_undone_state(&self.user, history_id, false, &mut tx, &self.pool).await?;
                tx.commit().await?;
                Ok(Some(to))
            }
            (Some(_), None) => {
                // Redo a delete:
                let mut tx = self.pool.begin().await?;

                delete_row_tx(
                    &self.config,
                    &self.datatype_conditions,
                    &self.rule_conditions,
                    &self.pool,
                    &mut tx,
                    table,
                    &row_number,
                )
                .await?;

                switch_undone_state(&self.user, history_id, false, &mut tx, &self.pool).await?;
                tx.commit().await?;
                Ok(None)
            }
            (Some(_), Some(to)) => {
                // Redo an an update:
                let mut tx = self.pool.begin().await?;

                let to = ValveRow::from_rich_json(Some(row_number), &to)?;
                update_row_tx(
                    &self.config,
                    &self.datatype_conditions,
                    &self.rule_conditions,
                    &self.pool,
                    &mut tx,
                    table,
                    &to,
                    &row_number,
                    false,
                    false,
                )
                .await?;

                switch_undone_state(&self.user, history_id, false, &mut tx, &self.pool).await?;
                tx.commit().await?;
                Ok(Some(to))
            }
        }
    }

    /// Given a table name, a column name, and (optionally) a string to match, return a JSON array
    /// of possible valid values for the given column which contain the matching string as a
    /// substring (or all of them if no matching string is given). The JSON array returned is
    /// formatted for Typeahead, i.e., it takes the form:
    /// `[{"id": id, "label": label, "order": order}, ...]`.
    pub async fn get_matching_values(
        &self,
        table_name: &str,
        column_name: &str,
        matching_string: Option<&str>,
    ) -> Result<SerdeValue> {
        let config = &self.config;
        let datatype_conditions = &self.datatype_conditions;
        let structure_conditions = &self.structure_conditions;
        let pool = &self.pool;
        let dt_name = &config
            .table
            .get(table_name)
            .ok_or(ValveError::InputError(format!(
                "Undefined table '{}'",
                table_name
            )))?
            .column
            .get(column_name)
            .ok_or(ValveError::InputError(format!(
                "Undefined column '{}.{}'",
                table_name, column_name
            )))?
            .datatype;

        let dt_condition = datatype_conditions
            .get(dt_name)
            .and_then(|d| Some(d.parsed.clone()));

        let mut values = vec![];
        match dt_condition {
            Some(Expression::Function(name, args)) if name == "in" => {
                for arg in args {
                    if let Expression::Label(arg) = *arg {
                        // Remove the enclosing quotes from the values being returned:
                        let label = unquote(&arg).unwrap_or_else(|_| arg);
                        if let Some(s) = matching_string {
                            if label.contains(s) {
                                values.push(label);
                            }
                        }
                    }
                }
            }
            _ => {
                // If the datatype for the column does not correspond to an `in(...)` function, then
                // we check the column's structure constraints. If they include a
                // `from(foreign_table.foreign_column)` condition, then the values are taken from
                // the foreign column. Otherwise if the structure includes an
                // `under(tree_table.tree_column, value)` condition, then get the values from the
                // tree column that are under `value`.
                let structure = structure_conditions.get(
                    &config
                        .table
                        .get(table_name)
                        .ok_or(ValveError::InputError(format!(
                            "Undefined table '{}'",
                            table_name
                        )))?
                        .column
                        .get(column_name)
                        .ok_or(ValveError::InputError(format!(
                            "Undefined column '{}.{}'",
                            table_name, column_name
                        )))?
                        .structure,
                );

                let sql_type =
                    get_sql_type_from_global_config(&config, table_name, &column_name, &pool);

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
                                    let fcolumn_text = cast_column_sql_to_text(&fcolumn, &sql_type);
                                    let sql = local_sql_syntax(
                                        &pool,
                                        &format!(
                                            r#"SELECT "{}" FROM "{}" WHERE {} LIKE {}"#,
                                            fcolumn, ftable, fcolumn_text, SQL_PARAM
                                        ),
                                    );
                                    let rows = sqlx_query(&sql)
                                        .bind(&matching_string)
                                        .fetch_all(pool)
                                        .await?;
                                    for row in rows.iter() {
                                        values.push(get_column_value(&row, &fcolumn, &sql_type));
                                    }
                                }
                            }
                            Expression::Function(name, args)
                                if name == "under" || name == "tree" =>
                            {
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
                                    .constraint
                                    .tree
                                    .get(table_name)
                                    .ok_or(ValveError::ConfigError(format!(
                                        "No tree config found for table '{}' found",
                                        table_name
                                    )))?
                                    .iter()
                                    .find(|t| t.child == *tree_col)
                                    .ok_or(ValveError::ConfigError(format!(
                                        "No tree: '{}.{}' found",
                                        table_name, tree_col
                                    )))?;
                                let child_column = &tree.child;

                                let (tree_sql, mut params) = with_tree_sql(
                                    &self.config,
                                    &tree,
                                    &table_name.to_string(),
                                    &table_name.to_string(),
                                    under_val.as_ref(),
                                    None,
                                    &pool,
                                );
                                let child_column_text =
                                    cast_column_sql_to_text(&child_column, &sql_type);
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
                                    values.push(get_column_value(&row, &child_column, &sql_type));
                                }
                            }
                            _ => {
                                return Err(ValveError::DataError(format!(
                                    "Unrecognised structure: {}",
                                    original
                                ))
                                .into())
                            }
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
}
