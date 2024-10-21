mod tests;

use crate::tests::{run_api_tests, run_dt_hierarchy_tests};
use ansi_term::Style;
use anyhow::Result;
use clap::{ArgAction, Parser, Subcommand};
use futures::{executor::block_on, TryStreamExt};
use ontodev_valve::{
    guess::guess,
    toolkit::{generic_select_with_message_values, local_sql_syntax},
    valve::{JsonRow, Valve, ValveCell, ValveRow},
    SQL_PARAM,
};
use serde_json::{json, Value as SerdeValue};
use sqlx::{query as sqlx_query, Row};
use std::{collections::HashMap, io};

// Help strings that are used in more than one subcommand:
static SAVE_DIR_HELP: &str = "Save tables to DIR instead of to their configured paths";

static TABLE_HELP: &str = "A table name";

static ROW_HELP: &str = "A row number";

static COLUMN_HELP: &str = "A column name or label";

static BUILD_ERROR: &str = "Error building Valve";

static RECONFIGURE_ERROR: &str = "Could not reconfigure Valve";

#[derive(Parser)]
#[command(version,
          about = "Valve: A lightweight validation engine -- command line interface",
          long_about = None)]
struct Cli {
    /// Read the contents of the table table from the given TSV file. If unspecified, Valve
    /// will read the table table location from the environment variable VALVE_SOURCE or exit
    /// with an error if it is undefined.
    #[arg(long, action = ArgAction::Set, env = "VALVE_SOURCE")]
    source: String,

    /// Can be one of (A) A URL of the form `postgresql://...` or `sqlite://...` (B) The filename
    /// (including path) of a sqlite database. If not specified, Valve will read the database
    /// location from the environment variable VALVE_DATABASE, or exit with an error if it is
    /// undefined.
    #[arg(long, action = ArgAction::Set, env = "VALVE_DATABASE")]
    database: String,

    /// Use this option with caution. When set, Valve will not not ask the user for confirmation
    /// before executing potentially destructive operations on the database and/or table files.
    #[arg(long, action = ArgAction::SetTrue)]
    assume_yes: bool,

    /// Print more information about progress and results to the terminal
    #[arg(long, action = ArgAction::SetTrue)]
    verbose: bool,

    // Subcommands:
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    // Note that the commands are declared below in the order in which we want them to appear
    // in the usage statement when valve is run with the option --help.
    /// Load all of the Valve-managed tables in a given database with data
    Load {
        #[arg(long,
              action = ArgAction::SetTrue,
              help = "(SQLite only) When this flag is set, the database \
                      settings will be tuned for initial loading. Note that \
                      these settings are unsafe and should be used for \
                      initial loading only, as data integrity will not be \
                      guaranteed in the case of an interrupted transaction.")]
        initial_load: bool,
        // TODO: Add a --dry-run flag.
    },

    /// Create all of the Valve-managed tables in a given database without loading any data.
    Create {},

    /// Drop all of the Valve-managed tables in a given database.
    DropAll {},

    /// Save all saveable tables as TSV files. If save_dir is specified, all tables will be saved,
    /// with the same filename, to save_dir instead of to their default locations.
    SaveAll {
        #[arg(long, value_name = "DIR", action = ArgAction::Set, help = SAVE_DIR_HELP)]
        save_dir: Option<String>,
    },

    /// Save the tables from a given list as TSV files. If save_dir is specified, all tables will be
    /// saved, with the same filename, to save_dir instead of to their default locations.
    Save {
        #[arg(value_name = "LIST",
              action = ArgAction::Set,
              value_delimiter = ',',
              help = "A comma-separated list of tables to save. Note that table names with spaces \
                      must be enclosed within quotes.")]
        tables: Vec<String>,

        #[arg(long, value_name = "DIR", action = ArgAction::Set, help = SAVE_DIR_HELP)]
        save_dir: Option<String>,
    },

    /// Move the configured path for a given table to a different location
    SaveAs {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "PATH", action = ArgAction::Set,
              help = "The new location of the table's source file")]
        path: String,
    },

    /// Get data from the database
    Get {
        #[command(subcommand)]
        subcommand: GetSubcommands,
    },

    /// Validate rows
    Validate {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "ROW", action = ArgAction::Set, help = ROW_HELP)]
        row: Option<u32>,

        #[arg(value_name = "COLUMN", action = ArgAction::Set, requires = "value",
              help = COLUMN_HELP)]
        column: Option<String>,

        #[arg(value_name = "VALUE", action = ArgAction::Set,
              help = "The value, of the given column, to validate")]
        value: Option<String>,
    },

    /// Add tables, rows, and messages to a given database
    Add {
        #[command(subcommand)]
        subcommand: AddSubcommands,
    },

    /// Update rows, messages, and values in the database
    Update {
        #[command(subcommand)]
        subcommand: UpdateSubcommands,
    },

    /// Delete rows and messages from the database
    Delete {
        #[command(subcommand)]
        subcommand: DeleteSubcommands,
    },

    /// Reorder rows in database tables
    Move {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "ROW", action = ArgAction::Set,
              help = "The number of the row from TABLE to be moved")]
        row: u32,

        #[arg(value_name = "AFTER", action = ArgAction::Set,
              help = "The number of the row coming immediately before ROW in TABLE in the new row \
                      order. If this is 0, the row will be moved to the first position.")]
        after: u32,
    },

    /// Undo changes to the database
    Undo {},

    /// Redo changes to the database that have been undone
    Redo {},

    /// Show recent changes to the database
    History {
        #[arg(long, value_name = "CONTEXT", action = ArgAction::Set,
              help = "Number of lines of redo / undo context (0 = infinite)",
              default_value_t = 5)]
        context: usize,
    },

    /// Print the SQL that is used to instantiate Valve-managed tables in a given database.
    GetSchema {},

    /// Print the Valve configuration as a JSON-formatted string.
    GetConfig {
        #[command(subcommand)]
        subcommand: GetConfigSubcommands,
    },

    /// Edit the Valve configuration tables
    Config {
        #[command(subcommand)]
        subcommand: ConfigSubcommands,
    },

    /// Run a set of predefined tests, on a specified pre-loaded database, that will test Valve's
    /// Application Programmer Interface.
    TestApi {},

    /// Run a set of predefined tests, on a specified pre-loaded database, that will test the
    /// validity of the configured datatype hierarchy.
    TestDtHierarchy {},
}

#[derive(Subcommand)]
enum GetSubcommands {
    /// Get all rows from the given table.
    Table {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,
    },

    /// Get a row having a given row number from a given table.
    Row {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "ROW", action = ArgAction::Set, help = ROW_HELP)]
        row: u32,
    },

    /// Get a cell representing the value of a given column of a given row from a given table.
    Cell {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "ROW", action = ArgAction::Set, help = ROW_HELP)]
        row: u32,

        #[arg(value_name = "COLUMN", action = ArgAction::Set, help = COLUMN_HELP)]
        column: String,
    },

    /// Get the value of a given column of a given row from a given table.
    Value {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "ROW", action = ArgAction::Set, help = ROW_HELP)]
        row: u32,

        #[arg(value_name = "COLUMN", action = ArgAction::Set, help = COLUMN_HELP)]
        column: String,
    },

    /// Get validation messages from the message table.
    Messages {
        #[arg(long, action = ArgAction::Set, help = "Get the message with this specific ID",
              required = false)]
        message_id: Option<u16>,

        #[arg(long, action = ArgAction::Set, required = false,
              help = "Only get messages whose rule matches the SQL LIKE-clause given by RULE")]
        rule: Option<String>,

        #[arg(value_name = "TABLE", action = ArgAction::Set,
              help = "Only get messages for TABLE")]
        table: Option<String>,

        #[arg(value_name = "ROW", action = ArgAction::Set,
              help = "Only get messages for row ROW of table TABLE")]
        row: Option<u32>,

        #[arg(value_name = "COLUMN", action = ArgAction::Set,
              help = "Only get messages for column COLUMN of row ROW of table TABLE")]
        column: Option<String>,
    },
}

#[derive(Subcommand)]
enum GetConfigSubcommands {
    /// View the full Valve configuration
    Valve {},

    /// View a table configuration
    Table {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,
    },

    /// View a column configuration
    Column {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "COLUMN", action = ArgAction::Set, help = COLUMN_HELP)]
        column: String,
    },

    /// View a datatype configuration
    Datatype {
        #[arg(value_name = "DATATYPE", action = ArgAction::Set, help = "A datatype name")]
        datatype: String,
    },

    /// View the ancestors of a given datatype
    Ancestors {
        #[arg(value_name = "DATATYPE", action = ArgAction::Set, help = "A datatype name")]
        datatype: String,
    },

    /// View the configured constraints for a given table
    Constraints {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,
    },

    /// View the rules for a given table
    Rules {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "COLUMN", action = ArgAction::Set, help = COLUMN_HELP)]
        column: Option<String>,
    },

    /// View special configuration table names
    Special {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: Option<String>,
    },

    /// View the order in which Valve-managed tables will be created, as determined by their
    /// dependency relations.
    TableOrder {},

    /// View the incoming dependencies for each Valve-managed table.
    IncomingDeps {},

    /// View the outgoing dependencies for each Valve-managed table.
    OutgoingDeps {},
}

#[derive(Subcommand)]
enum AddSubcommands {
    /// Read a JSON-formatted string representing a row (of the form: { "column_1": value1,
    /// "column_2": value2, ...}) from STDIN and add it to a given table, optionally printing
    /// (when the global --verbose flag has been set) a JSON representation of the row, including
    /// validation information and its assigned row_number, to the terminal before exiting.
    Row {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,
    },

    /// Read a JSON-formatted string representing a row (of the form: { "table": TABLE_NAME,
    /// "row": ROW_NUMBER, "column": COLUMN_NAME, "value": VALUE, "level": LEVEL,
    /// "rule": RULE, "message": MESSAGE}) from STDIN and add it to the message table. Note
    /// that if any of the "table", "row", or "column" fields are ommitted from the input JSON
    /// then they must be specified as positional parameters.
    Message {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: Option<String>,

        #[arg(value_name = "ROW", action = ArgAction::Set, help = ROW_HELP)]
        row: Option<u32>,

        #[arg(value_name = "COLUMN", action = ArgAction::Set, help = COLUMN_HELP)]
        column: Option<String>,
    },
}

#[derive(Subcommand)]
enum UpdateSubcommands {
    /// Read a JSON-formatted string representing a row (of the form: { "column_1": value1,
    /// "column_2": value2, ...}) from STDIN and use it as a replacement for the row
    /// currently assigned the row number ROW in the given database table. If ROW is not given,
    /// then Valve expects there to be a field called "row_number" with an integer value in the
    /// input JSON.
    Row {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "ROW", action = ArgAction::Set, help = ROW_HELP)]
        row: Option<u32>,
    },

    /// Update the current value of the given column of the given row of the given table with
    /// VALUE.
    Value {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "ROW", action = ArgAction::Set, help = ROW_HELP)]
        row: u32,

        #[arg(value_name = "COLUMN", action = ArgAction::Set, help = COLUMN_HELP)]
        column: String,

        #[arg(value_name = "VALUE", action = ArgAction::Set,
              help = "The value, of the given column, to update")]
        value: String,
    },

    /// Read a JSON-formatted string representing a row (of the form: { "table": TABLE_NAME,
    /// "row": ROW_NUMBER, "column": COLUMN_NAME, "value": VALUE, "level": LEVEL,
    /// "rule": RULE, "message": MESSAGE}) from STDIN and update the row given identified by
    /// JSON_ID with the given values. Note that if any of the "table", "row", or "column" fields
    /// are ommitted from the input JSON then they must be specified as positional parameters.
    Message {
        #[arg(value_name = "MESSAGE_ID", action = ArgAction::Set,
              help = "The ID of the message to update")]
        message_id: u16,

        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: Option<String>,

        #[arg(value_name = "ROW", action = ArgAction::Set, help = ROW_HELP)]
        row: Option<u32>,

        #[arg(value_name = "COLUMN", action = ArgAction::Set, help = COLUMN_HELP)]
        column: Option<String>,
    },
}

#[derive(Subcommand)]
enum DeleteSubcommands {
    /// Delete rows from a given table.
    Row {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "ROW", action = ArgAction::Set, value_delimiter = ' ', num_args = 1..,
              help = ROW_HELP)]
        rows: Vec<u32>,
    },

    /// Delete messages from the message table.
    Messages {
        #[arg(long, action = ArgAction::Set, help = "The specific ID of the message to delete",
              required = false)]
        message_id: Option<u16>,

        #[arg(long, action = ArgAction::Set, required = false,
              help = "Delete all messages whose rule matches the SQL LIKE-clause given by RULE")]
        rule: Option<String>,
    },
}

#[derive(Subcommand)]
enum ConfigSubcommands {
    /// Add tables, columns, and datatypes to the database
    Add {
        #[command(subcommand)]
        subcommand: ConfigAddSubcommands,
    },

    /// Rename a column
    Rename {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "COLUMN", action = ArgAction::Set, help = COLUMN_HELP)]
        column: String,

        #[arg(value_name = "NEW_NAME", action = ArgAction::Set,
              help = "The desired new name for the column")]
        new_name: String,
    },

    /// Move a column to a new table
    Move {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "COLUMN", action = ArgAction::Set, help = COLUMN_HELP)]
        column: String,

        #[arg(value_name = "NEW_TABLE", action = ArgAction::Set,
              help = "The name of the table to move the column to")]
        new_table: String,
    },

    /// Delete tables, columns, datatypes and rules
    Delete {
        #[command(subcommand)]
        subcommand: ConfigDeleteSubcommands,
    },
}

#[derive(Subcommand)]
enum ConfigAddSubcommands {
    /// Add a table to the database
    Table {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "PATH", action = ArgAction::Set,
              help = "The TSV file representing the table whose column configuration will be \
                      guessed.")]
        path: String,

        #[arg(long, value_name = "SIZE", action = ArgAction::Set,
              help = "Sample size to use when guessing",
              default_value_t = 10000)]
        sample_size: usize,

        #[arg(long, value_name = "RATE", action = ArgAction::Set,
              help = "A number between 0 and 1 (inclusive) representing the proportion of errors \
                      expected",
              default_value_t = 0.1)]
        error_rate: f32,

        #[arg(long, value_name = "SEED", action = ArgAction::Set,
              help = "Use SEED for random sampling")]
        seed: Option<u64>,
    },

    /// TODO: Add docstring
    Column {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "COLUMN", action = ArgAction::Set, help = COLUMN_HELP)]
        column: String,
    },

    /// TODO: Add docstring
    Datatype {
        #[arg(value_name = "DATATYPE", action = ArgAction::Set, help = "A datatype name")]
        datatype: String,
    },
}

#[derive(Subcommand)]
enum ConfigDeleteSubcommands {
    /// TODO: Add docstring
    Table {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,
    },

    /// TODO: Add docstring
    Column {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "COLUMN", action = ArgAction::Set, help = COLUMN_HELP)]
        column: String,
    },

    /// TODO: Add docstring
    Datatype {
        #[arg(value_name = "DATATYPE", action = ArgAction::Set, help = "A datatype name")]
        datatype: String,
    },
}

#[async_std::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    // Although Valve::build() will accept a non-TSV argument (in which case that argument is
    // ignored and a table called 'table' is looked up in the given database instead), we do not
    // allow non-TSV arguments on the command line:
    if !cli.source.to_lowercase().ends_with(".tsv") {
        println!("SOURCE must be a file ending (case-insensitively) with .tsv");
        std::process::exit(1);
    }

    // This has to be done multiple times so we declare a closure. We use a closure instead of a
    // function so that the cli.verbose and cli.assume_yes fields are in scope:
    let build_valve = |source: &str, database: &str| -> Result<Valve> {
        let mut valve = block_on(Valve::build(&source, &database)).expect(BUILD_ERROR);
        valve.set_verbose(cli.verbose);
        valve.set_interactive(!cli.assume_yes);
        Ok(valve)
    };

    // Prints the table dependencies in either incoming or outgoing order.
    fn print_dependencies(valve: &Valve, incoming: bool) {
        let dependencies = valve
            .collect_dependencies(incoming)
            .expect("Could not collect dependencies");
        for (table, deps) in dependencies.iter() {
            let deps = {
                let deps = deps.iter().map(|s| format!("'{}'", s)).collect::<Vec<_>>();
                if deps.is_empty() {
                    "None".to_string()
                } else {
                    deps.join(", ")
                }
            };
            let preamble = {
                if incoming {
                    format!("Tables that depend on '{}'", table)
                } else {
                    format!("Table '{}' depends on", table)
                }
            };
            println!("{}: {}", preamble, deps);
        }
    }

    // Given a Valve instance, a table name, a row number, a column name, and an input value,
    // fetches the row from the given table with the given row number, such that the value of the
    // given column is replaced with the given input_value.
    async fn fetch_row_with_input_value(
        valve: &Valve,
        table: &str,
        row: u32,
        column: &str,
        input_value: &str,
    ) -> JsonRow {
        let mut row = valve
            .get_row_from_db(table, &row)
            .await
            .expect("Error getting row");
        let value = match serde_json::from_str::<SerdeValue>(&input_value) {
            Ok(value) => value,
            Err(_) => json!(input_value),
        };
        let cell = ValveCell {
            value: value,
            valid: true,
            ..Default::default()
        };
        *row.contents
            .get_mut(column)
            .expect(&format!("No column '{column}' in row")) = cell;
        row.contents_to_simple_json()
            .expect("Can't convert to simple JSON")
    }

    // Reads an intput row from STDIN. This should be formatted as a simple JSON, optionally with
    // a "row_number" field included. Returns the row number (if any) that was included in the
    // JSON, as well as a JSON representation of the row without the row_number field included.
    async fn input_row() -> (Option<u32>, JsonRow) {
        let mut json_row = {
            let mut json_row = String::new();
            io::stdin()
                .read_line(&mut json_row)
                .expect("Error reading from STDIN");
            let json_row = serde_json::from_str::<SerdeValue>(&json_row)
                .expect(&format!("Invalid JSON: {json_row}"))
                .as_object()
                .expect(&format!("{json_row} is not a JSON object"))
                .clone();
            json_row
        };

        // If the input row contains a row number as one of its cells, remove it from the JSON
        // and return it separately as the first position of the returned tuple. Otherwise the
        // first position in the tuple will be None.
        let row_number = match json_row.get("row_number") {
            None => None,
            Some(value) => {
                let row_number = value.as_i64().expect("Not a number");
                let row_number = Some(row_number as u32);
                json_row.remove("row_number").expect("No row_number in row");
                row_number
            }
        };

        (row_number, json_row)
    }

    // Reads and parses a JSON-formatted string representing a validation message (for the expected
    // format see, e.g., AddSubcommands::Message above), and returns the tuple:
    // (table, row, column, value, level, rule, message)
    fn read_input_message(
        table: &Option<String>,
        row: &Option<u32>,
        column: &Option<String>,
    ) -> (String, u32, String, String, String, String, String) {
        let mut json_row = String::new();
        io::stdin()
            .read_line(&mut json_row)
            .expect("Error reading from STDIN");
        let json_row = serde_json::from_str::<SerdeValue>(&json_row)
            .expect(&format!("Invalid JSON: {json_row}"))
            .as_object()
            .expect(&format!("{json_row} is not a JSON object"))
            .clone();
        let table = match json_row.get("table") {
            Some(table) => table.as_str().expect("Not a string").to_string(),
            None => match table {
                Some(table) => table.to_string(),
                None => panic!("No table given"),
            },
        };
        let row = match json_row.get("row") {
            Some(rn) => {
                let rn = rn.as_i64().expect("Not a number");
                rn as u32
            }
            None => match row {
                Some(rn) => rn.clone(),
                None => panic!("No row given"),
            },
        };
        let column = match json_row.get("column") {
            Some(column) => column.as_str().expect("Not a string").to_string(),
            None => match column {
                Some(column) => column.to_string(),
                None => panic!("No column given"),
            },
        };
        let value = match json_row.get("value") {
            Some(value) => value.as_str().expect("Not a string").to_string(),
            None => panic!("No value given"),
        };
        let level = match json_row.get("level") {
            Some(level) => level.as_str().expect("Not a string").to_string(),
            None => panic!("No level given"),
        };
        let rule = match json_row.get("rule") {
            Some(rule) => rule.as_str().expect("Not a string").to_string(),
            None => panic!("No rule given"),
        };
        let message = match json_row.get("message") {
            Some(message) => message.as_str().expect("Not a string").to_string(),
            None => panic!("No message given"),
        };

        (table, row, column, value, level, rule, message)
    }

    match &cli.command {
        Commands::Add { subcommand } => {
            match subcommand {
                AddSubcommands::Row { table } => {
                    let mut row = String::new();
                    io::stdin()
                        .read_line(&mut row)
                        .expect("Error reading from STDIN");
                    let row: SerdeValue =
                        serde_json::from_str(&row).expect(&format!("Invalid JSON: {row}"));
                    let row = row
                        .as_object()
                        .expect(&format!("{row} is not a JSON object"));
                    let valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
                    let (_, row) = valve
                        .insert_row(table, row)
                        .await
                        .expect("Error inserting row");
                    if cli.verbose {
                        println!(
                            "{}",
                            json!(row
                                .to_rich_json()
                                .expect("Error converting row to rich JSON"))
                        );
                    }
                }
                AddSubcommands::Message { table, row, column } => {
                    let (table, row, column, value, level, rule, message) =
                        read_input_message(table, row, column);
                    let valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
                    let message_id = valve
                        .insert_message(&table, row, &column, &value, &level, &rule, &message)
                        .await?;
                    println!("{message_id}");
                }
            };
        }
        Commands::Config { subcommand } => {
            let mut valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
            match subcommand {
                // TODO: Although the table table must be named "table", the column
                // table could in principle have a different name. We need to take this into account
                // when updating the column table in the match arms below.
                ConfigSubcommands::Add { subcommand } => match subcommand {
                    ConfigAddSubcommands::Table {
                        table,
                        path,
                        sample_size,
                        error_rate,
                        seed,
                    } => {
                        let table_added = guess(
                            &valve,
                            cli.verbose,
                            Some(table),
                            path,
                            seed,
                            sample_size,
                            error_rate,
                            cli.assume_yes,
                        );
                        if table_added {
                            valve.save_tables(&vec!["table", "column"], &None).await?;
                            valve.reconfigure().expect(RECONFIGURE_ERROR);
                            valve
                                .load_tables(&vec!["table", "column"], cli.assume_yes)
                                .await?;
                            valve.ensure_all_tables_created().await?;
                        }
                    }
                    ConfigAddSubcommands::Column { .. } => todo!(),
                    ConfigAddSubcommands::Datatype { .. } => todo!(),
                },
                // TODO: Note that in the below match arms, in addition to updating the column
                // name in the column table, we also need to update any structures that refer to
                // that column in other tables.
                ConfigSubcommands::Rename { .. } => todo!(),
                ConfigSubcommands::Move { .. } => todo!(),
                ConfigSubcommands::Delete { subcommand } => match subcommand {
                    ConfigDeleteSubcommands::Table { .. } => todo!(),
                    ConfigDeleteSubcommands::Column { .. } => todo!(),
                    ConfigDeleteSubcommands::Datatype { .. } => todo!(),
                },
            };
        }
        Commands::Create {} => {
            let valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
            valve
                .ensure_all_tables_created()
                .await
                .expect("Error creating tables");
        }
        Commands::Delete { subcommand } => {
            let valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
            match subcommand {
                DeleteSubcommands::Row { table, rows } => {
                    for row in rows {
                        valve
                            .delete_row(table, row)
                            .await
                            .expect("Could not delete row");
                    }
                }
                DeleteSubcommands::Messages { message_id, rule } => {
                    if let Some(message_id) = message_id {
                        valve
                            .delete_message(*message_id)
                            .await
                            .expect("Could not delete message");
                    } else {
                        if let Some(rule) = rule {
                            valve
                                .delete_messages_like(rule)
                                .await
                                .expect("Could not delete message");
                        }
                    }
                }
            };
        }
        Commands::DropAll {} => {
            let valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
            valve
                .drop_all_tables()
                .await
                .expect("Error dropping tables");
        }
        Commands::Get { subcommand } => {
            let valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
            match subcommand {
                GetSubcommands::Row { table, row } => {
                    let row = valve
                        .get_row_from_db(table, row)
                        .await
                        .expect("Error getting row");
                    println!(
                        "{}",
                        json!(row
                            .to_rich_json()
                            .expect("Error converting row to rich JSON"))
                    );
                }
                GetSubcommands::Cell { table, row, column } => {
                    let cell = valve
                        .get_cell_from_db(table, row, column)
                        .await
                        .expect("Error getting cell");
                    println!(
                        "{}",
                        json!(cell
                            .to_rich_json()
                            .expect("Error converting cell to rich JSON"))
                    );
                }
                GetSubcommands::Value { table, row, column } => {
                    let cell = valve
                        .get_cell_from_db(table, row, column)
                        .await
                        .expect("Error getting cell");
                    println!("{}", cell.strvalue());
                }
                GetSubcommands::Table { table } => {
                    let (sql, sql_params) =
                        generic_select_with_message_values(table, &valve.config, &valve.db_kind);
                    let sql = local_sql_syntax(&valve.db_kind, &sql);
                    let mut query = sqlx_query(&sql);
                    for param in &sql_params {
                        query = query.bind(param);
                    }

                    let mut row_stream = query.fetch(&valve.pool);
                    let mut is_first = true;
                    print!("[");
                    while let Some(row) = row_stream.try_next().await? {
                        if !is_first {
                            print!(",");
                        } else {
                            is_first = false;
                        }
                        let row = ValveRow::from_any_row(
                            &valve.config,
                            &valve.db_kind,
                            table,
                            &row,
                            &None,
                        )
                        .expect("Error converting to ValveRow");
                        println!(
                            "{}",
                            json!(row
                                .to_rich_json()
                                .expect("Error converting row to rich JSON"))
                        );
                    }
                    println!("]");
                }
                GetSubcommands::Messages {
                    table,
                    row,
                    column,
                    rule,
                    message_id,
                } => {
                    let mut sql = format!(
                        r#"SELECT "message_id",
                                  "table", "row", "column", "value", "level", "rule", "message"
                             FROM "message""#
                    );
                    let mut sql_params = vec![];
                    match message_id {
                        Some(message_id) => {
                            sql.push_str(&format!(r#" WHERE "message_id" = {message_id}"#));
                        }
                        None => {
                            if let Some(table) = table {
                                sql.push_str(&format!(r#"WHERE "table" = {SQL_PARAM}"#));
                                sql_params.push(table);
                            }
                            // The command-line parser will ensure that TABLE has been given
                            // whenever ROW is given, and that TABLE and ROW have both been given
                            // whenever COLUMN is given. The case of RULE is different since it is
                            // a long parameter that is parsed independently.
                            if let Some(row) = row {
                                sql.push_str(&format!(r#" AND "row" = {row}"#));
                            }
                            if let Some(column) = column {
                                sql.push_str(&format!(r#" AND "column" = {SQL_PARAM}"#));
                                sql_params.push(column);
                            }
                            if let Some(rule) = rule {
                                sql.push_str(&format!(
                                    r#" {connective} "rule" LIKE {SQL_PARAM}"#,
                                    connective = match table {
                                        None => "WHERE",
                                        Some(_) => "AND",
                                    }
                                ));
                                sql_params.push(rule);
                            }
                        }
                    };
                    let sql = local_sql_syntax(
                        &valve.db_kind,
                        &format!(r#"{sql} ORDER BY "table", "row", "column", "message_id""#,),
                    );
                    let mut query = sqlx_query(&sql);
                    for param in &sql_params {
                        query = query.bind(param);
                    }

                    let mut row_stream = query.fetch(&valve.pool);
                    let mut is_first = true;
                    print!("[");
                    while let Some(row) = row_stream.try_next().await? {
                        if !is_first {
                            print!(",");
                        } else {
                            is_first = false;
                        }
                        let rn: i64 = row.get::<i64, _>("row");
                        let rn = rn as u32;
                        let mid: i32 = row.get::<i32, _>("message_id");
                        let mid = mid as u16;
                        println!(
                            "{{\"message_id\":{},\"table\":{},\"row\":{},\"column\":{},\
                             \"value\":{},\"level\":{},\"rule\":{},\"message\":{}}}",
                            mid,
                            json!(row.get::<&str, _>("table")),
                            rn,
                            json!(row.get::<&str, _>("column")),
                            json!(row.get::<&str, _>("value")),
                            json!(row.get::<&str, _>("level")),
                            json!(row.get::<&str, _>("rule")),
                            json!(row.get::<&str, _>("message")),
                        );
                    }
                    println!("]");
                }
            };
        }
        Commands::GetConfig { subcommand } => {
            let valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
            match subcommand {
                GetConfigSubcommands::Ancestors { datatype } => {
                    println!(
                        "{}",
                        valve
                            .get_datatype_ancestor_names(datatype)
                            .iter()
                            .map(|name| {
                                if name.contains(" ") {
                                    format!("'{name}'")
                                } else {
                                    name.to_string()
                                }
                            })
                            .collect::<Vec<_>>()
                            .join(" ")
                    );
                }
                GetConfigSubcommands::Column { table, column } => {
                    let column_config = valve
                        .config
                        .table
                        .get(table)
                        .expect(&format!("Table '{table}' not found"))
                        .column
                        .get(column)
                        .expect(&format!("Column '{column}' not found"));
                    println!("{}", json!(column_config));
                }
                GetConfigSubcommands::Constraints { table } => {
                    let mut table_constraints = HashMap::new();
                    table_constraints.insert(
                        "primary",
                        json!(valve
                            .config
                            .constraint
                            .primary
                            .get(table)
                            .expect(&format!("No table '{table}'"))),
                    );
                    table_constraints.insert(
                        "unique",
                        json!(valve
                            .config
                            .constraint
                            .unique
                            .get(table)
                            .expect(&format!("No table '{table}'"))),
                    );
                    table_constraints.insert(
                        "foreign",
                        json!(valve
                            .config
                            .constraint
                            .foreign
                            .get(table)
                            .expect(&format!("No table '{table}'"))),
                    );
                    table_constraints.insert(
                        "tree",
                        json!(valve
                            .config
                            .constraint
                            .tree
                            .get(table)
                            .expect(&format!("No table '{table}'"))),
                    );
                    println!("{}", json!(table_constraints));
                }
                GetConfigSubcommands::Datatype { datatype } => {
                    let dt_config = valve
                        .config
                        .datatype
                        .get(datatype)
                        .expect(&format!("Datatype '{datatype}' not found"));
                    println!("{}", json!(dt_config));
                }
                GetConfigSubcommands::IncomingDeps {} => {
                    print_dependencies(&valve, true);
                }
                GetConfigSubcommands::OutgoingDeps {} => {
                    print_dependencies(&valve, false);
                }
                GetConfigSubcommands::Rules { table, column } => {
                    if !valve.config.table.contains_key(table) {
                        panic!("No table '{table}'");
                    }
                    if let Some(table_rules) = valve.config.rule.get(table) {
                        match column {
                            Some(column) => {
                                if let Some(column_rules) = table_rules.get(column) {
                                    println!("{}", json!(column_rules));
                                }
                            }
                            None => println!("{}", json!(table_rules)),
                        };
                    }
                }
                GetConfigSubcommands::Special { table } => {
                    match table {
                        None => {
                            println!("Table table name: '{}'", valve.config.special.table);
                            println!("Column table name: '{}'", valve.config.special.column);
                            println!("Datatype table name: '{}'", valve.config.special.datatype);
                            println!("Rule table name: '{}'", valve.config.special.rule);
                        }
                        Some(table) => {
                            let table = table.to_string();
                            match table.as_str() {
                                "table" => println!("{}", valve.config.special.table),
                                "column" => println!("{}", valve.config.special.column),
                                "datatype" => println!("{}", valve.config.special.datatype),
                                "rule" => println!("{}", valve.config.special.rule),
                                _ => panic!("Not a special table type: '{table}'"),
                            };
                        }
                    };
                }
                GetConfigSubcommands::Table { table } => {
                    let table_config = valve
                        .config
                        .table
                        .get(table)
                        .expect(&format!("{table} not found"));
                    println!("{}", json!(table_config));
                }
                GetConfigSubcommands::TableOrder {} => {
                    let sorted_table_list = valve.get_sorted_table_list(false);
                    println!("{}", sorted_table_list.join(", "));
                }
                GetConfigSubcommands::Valve {} => {
                    println!("{}", valve.config)
                }
            }
        }
        Commands::GetSchema {} => {
            let valve = build_valve(&cli.source, "").expect(BUILD_ERROR);
            let schema = valve.dump_schema().await.expect("Error dumping schema");
            println!("{}", schema);
        }
        Commands::History { context } => {
            let valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
            let mut undo_history = valve.get_changes_to_undo(*context).await?;
            let next_undo = match undo_history.len() {
                0 => 0,
                _ => undo_history[0].history_id,
            };
            undo_history.reverse();
            let id_width = next_undo.to_string().len();
            for undo in &undo_history {
                if undo.history_id == next_undo {
                    let line = format!("▲ {:>id_width$} {}", undo.history_id, undo.message);
                    println!("{}", Style::new().bold().paint(line));
                } else {
                    println!("  {:>id_width$} {}", undo.history_id, undo.message);
                }
            }

            let redo_history = valve.get_changes_to_redo(*context).await?;
            let next_redo = match redo_history.len() {
                0 => 0,
                _ => redo_history[0].history_id,
            };
            let mut highest_encountered_id = 0;
            for redo in &redo_history {
                if redo.history_id > highest_encountered_id {
                    highest_encountered_id = redo.history_id;
                }
                // We do not allow redoing changes that are older than the next record to undo.
                // If there are no such changes in the redo stack, then there will be no triangle,
                // which indicates that nothing can be redone even though there are entries in the
                // redo stack.
                if redo.history_id == next_redo && redo.history_id > next_undo {
                    println!("▼ {:>id_width$} {}", redo.history_id, redo.message);
                } else {
                    let line = format!("  {:>id_width$} {}", redo.history_id, redo.message);
                    // If the history_id under consideration is lower than the next undo, or if
                    // there is a redo operation appearing before this one in the returned results
                    // that has a greater history_id, then this is an orphaned operation that cannot
                    // be redone. We choose not to include orphaned ops in the history output:
                    if redo.history_id >= next_undo && redo.history_id >= highest_encountered_id {
                        println!("{line}");
                    }
                }
            }
        }
        Commands::Load { initial_load } => {
            let mut valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
            if *initial_load {
                block_on(valve.configure_for_initial_load())
                    .expect("Could not configure for initial load");
            }
            valve
                .load_all_tables(true)
                .await
                .expect("Error loading tables");
        }
        Commands::Move { table, row, after } => {
            let valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
            valve.move_row(table, row, after).await?;
        }
        Commands::Redo {} | Commands::Undo {} => {
            let valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
            let updated_row = match &cli.command {
                Commands::Undo {} => valve.undo().await?,
                Commands::Redo {} => valve.redo().await?,
                _ => unreachable!(),
            };
            if let Some(valve_row) = updated_row {
                print!(
                    "{}",
                    json!(valve_row
                        .to_rich_json()
                        .expect("Error converting row to rich JSON"))
                );
            }
        }
        Commands::Save { save_dir, tables } => {
            let valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
            let tables = tables
                .iter()
                .filter(|s| *s != "")
                .map(|s| s.as_str())
                .collect::<Vec<_>>();
            valve
                .save_tables(&tables, &save_dir)
                .await
                .expect("Error saving tables");
        }
        Commands::SaveAll { save_dir } => {
            let valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
            valve
                .save_all_tables(&save_dir)
                .await
                .expect("Error saving tables");
        }
        Commands::SaveAs { table, path } => {
            let mut valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
            let sql = local_sql_syntax(
                &valve.db_kind,
                &format!(r#"UPDATE "table" SET "path" = {SQL_PARAM} WHERE "table" = {SQL_PARAM}"#),
            );
            let mut query = sqlx_query(&sql);
            query = query.bind(path);
            query = query.bind(table);
            query.execute(&valve.pool).await?;
            valve.save_tables(&vec!["table"], &None).await?;
            valve.save_tables(&vec![table], &None).await?;
            valve.reconfigure().expect(RECONFIGURE_ERROR);
            valve.load_tables(&vec!["table"], true).await?;
            valve.ensure_all_tables_created().await?;
        }
        Commands::TestApi {} => {
            let valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
            run_api_tests(&valve)
                .await
                .expect("Error running API tests");
        }
        Commands::TestDtHierarchy {} => {
            let valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
            run_dt_hierarchy_tests(&valve).expect("Error running datatype hierarchy tests");
        }
        Commands::Update { subcommand } => {
            let valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
            if let UpdateSubcommands::Message {
                message_id,
                table,
                row,
                column,
            } = subcommand
            {
                let (table, row, column, value, level, rule, message) =
                    read_input_message(table, row, column);
                let valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
                valve
                    .update_message(
                        *message_id,
                        &table,
                        row,
                        &column,
                        &value,
                        &level,
                        &rule,
                        &message,
                    )
                    .await?;
            } else {
                let (table, row_number, input_row) = match subcommand {
                    UpdateSubcommands::Row { table, row } => {
                        let (json_row_number, json_row) = input_row().await;
                        let row = match json_row_number {
                            None => row.clone(),
                            Some(value) => Some(value),
                        };
                        (table, row, json_row)
                    }
                    UpdateSubcommands::Value {
                        table,
                        row,
                        column,
                        value,
                    } => {
                        let json_row =
                            fetch_row_with_input_value(&valve, table, *row, column, value).await;
                        (table, Some(*row), json_row)
                    }
                    UpdateSubcommands::Message { .. } => unreachable!(),
                };
                let output_row = match row_number {
                    None => panic!("A row number must be specified."),
                    Some(rn) => valve.update_row(table, &rn, &input_row).await?,
                };
                // Print the results to STDOUT:
                println!(
                    "{}",
                    json!(output_row
                        .to_rich_json()
                        .expect("Error converting updated row to rich JSON"))
                );
            }
        }
        Commands::Validate {
            table,
            row,
            column,
            value,
        } => {
            let valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
            let (rn, input_row) = match value {
                Some(value) => {
                    let rn = row.expect("No row given");
                    let input_row = fetch_row_with_input_value(
                        &valve,
                        table,
                        rn,
                        match column {
                            Some(column) => column,
                            None => panic!("No column given"),
                        },
                        value,
                    )
                    .await;
                    (Some(rn), input_row)
                }
                None => {
                    let (rn, input_row) = input_row().await;
                    // If now row was input, default to `row` (which could still be None)
                    let rn = match rn {
                        Some(rn) => Some(rn),
                        None => *row,
                    };
                    (rn, input_row)
                }
            };
            // Validate the input row:
            let output_row = valve.validate_row(table, &input_row, rn).await?;

            // Print the results to STDOUT:
            println!(
                "{}",
                json!(output_row
                    .to_rich_json()
                    .expect("Error converting validated row to rich JSON"))
            );

            // Set the exit status:
            let exit_code = output_row.contents.iter().all(|(_, vcell)| vcell.valid);
            std::process::exit(match exit_code {
                true => 0,
                false => 1,
            });
        }
    }

    Ok(())
}
