mod tests;

use crate::tests::{run_api_tests, run_dt_hierarchy_tests};
use ansi_term::Style;
use anyhow::Result;
use clap::{ArgAction, Parser, Subcommand};
use futures::{executor::block_on, TryStreamExt};
use ontodev_valve::{
    guess::guess,
    toolkit::{
        convert_undo_or_redo_record_to_change, generic_select_with_message_values,
        get_db_records_to_redo, local_sql_syntax,
    },
    valve::{JsonRow, Valve, ValveCell, ValveRow},
    SQL_PARAM,
};
use serde_json::{json, Value as SerdeValue};
use sqlx::{query as sqlx_query, Row};
use std::io;

// Help strings that are used in more than one subcommand:
static SAVE_DIR_HELP: &str = "Save tables to DIR instead of to their configured paths";

static TABLE_HELP: &str = "A table name";

static ROW_HELP: &str = "A row number";

static COLUMN_HELP: &str = "A column name or label";

static BUILD_ERROR: &str = "Error building Valve";

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

    /// Save all saveable tables as TSV files.
    SaveAll {
        #[arg(long, value_name = "DIR", action = ArgAction::Set, help = SAVE_DIR_HELP)]
        save_dir: Option<String>,
    },

    /// Save the tables from a given list as TSV files.
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

    /// Get data from the database
    Get {
        #[command(subcommand)]
        get_subcommand: GetSubcommands,
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
        add_subcommand: AddSubcommands,
    },

    /// Update rows, messages, and values in the database
    Update {
        #[command(subcommand)]
        update_subcommand: UpdateSubcommands,
    },

    /// Delete rows and messages from the database
    Delete {
        #[command(subcommand)]
        delete_subcommand: DeleteSubcommands,
    },

    /// Undo the last row change
    Undo {},

    /// Redo the last row change
    Redo {},

    /// TODO: Add docstring
    History {},

    /// Print the Valve configuration as a JSON-formatted string.
    DumpConfig {},

    /// Print the order in which Valve-managed tables will be created, as determined by their
    /// dependency relations.
    ShowTableOrder {},

    /// Print the incoming dependencies for each Valve-managed table.
    ShowIncomingDeps {},

    /// Print the outgoing dependencies for each Valve-managed table.
    ShowOutgoingDeps {},

    /// Print the SQL that is used to instantiate Valve-managed tables in a given database.
    DumpSchema {},

    /// Guess the Valve column configuration for the data table represented by a given TSV file.
    Guess {
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

        #[arg(value_name = "TABLE_TSV", action = ArgAction::Set,
              help = "The TSV file representing the table whose column configuration will be \
                      guessed.")]
        table_tsv: String,
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
    /// TODO: Add a docstring.
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

    /// Get the validation messages associated with the value of a given column of a given row
    /// from a given table.
    Messages {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "ROW", action = ArgAction::Set, help = ROW_HELP)]
        row: Option<u32>,

        #[arg(value_name = "COLUMN", action = ArgAction::Set, help = COLUMN_HELP)]
        column: Option<String>,
    },
}

#[derive(Subcommand)]
enum AddSubcommands {
    /// Add a table located at a given path.
    Table {
        #[arg(value_name = "PATH", action = ArgAction::Set,
              help = "The filesystem path of the table")]
        path: String,
    },

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
    /// TODO: Add docstring
    Row {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "ROW", action = ArgAction::Set, help = ROW_HELP)]
        row: Option<u32>,
    },

    /// TODO: Add docstring
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
    Row {
        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "ROW", action = ArgAction::Set, value_delimiter = ' ', num_args = 1..,
              help = ROW_HELP)]
        rows: Vec<u32>,
    },

    // TODO: Allow for syntax: ./valve delete messages SQL_LIKE_STRING
    /// TODO: Add docstring
    Message {
        #[arg(value_name = "MESSAGE_ID", action = ArgAction::Set,
              help = "The ID of the message to update")]
        message_ids: Vec<u16>,
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

    // TODO: The match arms in this function should probably be split into separate functions.
    // TODO: Add a comment about this function.
    async fn get_input_row_or_row_from_input_value(
        valve: &Valve,
        table: &str,
        row: &Option<u32>,
        column: &Option<String>,
        value: &Option<String>,
    ) -> (Option<u32>, JsonRow) {
        let mut input_row = match value {
            None => {
                // If no value has been given then we expect the whole row to be input
                // via STDIN as a (simple) JSON-formatted string, after which we convert it
                // to a ValveRow:
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
            }
            Some(value) => {
                // If a value has been given, then a column and row number must also have been
                // given. We then retrieve the row with that number from the database as a
                // ValveRow, replacing the ValveCell corresponding to the given column with a
                // new ValveCell whose value is `value`.
                let mut row = valve
                    .get_row_from_db(table, &row.expect("No row given"))
                    .await
                    .expect("Error getting row");
                let value = match serde_json::from_str::<SerdeValue>(&value) {
                    Ok(value) => value,
                    Err(_) => json!(value),
                };
                let column = column.clone().expect("No column given");
                let cell = ValveCell {
                    value: value,
                    valid: true,
                    ..Default::default()
                };
                *row.contents
                    .get_mut(&column)
                    .expect(&format!("No column '{column}' in row")) = cell;
                row.contents_to_simple_json()
                    .expect("Can't convert to simple JSON")
            }
        };

        // If the input row contains a row number as one of its cells, remove that cell and
        // add the value of the row number to the row_number field of the row instead:
        let row_number = match input_row.get("row_number") {
            None => match row {
                None => None,
                Some(row) => Some(row.clone()),
            },
            Some(value) => {
                let row_number = value.as_i64().expect("Not a number");
                let row_number = Some(row_number as u32);
                input_row
                    .remove("row_number")
                    .expect("No row_number in row");
                row_number
            }
        };

        (row_number, input_row)
    }

    // TODO: Add docstring here:
    fn parse_message_input(
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
        Commands::Add { add_subcommand } => {
            match add_subcommand {
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
                        parse_message_input(table, row, column);
                    let valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
                    let message_id = valve
                        .insert_message(&table, row, &column, &value, &level, &rule, &message)
                        .await?;
                    println!("{message_id}");
                }
                AddSubcommands::Table { .. } => todo!(),
            };
        }
        Commands::Create {} => {
            let valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
            valve
                .create_all_tables()
                .await
                .expect("Error creating tables");
        }
        Commands::Delete { delete_subcommand } => {
            let valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
            match delete_subcommand {
                DeleteSubcommands::Row { table, rows } => {
                    for row in rows {
                        valve
                            .delete_row(table, row)
                            .await
                            .expect("Could not delete row");
                    }
                }
                DeleteSubcommands::Message { message_ids } => {
                    for message_id in message_ids {
                        valve
                            .delete_message(*message_id)
                            .await
                            .expect("Could not delete message");
                    }
                }
            };
        }
        Commands::Undo {} | Commands::Redo {} => {
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
        Commands::DropAll {} => {
            let valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
            valve
                .drop_all_tables()
                .await
                .expect("Error dropping tables");
        }
        Commands::DumpConfig {} => {
            let valve = build_valve(&cli.source, "").expect(BUILD_ERROR);
            println!("{}", valve.config);
        }
        Commands::DumpSchema {} => {
            let valve = build_valve(&cli.source, "").expect(BUILD_ERROR);
            let schema = valve.dump_schema().await.expect("Error dumping schema");
            println!("{}", schema);
        }
        Commands::Get { get_subcommand } => {
            let valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
            match get_subcommand {
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
                GetSubcommands::Messages { table, row, column } => {
                    let mut sql = format!(
                        r#"SELECT "row", "column", "value", "level", "rule", "message"
                             FROM "message"
                            WHERE "table" = {SQL_PARAM}"#
                    );
                    let mut sql_params = vec![table];
                    match row {
                        Some(row) => {
                            sql.push_str(&format!(r#" AND "row" = {row}"#));
                            match column {
                                Some(column) => {
                                    sql.push_str(&format!(r#" AND "column" = {SQL_PARAM}"#));
                                    sql_params.push(column);
                                }
                                None => (),
                            }
                        }
                        None => (),
                    };
                    let sql = local_sql_syntax(
                        &valve.db_kind,
                        &format!(r#"{sql} ORDER BY "row", "column", "message_id""#,),
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
                        println!(
                            "{{\"row\":{},\"column\":{},\"value\":{},\
                             \"level\":{},\"rule\":{},\"message\":{}}}",
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
        Commands::Guess {
            sample_size,
            error_rate,
            seed,
            table_tsv,
        } => {
            let valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
            guess(
                &valve,
                cli.verbose,
                table_tsv,
                seed,
                sample_size,
                error_rate,
                cli.assume_yes,
            );
        }
        Commands::History {} => {
            let valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
            let redo_history = get_db_records_to_redo(&valve.pool, 1).await?;
            let next_redo = match redo_history.len() {
                0 => 0,
                1 => {
                    let history_id = redo_history[0]
                        .try_get::<i32, _>("history_id")
                        .expect("No 'history_id' in row");
                    history_id as u16
                }
                _ => panic!("Too many redos"),
            };
            if next_redo != 0 {
                let last_undo = convert_undo_or_redo_record_to_change(&redo_history[0])?;
                println!("▲ {} {}", last_undo.history_id, last_undo.message);
            };

            let undo_history = valve.get_changes_to_undo(0).await?;
            let next_undo = match undo_history.len() {
                0 => 0,
                _ => undo_history[0].history_id,
            };

            for undo in &undo_history {
                if undo.history_id == next_undo {
                    let line = format!("▼ {} {}", undo.history_id, undo.message);
                    println!("{}", Style::new().bold().paint(line));
                } else {
                    println!("  {} {}", undo.history_id, undo.message);
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
        Commands::ShowIncomingDeps {} => {
            let valve = build_valve(&cli.source, "").expect(BUILD_ERROR);
            print_dependencies(&valve, true);
        }
        Commands::ShowOutgoingDeps {} => {
            let valve = build_valve(&cli.source, "").expect(BUILD_ERROR);
            print_dependencies(&valve, false);
        }
        Commands::ShowTableOrder {} => {
            let valve = build_valve(&cli.source, "").expect(BUILD_ERROR);
            let sorted_table_list = valve.get_sorted_table_list(false);
            println!("{}", sorted_table_list.join(", "));
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
        Commands::Update { update_subcommand } => {
            let valve = build_valve(&cli.source, &cli.database).expect(BUILD_ERROR);
            if let UpdateSubcommands::Message {
                message_id,
                table,
                row,
                column,
            } = update_subcommand
            {
                let (table, row, column, value, level, rule, message) =
                    parse_message_input(table, row, column);
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
                let (table, row_number, input_row) = match update_subcommand {
                    UpdateSubcommands::Row { table, row } => {
                        let (row_number, row) =
                            get_input_row_or_row_from_input_value(&valve, table, row, &None, &None)
                                .await;
                        (table, row_number, row)
                    }
                    UpdateSubcommands::Value {
                        table,
                        row,
                        column,
                        value,
                    } => {
                        let (row_number, row) = get_input_row_or_row_from_input_value(
                            &valve,
                            table,
                            &Some(*row),
                            &Some(column.to_string()),
                            &Some(value.to_string()),
                        )
                        .await;
                        (table, row_number, row)
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
            let (row_number, input_row) =
                get_input_row_or_row_from_input_value(&valve, table, row, column, value).await;

            // Validate the input row:
            let output_row = valve.validate_row(table, &input_row, row_number).await?;

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
