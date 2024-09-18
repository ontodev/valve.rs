mod tests;

use crate::tests::{run_api_tests, run_dt_hierarchy_tests};
use anyhow::Result;
use clap::{ArgAction, Parser, Subcommand};
use futures::executor::block_on;
use ontodev_valve::{guess::guess, valve::Valve};
use serde_json::{json, Value as SerdeValue};

// Help strings that are used in more than one subcommand:
static SOURCE_HELP: &str = "The location of a TSV file, representing the 'table' table, \
                            from which to read the Valve configuration.";

static DATABASE_HELP: &str = "Can be one of (A) A URL of the form `postgresql://...` \
                              or `sqlite://...` (B) The filename (including path) of \
                              a sqlite database.";

static SAVE_DIR_HELP: &str = "Save tables to DIR instead of to their configured paths";

static TABLE_HELP: &str = "The name of a table";

static ROW_HELP: &str = "A JSON representation of a row";

#[derive(Parser)]
#[command(version,
          about = "Valve: A lightweight validation engine -- command line interface",
          long_about = None)]
struct Cli {
    /// Use this option with caution. When set, Valve will not not ask the user for confirmation
    /// before executing potentially destructive operations on the database and/or table files.
    #[arg(long, action = ArgAction::SetTrue)]
    assume_yes: bool,

    /// Write more progress information to the terminal.
    #[arg(long, action = ArgAction::SetTrue)]
    verbose: bool,

    // Subcommands:
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Loads all of the tables in a given database.
    Load {
        #[arg(value_name = "SOURCE", action = ArgAction::Set, help = SOURCE_HELP)]
        source: String,

        #[arg(value_name = "DATABASE", action = ArgAction::Set, help = DATABASE_HELP)]
        database: String,

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

    /// Creates a database in a given location but does not load any of the tables.
    Create {
        #[arg(value_name = "SOURCE", action = ArgAction::Set, help = SOURCE_HELP)]
        source: String,

        #[arg(value_name = "DATABASE", action = ArgAction::Set, help = DATABASE_HELP)]
        database: String,
    },

    /// Drops all of the configured tables in the given database.
    DropAll {
        #[arg(value_name = "SOURCE", action = ArgAction::Set, help = SOURCE_HELP)]
        source: String,

        #[arg(value_name = "DATABASE", action = ArgAction::Set, help = DATABASE_HELP)]
        database: String,
    },

    /// Saves all configured data tables as TSV files.
    SaveAll {
        #[arg(value_name = "SOURCE", action = ArgAction::Set, help = SOURCE_HELP)]
        source: String,

        #[arg(value_name = "DATABASE", action = ArgAction::Set, help = DATABASE_HELP)]
        database: String,

        #[arg(long, value_name = "DIR", action = ArgAction::Set, help = SAVE_DIR_HELP)]
        save_dir: Option<String>,
    },

    /// Saves the configured data tables from the given list as TSV files.
    Save {
        #[arg(value_name = "SOURCE", action = ArgAction::Set, help = SOURCE_HELP)]
        source: String,

        #[arg(value_name = "DATABASE", action = ArgAction::Set, help = DATABASE_HELP)]
        database: String,

        #[arg(value_name = "LIST",
              action = ArgAction::Set,
              value_delimiter = ',',
              help = "A comma-separated list of tables to save. Note that table names with spaces \
                      must be enclosed within quotes.")]
        tables: Vec<String>,

        #[arg(long, value_name = "DIR", action = ArgAction::Set, help = SAVE_DIR_HELP)]
        save_dir: Option<String>,
    },

    /// Add a given row to a given table.
    Add {
        #[arg(value_name = "SOURCE", action = ArgAction::Set, help = SOURCE_HELP)]
        source: String,

        #[arg(value_name = "DATABASE", action = ArgAction::Set, help = DATABASE_HELP)]
        database: String,

        #[arg(value_name = "TABLE", action = ArgAction::Set, help = TABLE_HELP)]
        table: String,

        #[arg(value_name = "ROW", action = ArgAction::Set, help = ROW_HELP)]
        row: String,
    },

    /// Prints the Valve configuration as a JSON-formatted string to the terminal.
    DumpConfig {
        #[arg(value_name = "SOURCE", action = ArgAction::Set, help = SOURCE_HELP)]
        source: String,
    },

    /// Prints the order in which the configured tables will be created, as determined by their
    /// dependency relations, to the terminal.
    ShowTableOrder {
        #[arg(value_name = "SOURCE", action = ArgAction::Set, help = SOURCE_HELP)]
        source: String,
    },

    /// Prints the incoming dependencies for each configured table to the terminal.
    ShowIncomingDeps {
        #[arg(value_name = "SOURCE", action = ArgAction::Set, help = SOURCE_HELP)]
        source: String,
    },

    /// Prints the outgoing dependencies for each configured table to the terminal.
    ShowOutgoingDeps {
        #[arg(value_name = "SOURCE", action = ArgAction::Set, help = SOURCE_HELP)]
        source: String,
    },

    /// Prints the SQL that will be used to create the database tables to the terminal.
    DumpSchema {
        #[arg(value_name = "SOURCE", action = ArgAction::Set, help = SOURCE_HELP)]
        source: String,
    },

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

        #[arg(value_name = "SOURCE", action = ArgAction::Set, help = SOURCE_HELP)]
        source: String,

        #[arg(value_name = "DATABASE", action = ArgAction::Set, help = DATABASE_HELP)]
        database: String,

        #[arg(value_name = "TABLE_TSV", action = ArgAction::Set,
              help = "The TSV file representing the table whose column configuration will be \
                      guessed.")]
        table_tsv: String,
    },

    /// Runs a set of predefined tests, on a specified pre-loaded database, that will test Valve's
    /// Application Programmer Interface.
    TestApi {
        #[arg(value_name = "SOURCE", action = ArgAction::Set, help = SOURCE_HELP)]
        source: String,

        #[arg(value_name = "DATABASE", action = ArgAction::Set, help = DATABASE_HELP)]
        database: String,
    },

    /// Runs a set of predefined tests, on a specified pre-loaded database, that will test the
    /// validity of the configured datatype hierarchy.
    TestDtHierarchy {
        #[arg(value_name = "SOURCE", action = ArgAction::Set, help = SOURCE_HELP)]
        source: String,

        #[arg(value_name = "DATABASE", action = ArgAction::Set, help = DATABASE_HELP)]
        database: String,
    },
}

#[async_std::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // This has to be done multiple times so we declare a closure. We use a closure instead of a
    // function so that the cli.verbose and cli.assume_yes fields are in scope:
    let build_valve = |source: &str, database: &str| -> Result<Valve> {
        let mut valve = block_on(Valve::build(&source, &database)).unwrap();
        valve.set_verbose(cli.verbose);
        valve.set_interactive(!cli.assume_yes);
        Ok(valve)
    };

    // Although Valve::build() will accept a non-TSV argument (in which case that argument is
    // ignored and a table called 'table' is looked up in the given database instead), we do not
    // allow non-TSV arguments on the command line:
    fn exit_unless_tsv(source: &str) {
        if !source.to_lowercase().ends_with(".tsv") {
            println!("SOURCE must be a file ending (case-insensitively) with .tsv");
            std::process::exit(1);
        }
    }

    // Prints the table dependencies in either incoming or outgoing order.
    fn print_dependencies(valve: &Valve, incoming: bool) {
        let dependencies = valve.collect_dependencies(incoming).unwrap();
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

    match &cli.command {
        Commands::Add {
            source,
            database,
            row,
            table,
        } => {
            let row: SerdeValue = serde_json::from_str(row).expect(&format!("Invalid JSON: {row}"));
            let row = row
                .as_object()
                .expect(&format!("{row} is not a JSON object"));
            let valve = build_valve(source, database).expect("Unable to build Valve");
            let (_, row) = valve
                .insert_row(table, row)
                .await
                .expect("Error inserting row");
            if cli.verbose {
                println!("{}", json!(row.to_rich_json().unwrap()));
            }
        }
        Commands::Load {
            initial_load,
            source,
            database,
        } => {
            exit_unless_tsv(source);
            let mut valve = build_valve(source, database).unwrap();
            if *initial_load {
                block_on(valve.configure_for_initial_load()).unwrap();
            }
            valve.load_all_tables(true).await.unwrap();
        }
        Commands::Create { source, database } => {
            exit_unless_tsv(source);
            let valve = build_valve(source, database).unwrap();
            valve.create_all_tables().await.unwrap();
        }
        Commands::DropAll { source, database } => {
            exit_unless_tsv(source);
            let valve = build_valve(source, database).unwrap();
            valve.drop_all_tables().await.unwrap();
        }
        Commands::DumpConfig { source } => {
            exit_unless_tsv(source);
            let valve = build_valve(source, "").unwrap();
            println!("{}", valve.config);
        }
        Commands::ShowTableOrder { source } => {
            exit_unless_tsv(source);
            let valve = build_valve(source, "").unwrap();
            let sorted_table_list = valve.get_sorted_table_list(false);
            println!("{}", sorted_table_list.join(", "));
        }
        Commands::ShowIncomingDeps { source } => {
            exit_unless_tsv(source);
            let valve = build_valve(source, "").unwrap();
            print_dependencies(&valve, true);
        }
        Commands::ShowOutgoingDeps { source } => {
            exit_unless_tsv(source);
            let valve = build_valve(source, "").unwrap();
            print_dependencies(&valve, false);
        }
        Commands::DumpSchema { source } => {
            exit_unless_tsv(source);
            let valve = build_valve(source, "").unwrap();
            let schema = valve.dump_schema().await.unwrap();
            println!("{}", schema);
        }
        Commands::SaveAll {
            save_dir,
            source,
            database,
        } => {
            exit_unless_tsv(source);
            let valve = build_valve(source, database).unwrap();
            valve.save_all_tables(&save_dir).await.unwrap();
        }
        Commands::Save {
            save_dir,
            source,
            database,
            tables,
        } => {
            exit_unless_tsv(source);
            let valve = build_valve(source, database).unwrap();
            let tables = tables
                .iter()
                .filter(|s| *s != "")
                .map(|s| s.as_str())
                .collect::<Vec<_>>();
            valve.save_tables(&tables, &save_dir).await.unwrap();
        }
        Commands::Guess {
            sample_size,
            error_rate,
            seed,
            source,
            database,
            table_tsv,
        } => {
            exit_unless_tsv(source);
            let valve = build_valve(source, database).unwrap();
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
        Commands::TestApi { source, database } => {
            exit_unless_tsv(source);
            let valve = build_valve(source, database).unwrap();
            run_api_tests(&valve).await.unwrap();
        }
        Commands::TestDtHierarchy { source, database } => {
            exit_unless_tsv(source);
            let valve = build_valve(source, database).unwrap();
            run_dt_hierarchy_tests(&valve).unwrap();
        }
    }

    Ok(())
}
