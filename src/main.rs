mod tests;

use crate::tests::{run_api_tests, run_dt_hierarchy_tests};
use anyhow::Result;
use clap::{Parser, Subcommand};
use futures::executor::block_on;
use ontodev_valve::valve::Valve;

// Help strings that are used in more than one subcommand:
static DESTINATION_HELP: &str = "Can be one of (A) A URL of the form `postgresql://...` \
                                 or `sqlite://...` (B) The filename (including path) of \
                                 a sqlite database.";
static SAVE_DIR_HELP: &str = "Save tables to DIR instead of to their configured paths";

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(action = clap::ArgAction::Set,
          help = "The location of a TSV file, representing the 'table' table, from which to read \
                  the Valve configuration")]
    source: String,

    /// Prompt the user before automatically making any changes to the database in order to satisfy
    /// table dependencies.
    #[arg(long, action = clap::ArgAction::SetTrue)]
    interactive: bool,

    /// Write more progress information to the terminal.
    #[arg(long, action = clap::ArgAction::SetTrue)]
    verbose: bool,

    // Subcommands:
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Loads a given database.
    Load {
        #[arg(value_name = "DESTINATION", action = clap::ArgAction::Set, help = DESTINATION_HELP)]
        destination: String,

        #[arg(long,
              action = clap::ArgAction::SetTrue,
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
        #[arg(value_name = "DESTINATION", action = clap::ArgAction::Set, help = DESTINATION_HELP)]
        destination: String,
    },

    /// Drops all of the configured tables in the given database.
    DropAll {
        #[arg(value_name = "DESTINATION", action = clap::ArgAction::Set, help = DESTINATION_HELP)]
        destination: String,
    },

    /// Saves all configured data tables as TSV files to (by default) their configured paths, or
    /// optionally to a given alternate directory.
    SaveAll {
        #[arg(value_name = "DESTINATION", action = clap::ArgAction::Set, help = DESTINATION_HELP)]
        destination: String,

        #[arg(long, value_name = "DIR", action = clap::ArgAction::Set, help = SAVE_DIR_HELP)]
        save_dir: Option<String>,
    },

    /// Saves the configured data tables from the given list as TSV files to (by default) their
    /// configured paths, or optionally to a given alternate directory.
    Save {
        #[arg(value_name = "DESTINATION", action = clap::ArgAction::Set, help = DESTINATION_HELP)]
        destination: String,

        #[arg(value_name = "LIST",
              action = clap::ArgAction::Set,
              value_delimiter = ',',
              help = "A comma-separated list of tables to save. Note that table names with spaces \
                      must be enclosed within quotes.")]
        tables: Vec<String>,

        #[arg(long, value_name = "DIR", action = clap::ArgAction::Set, help = SAVE_DIR_HELP)]
        save_dir: Option<String>,
    },

    /// Prints the Valve configuration as a JSON-formatted string to the terminal.
    DumpConfig {},

    /// Prints the order in which the configured tables will be created, as determined by their
    /// dependency relations, to the terminal.
    ShowTableOrder {},

    /// Prints the incoming dependencies for each configured table to the terminal.
    ShowIncomingDeps {},

    /// Prints the outgoing dependencies for each configured table to the terminal.
    ShowOutgoingDeps {},

    /// Prints the SQL that will be used to create the database tables to the terminal.
    DumpSchema {},

    /// TODO: Add a doc string here.
    Guess {
        #[arg(value_name = "DESTINATION", action = clap::ArgAction::Set, help = DESTINATION_HELP)]
        destination: String,

        #[arg(value_name = "TSV", action = clap::ArgAction::Set, help = "Foo foo foo")]
        guess_file: String,
    },

    /// Runs a set of predefined tests, on a specified pre-loaded database, that will test Valve's
    /// Application Programmer Interface.
    TestApi {
        #[arg(value_name = "DESTINATION", action = clap::ArgAction::Set, help = DESTINATION_HELP)]
        destination: String,
    },

    /// Runs a set of predefined tests, on a specified pre-loaded database, that will test the
    /// validity of the configured datatype hierarchy.
    TestDtHierarchy {
        #[arg(value_name = "DESTINATION", action = clap::ArgAction::Set, help = DESTINATION_HELP)]
        destination: String,
    },
}

#[async_std::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Although Valve::build() will accept a non-TSV argument (in which case it is ignored and
    // a table called 'table' is looked up in the database instead), we do not allow this on the
    // command line:
    if !cli.source.to_lowercase().ends_with(".tsv") {
        println!("SOURCE must be a file ending (case-insensitively) with .tsv");
        std::process::exit(1);
    }

    // This has to be done multiple times so we declare a closure:
    let build_valve = |source: &str, destination: &str| -> Result<Valve> {
        let mut valve = block_on(Valve::build(&source, &destination)).unwrap();
        valve.set_verbose(cli.verbose);
        valve.set_interactive(cli.interactive);
        Ok(valve)
    };

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
        Commands::Load {
            initial_load,
            destination,
        } => {
            let mut valve = build_valve(&cli.source, destination).unwrap();
            if *initial_load {
                block_on(valve.configure_for_initial_load()).unwrap();
            }
            valve.load_all_tables(true).await.unwrap();
        }
        Commands::Create { destination } => {
            let valve = build_valve(&cli.source, destination).unwrap();
            valve.create_all_tables().await.unwrap();
        }
        Commands::DropAll { destination } => {
            let valve = build_valve(&cli.source, destination).unwrap();
            valve.drop_all_tables().await.unwrap();
        }
        Commands::DumpConfig {} => {
            let valve = build_valve(&cli.source, "").unwrap();
            println!("{}", valve.config);
        }
        Commands::ShowTableOrder {} => {
            let valve = build_valve(&cli.source, "").unwrap();
            let sorted_table_list = valve.get_sorted_table_list(false);
            println!("{}", sorted_table_list.join(", "));
        }
        Commands::ShowIncomingDeps {} => {
            let valve = build_valve(&cli.source, "").unwrap();
            print_dependencies(&valve, true);
        }
        Commands::ShowOutgoingDeps {} => {
            let valve = build_valve(&cli.source, "").unwrap();
            print_dependencies(&valve, false);
        }
        Commands::DumpSchema {} => {
            let valve = build_valve(&cli.source, "").unwrap();
            let schema = valve.dump_schema().await.unwrap();
            println!("{}", schema);
        }
        Commands::SaveAll {
            save_dir,
            destination,
        } => {
            let valve = build_valve(&cli.source, destination).unwrap();
            valve.save_all_tables(&save_dir).unwrap();
        }
        Commands::Save {
            save_dir,
            destination,
            tables,
        } => {
            let valve = build_valve(&cli.source, destination).unwrap();
            let tables = tables
                .iter()
                .filter(|s| *s != "")
                .map(|s| s.as_str())
                .collect::<Vec<_>>();
            valve.save_tables(&tables, &save_dir).unwrap();
        }
        Commands::Guess {
            destination,
            guess_file,
        } => {
            println!("DEST: {}, GUESS TABLE: {}", destination, guess_file);
        }
        Commands::TestApi { destination } => {
            let valve = build_valve(&cli.source, destination).unwrap();
            run_api_tests(&valve).await.unwrap();
        }
        Commands::TestDtHierarchy { destination } => {
            let valve = build_valve(&cli.source, destination).unwrap();
            run_dt_hierarchy_tests(&valve).unwrap();
        }
    }

    Ok(())
}
