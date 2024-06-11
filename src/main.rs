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
    // Required global positional parameters:
    source: String,

    // Global flags:
    #[arg(long, action = clap::ArgAction::SetTrue)]
    interactive: bool,
    #[arg(long, action = clap::ArgAction::SetTrue)]
    verbose: bool,

    // Subcommands:
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Read the configuration referred to by SOURCE and load the specified database.
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

    /// Read the configuration referred to by SOURCE and create a corresponding database in
    /// a specified location but do not load any of the tables.
    Create {
        #[arg(value_name = "DESTINATION", action = clap::ArgAction::Set, help = DESTINATION_HELP)]
        destination: String,
    },

    /// Read the configuration referred to by SOURCE and drop all of the configured tables in the
    /// given database.
    DropAll {
        #[arg(value_name = "DESTINATION", action = clap::ArgAction::Set, help = DESTINATION_HELP)]
        destination: String,
    },

    /// Read the configuration referred to by SOURCE and save all configured data tables as
    /// TSV files to (by default) their configured paths, or optionally to a specified alternate
    /// directory.
    SaveAll {
        #[arg(value_name = "DESTINATION", action = clap::ArgAction::Set, help = DESTINATION_HELP)]
        destination: String,

        #[arg(long, value_name = "DIR", action = clap::ArgAction::Set, help = SAVE_DIR_HELP)]
        save_dir: Option<String>,
    },

    /// Read the configuration referred to by SOURCE and save the configured data tables from the
    /// given list as TSV files to (by default) their configured paths, or optionally to a specified
    /// alternate directory.
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

    /// Read the configuration referred to by SOURCE and print it as a JSON-formatted string.
    DumpConfig {},

    /// Read the configuration referred to by SOURCE and print the order in which the configured
    /// tables will be created, as determined by their dependency relations.
    ShowTableOrder {},

    /// Read the configuration referred to by SOURCE and print the incoming dependencies for each
    /// configured table.
    ShowIncomingDeps {},

    /// Read the configuration referred to by SOURCE and print the outgoing dependencies for each
    /// configured table.
    ShowOutgoingDeps {},

    /// Read the configuration referred to by SOURCE and print the SQL that will be used to create
    /// the database tables to stdout.
    DumpSchema {},

    /// TODO: Add a doc string here.
    Guess {
        #[arg(value_name = "DESTINATION", action = clap::ArgAction::Set, help = DESTINATION_HELP)]
        destination: String,

        #[arg(value_name = "TSV", action = clap::ArgAction::Set)]
        guess_file: String,
    },

    /// Read the configuration referred to by SOURCE and run a set of predefined tests, on
    /// a specified pre-loaded database, that will test Valve's Application Programmer Interface.
    TestApi {
        #[arg(value_name = "DESTINATION", action = clap::ArgAction::Set, help = DESTINATION_HELP)]
        destination: String,
    },

    /// Read the configuration referred to by SOURCE and run a set of predefined tests, on
    /// a specified pre-loaded database, that will test the validity of the configured
    /// datatype hierarchy.
    TestDtHierarchy {
        #[arg(value_name = "DESTINATION", action = clap::ArgAction::Set, help = DESTINATION_HELP)]
        destination: String,
    },
}

#[async_std::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // TODO: Instead of a closure, just build it since it is required for every option, although
    // you will need to change the signature for run_api_tests() first.
    let build_valve = |source: &str, destination: &str| -> Result<Valve> {
        let mut valve = block_on(Valve::build(&source, &destination)).unwrap();
        valve.set_verbose(cli.verbose);
        valve.set_interactive(cli.interactive);
        Ok(valve)
    };

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
            // TODO: Refactor this since the code is shared in common with ShowOutgoingDeps:
            let dependencies = valve.collect_dependencies(true).unwrap();
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
                    if true {
                        format!("Tables that depend on '{}'", table)
                    } else {
                        format!("Table '{}' depends on", table)
                    }
                };
                println!("{}: {}", preamble, deps);
            }
        }
        Commands::ShowOutgoingDeps {} => {
            let valve = build_valve(&cli.source, "").unwrap();
            // TODO: Refactor this since the code is shared in common with ShowOutgoingDeps:
            let dependencies = valve.collect_dependencies(false).unwrap();
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
                    if false {
                        format!("Tables that depend on '{}'", table)
                    } else {
                        format!("Table '{}' depends on", table)
                    }
                };
                println!("{}: {}", preamble, deps);
            }
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
            run_api_tests(&cli.source, &destination).await.unwrap();
        }
        Commands::TestDtHierarchy { destination } => {
            let valve = build_valve(&cli.source, destination).unwrap();
            valve.create_all_tables().await.unwrap();
            run_dt_hierarchy_tests(&valve).unwrap();
        }
    }

    Ok(())
}
