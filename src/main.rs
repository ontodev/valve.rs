mod tests;

use crate::tests::{run_api_tests, run_dt_hierarchy_tests};
use anyhow::Result;
use clap::{ArgAction, Parser, Subcommand};
use futures::executor::block_on;
use indexmap::IndexMap;
use ontodev_valve::valve::Valve;
use rand::{random, rngs::StdRng, Rng, SeedableRng};
use regex::Regex;

// Help strings that are used in more than one subcommand:
static SOURCE_HELP: &str = "The location of a TSV file, representing the 'table' table, \
                            from which to read the Valve configuration.";

static DESTINATION_HELP: &str = "Can be one of (A) A URL of the form `postgresql://...` \
                                 or `sqlite://...` (B) The filename (including path) of \
                                 a sqlite database.";

static SAVE_DIR_HELP: &str = "Save tables to DIR instead of to their configured paths";

#[derive(Parser)]
#[command(version,
          about = "Valve: A lightweight validation engine -- command line interface",
          long_about = None)]
struct Cli {
    /// Prompt the user before automatically making changes to the database that may be
    /// required to satisfy table dependencies.
    #[arg(long, action = ArgAction::SetTrue)]
    interactive: bool,

    /// Write more progress information to the terminal.
    #[arg(long, action = ArgAction::SetTrue)]
    verbose: bool,

    // Subcommands:
    #[command(subcommand)]
    command: Commands,
}

// TODO: Move this struct to guess.rs
#[derive(Debug)]
struct Sample {
    normalized: String,
    values: Vec<String>,
}

#[derive(Subcommand)]
enum Commands {
    /// Loads a given database.
    Load {
        #[arg(value_name = "SOURCE", action = ArgAction::Set, help = SOURCE_HELP)]
        source: String,

        #[arg(value_name = "DESTINATION", action = ArgAction::Set, help = DESTINATION_HELP)]
        destination: String,

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

        #[arg(value_name = "DESTINATION", action = ArgAction::Set, help = DESTINATION_HELP)]
        destination: String,
    },

    /// Drops all of the configured tables in the given database.
    DropAll {
        #[arg(value_name = "SOURCE", action = ArgAction::Set, help = SOURCE_HELP)]
        source: String,

        #[arg(value_name = "DESTINATION", action = ArgAction::Set, help = DESTINATION_HELP)]
        destination: String,
    },

    /// Saves all configured data tables as TSV files.
    SaveAll {
        #[arg(value_name = "SOURCE", action = ArgAction::Set, help = SOURCE_HELP)]
        source: String,

        #[arg(value_name = "DESTINATION", action = ArgAction::Set, help = DESTINATION_HELP)]
        destination: String,

        #[arg(long, value_name = "DIR", action = ArgAction::Set, help = SAVE_DIR_HELP)]
        save_dir: Option<String>,
    },

    /// Saves the configured data tables from the given list as TSV files.
    Save {
        #[arg(value_name = "SOURCE", action = ArgAction::Set, help = SOURCE_HELP)]
        source: String,

        #[arg(value_name = "DESTINATION", action = ArgAction::Set, help = DESTINATION_HELP)]
        destination: String,

        #[arg(value_name = "LIST",
              action = ArgAction::Set,
              value_delimiter = ',',
              help = "A comma-separated list of tables to save. Note that table names with spaces \
                      must be enclosed within quotes.")]
        tables: Vec<String>,

        #[arg(long, value_name = "DIR", action = ArgAction::Set, help = SAVE_DIR_HELP)]
        save_dir: Option<String>,
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

        #[arg(long, value_name = "SIZE", action = ArgAction::Set,
              help = "The maximum number of values to use for in(...) datatype conditions",
              default_value_t = 10)]
        enum_size: usize,

        #[arg(long, value_name = "SEED", action = ArgAction::Set,
              help = "Use SEED for random sampling")]
        seed: Option<u64>,

        #[arg(long, action = ArgAction::SetTrue,
              help = "Do not ask for confirmation before writing suggested modifications to the \
                      database")]
        yes: bool,

        #[arg(value_name = "SOURCE", action = ArgAction::Set, help = SOURCE_HELP)]
        source: String,

        #[arg(value_name = "DESTINATION", action = ArgAction::Set, help = DESTINATION_HELP)]
        destination: String,

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

        #[arg(value_name = "DESTINATION", action = ArgAction::Set, help = DESTINATION_HELP)]
        destination: String,
    },

    /// Runs a set of predefined tests, on a specified pre-loaded database, that will test the
    /// validity of the configured datatype hierarchy.
    TestDtHierarchy {
        #[arg(value_name = "SOURCE", action = ArgAction::Set, help = SOURCE_HELP)]
        source: String,

        #[arg(value_name = "DESTINATION", action = ArgAction::Set, help = DESTINATION_HELP)]
        destination: String,
    },
}

#[async_std::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // This has to be done multiple times so we declare a closure. We use a closure instead of a
    // function so that the cli.verbose and cli.interactive fields are in scope:
    let build_valve = |source: &str, destination: &str| -> Result<Valve> {
        let mut valve = block_on(Valve::build(&source, &destination)).unwrap();
        valve.set_verbose(cli.verbose);
        valve.set_interactive(cli.interactive);
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

    // TODO: Move this to its own file, guess.rs (but do it later).
    // TODO: Add a comment here:
    fn get_random_sample(
        table_tsv: &str,
        sample_size: usize,
        rng: &mut StdRng,
    ) -> IndexMap<String, Sample> {
        // Count the number of rows in the file and then generate a random sample of row numbers:
        let sample_row_numbers = {
            let total_rows = std::fs::read_to_string(table_tsv)
                .expect(&format!("Error reading from {}", table_tsv))
                .lines()
                .count();

            // If the total number of rows in the file is smaller than sample_size then sample
            // everything, otherwise take a random sample of row_numbers from the file. The reason
            // that the range runs from 0 to (total_rows - 1) is that total_rows includes the
            // header row, which is going to be removed in the first step below (as a result of
            // calling next()).
            if total_rows <= sample_size {
                (0..total_rows - 1).collect::<Vec<_>>()
            } else {
                let mut samples = rng
                    .sample_iter(rand::distributions::Uniform::new(0, total_rows - 1))
                    .take(sample_size)
                    .collect::<Vec<_>>();
                // We call sort here since, when we collect the actual sample rows, we will be
                // using an iterator over the rows which we will need to consume in an ordered way.
                samples.sort();
                samples
            }
        };

        // Create a CSV reader:
        let mut rdr = match std::fs::File::open(table_tsv) {
            Err(e) => panic!("Unable to open '{}': {}", table_tsv, e),
            Ok(table_file) => csv::ReaderBuilder::new()
                .has_headers(false)
                .delimiter(b'\t')
                .from_reader(table_file),
        };

        // Use the CSV reader and the sample row numbers collected above to construct the random
        // sample of rows from the file:
        let mut records = rdr.records();
        let headers = records
            .next()
            .expect("Header row not found.")
            .expect("Error while reading header row")
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        // We use this pattern to normalize the labels represented by the column headers:
        let pattern = Regex::new(r#"[^0-9a-zA-Z_]+"#).expect("Invalid regex pattern");
        let mut samples: IndexMap<String, Sample> = IndexMap::new();
        let mut prev_rn = None;
        for rn in &sample_row_numbers {
            let nth_record = {
                let n = match prev_rn {
                    None => *rn,
                    Some(prev_rn) => *rn - prev_rn - 1,
                };
                records
                    .nth(n)
                    .expect(&format!("No record found at position {}", n))
                    .expect(&format!("Error while reading record at position {}", n))
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
            };
            for (i, label) in headers.iter().enumerate() {
                // If samples doesn't already contain an entry for this label, add one:
                if !samples.contains_key(label) {
                    let ncolumn = {
                        let mut ncolumn = pattern.replace_all(label, "_").to_lowercase();
                        if ncolumn.starts_with("_") {
                            ncolumn = ncolumn.strip_prefix("_").unwrap().to_string();
                        }
                        if ncolumn.ends_with("_") {
                            ncolumn = ncolumn.strip_suffix("_").unwrap().to_string();
                        }
                        for (_label, sample) in samples.iter() {
                            if sample.normalized == ncolumn {
                                println!(
                                    "The data has more than one column with the normalized name {}",
                                    ncolumn
                                );
                                std::process::exit(1);
                            }
                        }
                        ncolumn
                    };
                    samples.insert(
                        label.to_string(),
                        Sample {
                            normalized: ncolumn,
                            values: vec![],
                        },
                    );
                }
                // Add the ith entry of the nth_record to the sample under the entry for label:
                samples
                    .get_mut(label)
                    .expect(&format!("Could not find label '{}' in sample", label))
                    .values
                    .push(nth_record[i].to_string());
            }
            prev_rn = Some(*rn);
        }
        samples
    }

    match &cli.command {
        Commands::Load {
            initial_load,
            source,
            destination,
        } => {
            exit_unless_tsv(source);
            let mut valve = build_valve(source, destination).unwrap();
            if *initial_load {
                block_on(valve.configure_for_initial_load()).unwrap();
            }
            valve.load_all_tables(true).await.unwrap();
        }
        Commands::Create {
            source,
            destination,
        } => {
            exit_unless_tsv(source);
            let valve = build_valve(source, destination).unwrap();
            valve.create_all_tables().await.unwrap();
        }
        Commands::DropAll {
            source,
            destination,
        } => {
            exit_unless_tsv(source);
            let valve = build_valve(source, destination).unwrap();
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
            destination,
        } => {
            exit_unless_tsv(source);
            let valve = build_valve(source, destination).unwrap();
            valve.save_all_tables(&save_dir).unwrap();
        }
        Commands::Save {
            save_dir,
            source,
            destination,
            tables,
        } => {
            exit_unless_tsv(source);
            let valve = build_valve(source, destination).unwrap();
            let tables = tables
                .iter()
                .filter(|s| *s != "")
                .map(|s| s.as_str())
                .collect::<Vec<_>>();
            valve.save_tables(&tables, &save_dir).unwrap();
        }
        Commands::Guess {
            sample_size,
            error_rate,
            enum_size,
            seed,
            yes,
            source,
            destination,
            table_tsv,
        } => {
            // Build a Valve instance:
            exit_unless_tsv(source);
            let valve = build_valve(source, destination).unwrap();

            // If a seed was provided, use it to create the random number generator instead of
            // creating it using fresh entropy:
            let mut rng = match seed {
                None => StdRng::from_entropy(),
                Some(seed) => StdRng::seed_from_u64(*seed),
            };

            // Use the name of the TSV file to determine the table name:
            let table = std::path::Path::new(table_tsv)
                .file_stem()
                .expect(&format!(
                    "Error geting file stem for {}: Not a filename",
                    table_tsv
                ))
                .to_str()
                .expect(&format!(
                    "Error getting file stem for {}: Not a valid UTF-8 string",
                    table_tsv
                ));

            // TODO: Create a parser? Reuse the valve_grammar?

            log::info!(
                "Getting random sample of {} rows from {} ...",
                sample_size,
                table_tsv
            );
            let sample = get_random_sample(table_tsv, *sample_size, &mut rng);
            println!("RANDOM SAMPLE: {:#?}", sample);
            // YOU ARE HERE.
        }
        Commands::TestApi {
            source,
            destination,
        } => {
            exit_unless_tsv(source);
            let valve = build_valve(source, destination).unwrap();
            run_api_tests(&valve).await.unwrap();
        }
        Commands::TestDtHierarchy {
            source,
            destination,
        } => {
            exit_unless_tsv(source);
            let valve = build_valve(source, destination).unwrap();
            run_dt_hierarchy_tests(&valve).unwrap();
        }
    }

    Ok(())
}
