//! Implementation of the column configuration guesser

use indexmap::IndexMap;
use ontodev_valve::valve::{Valve, ValveConfig};
use rand::{rngs::StdRng, Rng, SeedableRng};
use regex::Regex;

/// TODO: Add a docstring here.
#[derive(Debug)]
pub struct Sample {
    pub normalized: String,
    pub values: Vec<String>,
}

/// TODO: Add a docstring here.
pub fn guess(
    valve: &Valve,
    verbose: bool,
    table_tsv: &str,
    seed: &Option<u64>,
    sample_size: &usize,
    error_rate: &f32,
) {
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

    if verbose {
        println!(
            "Getting random sample of {} rows from {} ...",
            sample_size, table_tsv
        );
    }
    let sample = get_random_sample(table_tsv, *sample_size, &mut rng);
    println!("RANDOM SAMPLE: {:#?}", sample);
    for (i, (label, _values)) in sample.iter().enumerate() {
        if verbose {
            println!("Annotating label '{}' ...", label);
        }
        annotate(label, &sample, &valve.config, error_rate, i == 0);
        // TODO: The rest ...
    }
    if verbose {
        println!("Done!");
    }
}

/// TODO: Add a docstring here.
pub fn annotate(
    label: &str,
    sample: &IndexMap<String, Sample>,
    config: &ValveConfig,
    error_rate: &f32,
    is_primary_candidate: bool,
) {
    // YOU ARE HERE.
}

/// TODO: Add a docstring here.
pub fn get_random_sample(
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
                            eprintln!(
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
