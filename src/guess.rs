//! Implementation of the column configuration guesser

use fix_fn::fix_fn;
use indexmap::IndexMap;
use ontodev_valve::valve::{Valve, ValveConfig, ValveDatatypeConfig};
use rand::{rngs::StdRng, Rng, SeedableRng};
use regex::Regex;
use std::collections::HashMap;

/// TODO: Add a docstring here.
// TODO: We probably don't need Debug here.
#[derive(Clone, Debug, Default)]
pub struct Sample {
    pub normalized: String,
    pub nulltype: String,
    pub datatype: String,
    pub structure: String,
    pub values: Vec<String>,
}

/// TODO: Add a docstring here.
#[derive(Clone, Debug, Default)]
pub struct Match {
    datatype: String,
    success_rate: f32,
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

    if verbose {
        println!(
            "Getting random sample of {} rows from {} ...",
            sample_size, table_tsv
        );
    }
    let mut samples = get_random_samples(table_tsv, *sample_size, &mut rng);
    //println!("RANDOM SAMPLES: {:#?}", samples);
    for (i, (label, sample)) in samples.iter_mut().enumerate() {
        if verbose {
            println!("Annotating label '{}' ...", label);
        }
        annotate(label, sample, &valve, error_rate, i == 0);
        // TODO: The rest ...
    }
    if verbose {
        println!("Done!");
    }
}

/// TODO: Add docstring here.
pub fn get_hierarchy_for_dt(
    config: &ValveConfig,
    primary_dt_name: &str,
) -> Vec<ValveDatatypeConfig> {
    let get_parents = fix_fn!(|get_parents, dt_name: &str| -> Vec<ValveDatatypeConfig> {
        let mut datatypes = vec![];
        if dt_name != "" {
            let datatype = config
                .datatype
                .get(dt_name)
                .expect(&format!("'{}' not found in datatype config", dt_name));
            if datatype.datatype != primary_dt_name {
                datatypes.push(datatype.clone())
            }
            datatypes.append(&mut get_parents(&datatype.parent));
        }
        datatypes
    });

    let mut hierarchy_for_dt = vec![config.datatype.get(primary_dt_name).unwrap().clone()];
    hierarchy_for_dt.append(&mut get_parents(primary_dt_name));
    hierarchy_for_dt
}

/// TODO: Add docstring here.
pub fn get_dt_hierarchies(
    config: &ValveConfig,
) -> HashMap<usize, HashMap<String, Vec<ValveDatatypeConfig>>> {
    // Returns datatype hierarchies, from the given collections of datatype hierachies and
    // universal datatypes, that are deeper in the hierarchy than `depth`.
    fn get_higher_datatypes(
        dt_hierarchies: &HashMap<usize, HashMap<String, Vec<ValveDatatypeConfig>>>,
        universals: &HashMap<String, Vec<ValveDatatypeConfig>>,
        depth: usize,
    ) -> HashMap<String, Vec<ValveDatatypeConfig>> {
        let current_datatypes = dt_hierarchies
            .get(&depth)
            .and_then(|d| {
                Some(
                    d.keys()
                        //.cloned()
                        .collect::<Vec<_>>(),
                )
            })
            .unwrap_or(vec![]);
        let mut higher_datatypes = HashMap::new();
        if !current_datatypes.is_empty() {
            let universals = universals.keys().collect::<Vec<_>>();
            let lower_datatypes = {
                let mut lower_datatypes = vec![];
                for i in 0..depth {
                    lower_datatypes.append(
                        &mut dt_hierarchies
                            .get(&i)
                            .and_then(|d| {
                                Some(
                                    d.keys()
                                        //.cloned()
                                        .collect::<Vec<_>>(),
                                )
                            })
                            .unwrap_or(vec![]),
                    );
                }
                lower_datatypes
            };
            for (dt_name, dt_hierarchy) in dt_hierarchies
                .get(&depth)
                .expect(&format!("No datatype hierarchies at depth: {}", depth,))
                .iter()
            {
                if dt_hierarchy.len() > 1 {
                    let parent_hierarchy = &dt_hierarchy[1..];
                    let parent = &parent_hierarchy[0].datatype;
                    if !current_datatypes.contains(&&parent)
                        && !lower_datatypes.contains(&&parent)
                        && !universals.contains(&&parent)
                    {
                        higher_datatypes.insert(parent.to_string(), parent_hierarchy.to_vec());
                    }
                }
            }
        }
        higher_datatypes
    }

    let dt_names = config.datatype.keys().collect::<Vec<_>>();
    let mut dt_hierarchies = HashMap::from([(0, HashMap::new())]);
    let mut universals = HashMap::new();
    for dt_name in &dt_names {
        // Check if this datatype has any children:
        if dt_names
            .iter()
            .filter(|child| &config.datatype.get(**child).unwrap().parent == *dt_name)
            .collect::<Vec<_>>()
            .is_empty()
        {
            // Add all leaf datatypes (i.e., those without children) to dt_hierarchies at 0 depth:
            dt_hierarchies
                .get_mut(&0)
                .unwrap()
                .insert(dt_name.to_string(), get_hierarchy_for_dt(config, dt_name));
        } else if config
            .datatype
            .get(*dt_name)
            .expect(&format!("'{}' not found in datatype config", dt_name))
            .parent
            == ""
            || config.datatype.get(*dt_name).unwrap().condition == ""
        {
            // Ungrounded and unconditioned datatypes go into the universals category, which are
            // added to the top of dt_hierarchies later:
            universals.insert(dt_name.to_string(), get_hierarchy_for_dt(config, dt_name));
        }
    }
    let mut depth = 0;
    let mut higher_dts = get_higher_datatypes(&dt_hierarchies, &universals, depth);
    while !higher_dts.is_empty() {
        depth += 1;
        dt_hierarchies.insert(depth, higher_dts.clone());
        higher_dts = get_higher_datatypes(&dt_hierarchies, &universals, depth);
    }
    dt_hierarchies.insert(depth + 1, universals);
    dt_hierarchies
}

/// TODO: Add a docstring here.
pub fn annotate(
    label: &str,
    sample: &mut Sample,
    valve: &Valve,
    error_rate: &f32,
    is_primary_candidate: bool,
) {
    // TODO: Add a comment here.
    fn get_datatype(
        valve: &Valve,
        sample: &Sample,
        dt_hierarchies: &HashMap<usize, HashMap<String, Vec<ValveDatatypeConfig>>>,
        error_rate: &f32,
    ) -> String {
        let is_match = |datatype: &ValveDatatypeConfig| -> (bool, f32) {
            // If the datatype has no associated condition then it matches anything:
            if datatype.condition == "" {
                return (true, 1 as f32);
            }
            // If the SQL type is NULL this datatype is ruled out:
            if datatype.sql_type.to_lowercase() == "null" {
                return (false, 0 as f32);
            }

            // Otherwise we test the datatype condition against all of the values in the sample:
            let condition = &valve
                .datatype_conditions
                .get(&datatype.datatype)
                .expect(&format!(
                    "Condition '{}' not found in datatype conditions",
                    datatype.condition
                ))
                .compiled;
            let num_values = sample.values.len();
            let num_passed = sample.values.iter().filter(|v| condition(v)).count();
            let success_rate = num_passed as f32 / num_values as f32;
            return ((1 as f32 - success_rate) <= *error_rate, success_rate);
        };

        let tiebreak = |matches: &Vec<Match>| -> String {
            // TODO: Implement this properly later.
            return matches[0].datatype.to_string();
        };

        for depth in 0..dt_hierarchies.len() {
            let datatypes_to_check = dt_hierarchies
                .get(&depth)
                .unwrap()
                .iter()
                .map(|(dt_name, dt)| dt[0].clone())
                .collect::<Vec<_>>();
            let mut matching_datatypes = vec![];
            for datatype in &datatypes_to_check {
                let (success, success_rate) = is_match(datatype);
                if success {
                    matching_datatypes.push(Match {
                        datatype: datatype.datatype.to_string(),
                        success_rate: success_rate,
                    });
                }
            }

            println!("Matching datatypes: {:?}", matching_datatypes);

            if matching_datatypes.len() == 1 {
                return matching_datatypes[0].datatype.to_string();
            } else if matching_datatypes.len() > 1 {
                return tiebreak(&matching_datatypes);
            }
        }

        // Return an empty string if nothing is found:
        return String::new();
    }

    let sample_has_nulltype = {
        let num_values = sample.values.len();
        let num_empties = sample.values.iter().filter(|v| *v == "").count();
        let pct_empty = num_empties as f32 / num_values as f32;
        pct_empty > *error_rate
    };

    // Use the data sample to guess whether the given column should allow empty values:
    if sample_has_nulltype {
        sample.nulltype = "empty".to_string();
    }

    // Use the valve config to retrieve the valve datatype hierarchies:
    let dt_hierarchies = get_dt_hierarchies(&valve.config);
    sample.datatype = get_datatype(valve, &sample, &dt_hierarchies, error_rate);
    println!("SAMPLE DT: {}", sample.datatype);

    // TODO: The rest ...
}

/// TODO: Add a docstring here.
pub fn get_random_samples(
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
        // calling next()) when the headers are read.
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
                        ..Default::default()
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
