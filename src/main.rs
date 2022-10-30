mod api_test;

use crate::api_test::run_api_tests;

use ontodev_valve::configure_and_or_load;
use std::{env, process};

#[async_std::main]
async fn main() -> Result<(), sqlx::Error> {
    let args: Vec<String> = env::args().collect();
    let table;
    let database;
    if args.len() == 3 {
        table = &args[1];
        database = &args[2];
        configure_and_or_load(table, database, true).await?;
    } else if args.len() == 4 && &args[1] == "--api_test" {
        table = &args[2];
        database = &args[3];
        run_api_tests(table, database).await?;
    } else {
        eprintln!(
            r#"
Usage: valve [--api_test] TABLE DATABASE
Where:
  TABLE: The filename (including path) of the table table file"
  DATABASE: Can be one of:
    - A URL of the form `postgresql://...` or `sqlite://...`
    - The filename (including path) of a sqlite database."#
        );
        process::exit(1);
    }
    Ok(())
}
