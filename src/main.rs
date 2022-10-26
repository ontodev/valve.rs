use ontodev_valve::configure_and_or_load;
use std::{env, process};

#[async_std::main]
async fn main() -> Result<(), sqlx::Error> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!(
            r#"
Usage: valve TABLE DATABASE
Where:
  TABLE: The filename (including path) of the table table file"
  DATABASE: Can be one of:
    - A URL of the form `postgresql://...` or `sqlite://...`
    - The filename (including path) of a sqlite database."#);
        process::exit(1);
    }
    let table = &args[1];
    let database = &args[2];
    configure_and_or_load(table, database, true).await?;
    Ok(())
}
