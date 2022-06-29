use ontodev_valve::configure_and_or_load;
use std::{env, process};

#[async_std::main]
async fn main() -> Result<(), sqlx::Error> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: valve table db_path");
        process::exit(1);
    }
    let table = &args[1];
    let db_path = &args[2];
    configure_and_or_load(table, db_path, true).await?;
    Ok(())
}
