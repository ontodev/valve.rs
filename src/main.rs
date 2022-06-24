use std::{env, process};
use valve::configure_and_or_load;

#[async_std::main]
async fn main() -> Result<(), sqlx::Error> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: valve table db_dir");
        process::exit(1);
    }
    let table = &args[1];
    let db_dir = &args[2];
    configure_and_or_load(table, db_dir, true).await?;
    Ok(())
}
