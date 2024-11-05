use anyhow::Result;
use ontodev_valve::cli;

#[async_std::main]
async fn main() -> Result<()> {
    cli::process_command().await
}
