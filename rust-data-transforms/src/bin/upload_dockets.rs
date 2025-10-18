use clap::Parser;
use rust_data_transforms::jurisdiction_schema_mapping::FixedJurisdiction;
use rust_data_transforms::sql_ingester_tasks::nypuc_ingest::ingest_sql_fixed_jurisdiction_case;
use rust_data_transforms::sql_ingester_tasks::dokito_sql_connection::get_dokito_pool;
use rust_data_transforms::cli_input_types::CliProcessedDockets;
use serde_json;
use std::io::{self, Read, Write};
use anyhow::Result;

#[derive(Parser)]
#[command(name = "upload-dockets")]
#[command(about = "Uploads processed generic dockets to PostgreSQL database")]
struct Cli {
    #[arg(long, value_enum, help = "Fixed jurisdiction to use for database upload")]
    fixed_jur: FixedJurisdiction,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;

    if input.trim().is_empty() {
        anyhow::bail!("No input provided on stdin");
    }

    let pool = get_dokito_pool().await?;

    let cli_processed_dockets: CliProcessedDockets = serde_json::from_str(&input)?;
    let mut processed_dockets: Vec<_> = cli_processed_dockets.into();

    for processed_docket in processed_dockets.iter_mut() {
        ingest_sql_fixed_jurisdiction_case(processed_docket, cli.fixed_jur, &pool, false).await?;
    }

    let result = if processed_dockets.len() == 1 {
        serde_json::to_string(&processed_dockets[0])?
    } else {
        serde_json::to_string(&processed_dockets)?
    };
    io::stdout().write_all(result.as_bytes())?;

    io::stdout().flush()?;

    Ok(())
}