use anyhow::Result;
use clap::Parser;
use rust_data_transforms::cli_input_types::CliRawDockets;
use rust_data_transforms::data_processing_traits::ProcessFrom;
use rust_data_transforms::jurisdiction_schema_mapping::FixedJurisdiction;
use rust_data_transforms::sql_ingester_tasks::redis_author_cache::init_redis_client;
use rust_data_transforms::types::processed::ProcessedGenericDocket;
use serde_json;
use std::io::{self, Read, Write};
use tracing_subscriber;

#[derive(Parser)]
#[command(name = "process-dockets")]
#[command(about = "Processes raw generic dockets into processed format")]
struct Cli {
    #[arg(long, value_enum, help = "Fixed jurisdiction to use for processing")]
    fixed_jur: FixedJurisdiction,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_max_level(tracing::Level::INFO)
        .init();

    // Initialize Redis cache early
    if let Err(e) = init_redis_client().await {
        tracing::warn!("Redis initialization failed: {}, continuing with database-only mode", e);
    }

    let cli = Cli::parse();

    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;

    if input.trim().is_empty() {
        anyhow::bail!("No input provided on stdin");
    }

    let cli_raw_dockets: CliRawDockets = serde_json::from_str(&input)?;
    let raw_dockets: Vec<_> = cli_raw_dockets.into();
    let mut processed_dockets = Vec::new();

    for raw_docket in raw_dockets {
        let processed =
            ProcessedGenericDocket::process_from(raw_docket, None, cli.fixed_jur).await?;
        processed_dockets.push(processed);
    }

    // This returns a list even if only one was imported just to make the output json schema
    // consistent.
    let result = serde_json::to_string(&processed_dockets)?;

    io::stdout().write_all(result.as_bytes())?;
    io::stdout().flush()?;

    Ok(())
}

