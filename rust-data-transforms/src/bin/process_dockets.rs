use clap::Parser;
use rust_data_transforms::jurisdiction_schema_mapping::FixedJurisdiction;
use rust_data_transforms::data_processing_traits::ProcessFrom;
use rust_data_transforms::types::processed::ProcessedGenericDocket;
use rust_data_transforms::types::raw::RawGenericDocket;
use serde_json;
use std::io::{self, Read, Write};
use anyhow::Result;

#[derive(Parser)]
#[command(name = "process-dockets")]
#[command(about = "Processes raw generic dockets into processed format")]
struct Cli {
    #[arg(long, value_enum, help = "Fixed jurisdiction to use for processing")]
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

    let result = if input.trim_start().starts_with('[') {
        let raw_dockets: Vec<RawGenericDocket> = serde_json::from_str(&input)?;
        let mut processed_dockets = Vec::new();

        for raw_docket in raw_dockets {
            let processed = ProcessedGenericDocket::process_from(raw_docket, None, cli.fixed_jur).await?;
            processed_dockets.push(processed);
        }

        serde_json::to_string(&processed_dockets)?
    } else {
        let raw_docket: RawGenericDocket = serde_json::from_str(&input)?;
        let processed = ProcessedGenericDocket::process_from(raw_docket, None, cli.fixed_jur).await?;
        serde_json::to_string(&processed)?
    };

    io::stdout().write_all(result.as_bytes())?;
    io::stdout().flush()?;

    Ok(())
}