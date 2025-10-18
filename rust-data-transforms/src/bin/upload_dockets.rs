use clap::Parser;
use rust_data_transforms::jurisdiction_schema_mapping::FixedJurisdiction;
use rust_data_transforms::sql_ingester_tasks::nypuc_ingest::ingest_sql_fixed_jurisdiction_case;
use rust_data_transforms::sql_ingester_tasks::dokito_sql_connection::get_dokito_pool;
use rust_data_transforms::types::processed::ProcessedGenericDocket;
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

    if input.trim_start().starts_with('[') {
        let mut processed_dockets: Vec<ProcessedGenericDocket> = serde_json::from_str(&input)?;

        for processed_docket in processed_dockets.iter_mut() {
            ingest_sql_fixed_jurisdiction_case(processed_docket, cli.fixed_jur, &pool, false).await?;
        }

        let result = serde_json::to_string(&processed_dockets)?;
        io::stdout().write_all(result.as_bytes())?;
    } else {
        let mut processed_docket: ProcessedGenericDocket = serde_json::from_str(&input)?;
        ingest_sql_fixed_jurisdiction_case(&mut processed_docket, cli.fixed_jur, &pool, false).await?;

        let result = serde_json::to_string(&processed_docket)?;
        io::stdout().write_all(result.as_bytes())?;
    };

    io::stdout().flush()?;

    Ok(())
}