use anyhow::Result;
use clap::Parser;
use rust_data_transforms::cli_input_types::CliProcessedDockets;
use rust_data_transforms::jurisdiction_schema_mapping::FixedJurisdiction;
use rust_data_transforms::sql_ingester_tasks::dokito_sql_connection::get_dokito_pool;
use rust_data_transforms::sql_ingester_tasks::nypuc_ingest::ingest_sql_fixed_jurisdiction_case;
use serde_json;
use sqlx::{query, PgPool};
use std::io::{self, Read, Write};
use tracing_subscriber;
use tracing::info;

#[derive(Parser)]
#[command(name = "upload-dockets")]
#[command(about = "Uploads processed generic dockets to PostgreSQL database")]
struct Cli {
    #[arg(
        long,
        value_enum,
        help = "Fixed jurisdiction to use for database upload"
    )]
    fixed_jur: FixedJurisdiction,

    #[arg(
        long,
        help = "Preserve existing dockets instead of deleting them before upload"
    )]
    preserve_existing: bool,
}

async fn delete_existing_docket_cascade(
    pool: &PgPool,
    docket_govid: &str,
    fixed_jur: FixedJurisdiction,
) -> Result<()> {
    let pg_schema = fixed_jur.get_postgres_schema_name();

    // Check if docket exists and delete it (cascade will handle all dependent records)
    let deleted_count = query(&format!(
        "DELETE FROM {}.dockets WHERE docket_govid = $1",
        pg_schema
    ))
    .bind(docket_govid)
    .execute(pool)
    .await?
    .rows_affected();

    if deleted_count > 0 {
        info!("Deleted existing docket {} with cascade", docket_govid);
    } else {
        info!("No existing docket found for {}, proceeding with upload", docket_govid);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_max_level(tracing::Level::INFO)
        .init();

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
        // Delete existing docket if preserve_existing is false (default behavior)
        if !cli.preserve_existing {
            delete_existing_docket_cascade(&pool, processed_docket.case_govid.as_str(), cli.fixed_jur).await?;
        }

        ingest_sql_fixed_jurisdiction_case(processed_docket, cli.fixed_jur, &pool, false).await?;
    }

    // This returns a list even if only one was imported just to make the output json schema
    // consistent.
    //
    let result = serde_json::to_string(&processed_dockets)?;
    io::stdout().write_all(result.as_bytes())?;

    io::stdout().flush()?;

    Ok(())
}

