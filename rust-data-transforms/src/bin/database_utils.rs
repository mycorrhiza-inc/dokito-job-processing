use anyhow::Result;
use clap::{Parser, Subcommand};
use rust_data_transforms::indexes::attachment_url_index::{regenrate_url_attach_index, AttachIndex, upload_provided_attachment_index};
use rust_data_transforms::indexes::s3_storage_and_saving::generate_attachment_url_index;
use rust_data_transforms::jurisdiction_schema_mapping::FixedJurisdiction;
use rust_data_transforms::sql_ingester_tasks::dokito_sql_connection::get_dokito_pool;
use rust_data_transforms::sql_ingester_tasks::recreate_dokito_table_schema::recreate_schema;
use serde_json;
use sqlx::{FromRow, query_as};
use std::io::{self, Read};
use tracing_subscriber;

#[derive(FromRow)]
struct DocketId {
    docket_govid: String,
}

async fn list_docket_ids_for_jurisdiction(fixed_jur: FixedJurisdiction) -> Result<Vec<String>> {
    let pool = get_dokito_pool()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get database pool: {}", e))?;
    let pg_schema = fixed_jur.get_postgres_schema_name();

    let docket_ids = query_as::<_, DocketId>(&format!(
        "SELECT docket_govid FROM {}.dockets ORDER BY docket_govid",
        pg_schema
    ))
    .fetch_all(pool)
    .await?;

    Ok(docket_ids.into_iter().map(|d| d.docket_govid).collect())
}

async fn generate_and_upload_attachment_index() -> Result<()> {
    tracing::info!("Starting attachment index generation");

    // Generate the attachment index
    let attach_index = generate_attachment_url_index().await?;
    tracing::info!(
        "Generated attachment index with {} entries",
        attach_index.len()
    );

    // Upload to Redis and backup to S3 (this function handles both)
    regenrate_url_attach_index().await?;
    tracing::info!("Successfully uploaded attachment index to Redis and S3");

    // Convert to JSON and print to stdout for extra safety
    let json_output = serde_json::to_string_pretty(&attach_index)?;
    println!("{}", json_output);

    tracing::info!("Attachment index generation and upload completed successfully");
    Ok(())
}

async fn read_attachment_index_from_stdin() -> Result<AttachIndex> {
    tracing::info!("Reading attachment index from stdin");

    let mut buffer = String::new();
    io::stdin().read_to_string(&mut buffer)?;

    if buffer.trim().is_empty() {
        return Err(anyhow::anyhow!("No data provided via stdin"));
    }

    let attach_index: AttachIndex = serde_json::from_str(&buffer)?;
    tracing::info!("Successfully parsed attachment index with {} entries", attach_index.len());

    Ok(attach_index)
}

async fn upload_attachment_index_from_stdin() -> Result<()> {
    tracing::info!("Starting attachment index upload from stdin");

    // Read and parse the attachment index from stdin
    let attach_index = read_attachment_index_from_stdin().await?;

    // Upload to Redis and backup to S3 using the existing infrastructure
    upload_provided_attachment_index(attach_index).await?;

    tracing::info!("Successfully uploaded attachment index from stdin to Redis and S3");
    Ok(())
}

#[derive(Parser)]
#[command(name = "database-utils")]
#[command(about = "Database utility commands for managing dokito database schemas")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Completely drop and recreate the database schema for a given jurisdiction
    NukeAndReconfigureDatabase {
        #[arg(long, value_enum, help = "Fixed jurisdiction to nuke and reconfigure")]
        fixed_jur: FixedJurisdiction,
    },
    /// List all docket_govid values for a given jurisdiction
    ListDocketIds {
        #[arg(long, value_enum, help = "Fixed jurisdiction to list docket IDs for")]
        fixed_jur: FixedJurisdiction,
    },
    /// Generate attachment URL index, upload to Redis, backup to S3, and print JSON to stdout
    GenerateAttachmentIndex,
    /// Upload attachment index from JSON provided via stdin to Redis and S3
    UploadAttachmentIndexFromStdin,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_max_level(tracing::Level::INFO)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::NukeAndReconfigureDatabase { fixed_jur } => {
            tracing::info!(
                "Starting nuke and reconfigure database operation for jurisdiction: {}",
                fixed_jur.get_postgres_schema_name()
            );

            recreate_schema(fixed_jur).await?;

            tracing::info!("Successfully completed nuke and reconfigure database operation");
            eprintln!(
                "Database schema for {} has been successfully nuked and reconfigured",
                fixed_jur.get_postgres_schema_name()
            );
        }
        Commands::ListDocketIds { fixed_jur } => {
            tracing::info!(
                "Listing docket IDs for jurisdiction: {}",
                fixed_jur.get_postgres_schema_name()
            );

            let docket_ids = list_docket_ids_for_jurisdiction(fixed_jur).await?;

            let json_output = serde_json::to_string(&docket_ids)?;
            println!("{}", json_output);
        }
        Commands::GenerateAttachmentIndex => {
            generate_and_upload_attachment_index().await?;
        }
        Commands::UploadAttachmentIndexFromStdin => {
            upload_attachment_index_from_stdin().await?;
        }
    }

    Ok(())
}

