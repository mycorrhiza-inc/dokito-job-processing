use anyhow::Result;
use clap::{Parser, Subcommand};
use rust_data_transforms::indexes::attachment_url_index::{regenrate_url_attach_index, AttachIndex, upload_provided_attachment_index, lookup_hash_from_url};
use rust_data_transforms::indexes::s3_storage_and_saving::generate_attachment_url_index;
use rust_data_transforms::jurisdiction_schema_mapping::FixedJurisdiction;
use rust_data_transforms::sql_ingester_tasks::dokito_sql_connection::get_dokito_pool;
use rust_data_transforms::sql_ingester_tasks::recreate_dokito_table_schema::recreate_schema;
use serde_json;
use sqlx::{FromRow, query_as};
use uuid::Uuid;
use std::io::{self, Read};
use tracing_subscriber;
use futures::stream::{self, StreamExt};

#[derive(FromRow)]
struct DocketId {
    docket_govid: String,
}

#[derive(FromRow)]
struct AttachmentRecord {
    uuid: Uuid,
    attachment_url: String,
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

async fn update_attachment_hashes_from_redis(fixed_jur: FixedJurisdiction) -> Result<()> {
    let pool = get_dokito_pool()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get database pool: {}", e))?;
    let pg_schema = fixed_jur.get_postgres_schema_name();

    tracing::info!("Querying attachments without hashes for jurisdiction: {}", pg_schema);

    // Query for all attachments that have empty file_hash_if_downloaded field
    let attachments = query_as::<_, AttachmentRecord>(&format!(
        "SELECT uuid, attachment_url FROM {}.attachments WHERE file_hash_if_downloaded = '' OR file_hash_if_downloaded IS NULL",
        pg_schema
    ))
    .fetch_all(pool)
    .await?;

    tracing::info!("Found {} attachments without hashes", attachments.len());

    if attachments.is_empty() {
        tracing::info!("No attachments found without hashes, exiting");
        return Ok(());
    }

    // Process attachments concurrently with a limit of 20 at a time
    let update_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let processed_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    stream::iter(attachments)
        .map(|attachment| {
            let pool_ref = pool;
            let pg_schema = pg_schema;
            let update_count = update_count.clone();
            let processed_count = processed_count.clone();

            async move {
                processed_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let current_processed = processed_count.load(std::sync::atomic::Ordering::Relaxed);

                if current_processed % 100 == 0 {
                    tracing::info!("Processed {} attachments so far", current_processed);
                }

                // Look up the hash from Redis
                if let Some(raw_attachment) = lookup_hash_from_url(&attachment.attachment_url).await {
                    let hash_string = raw_attachment.hash.to_string();
                    if !hash_string.is_empty() {
                        // Update the database with the found hash
                        match sqlx::query(&format!(
                            "UPDATE {}.attachments SET file_hash_if_downloaded = $1 WHERE uuid = $2",
                            pg_schema
                        ))
                        .bind(&hash_string)
                        .bind(&attachment.uuid)
                        .execute(pool_ref)
                        .await
                        {
                            Ok(_) => {
                                update_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                tracing::debug!("Updated hash for attachment {} (URL: {})", attachment.uuid, attachment.attachment_url);
                            }
                            Err(e) => {
                                tracing::warn!("Failed to update attachment {}: {}", attachment.uuid, e);
                            }
                        }
                    } else {
                        tracing::debug!("Found attachment in Redis but hash is empty for URL: {}", attachment.attachment_url);
                    }
                } else {
                    tracing::debug!("No hash found in Redis for URL: {}", attachment.attachment_url);
                }
            }
        })
        .buffer_unordered(20) // Process up to 20 concurrently
        .collect::<Vec<_>>()
        .await;

    let final_update_count = update_count.load(std::sync::atomic::Ordering::Relaxed);
    let final_processed_count = processed_count.load(std::sync::atomic::Ordering::Relaxed);

    tracing::info!(
        "Completed processing {} attachments. Updated {} attachments with hashes from Redis cache",
        final_processed_count,
        final_update_count
    );

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
    /// Update attachment hashes from Redis cache for attachments missing file_hash_if_downloaded
    UpdateAttachmentHashesFromRedis {
        #[arg(long, value_enum, help = "Fixed jurisdiction to update attachment hashes for")]
        fixed_jur: FixedJurisdiction,
    },
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
        Commands::UpdateAttachmentHashesFromRedis { fixed_jur } => {
            tracing::info!(
                "Starting attachment hash update from Redis for jurisdiction: {}",
                fixed_jur.get_postgres_schema_name()
            );

            update_attachment_hashes_from_redis(fixed_jur).await?;

            tracing::info!("Successfully completed attachment hash update operation");
        }
    }

    Ok(())
}

