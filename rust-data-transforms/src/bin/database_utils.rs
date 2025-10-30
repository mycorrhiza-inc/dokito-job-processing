use anyhow::Result;
use clap::{Parser, Subcommand};
use rust_data_transforms::indexes::attachment_url_index::regenrate_url_attach_index;
use rust_data_transforms::attachments::{AttachmentLocator, get_redis_store};
// s3_storage_and_saving module removed - use Redis-based attachment system instead
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

// generate_and_upload_attachment_index removed - use Redis-based attachment system instead

// read_attachment_index_from_stdin removed - use Redis-based attachment system instead

// upload_attachment_index_from_stdin removed - use Redis-based attachment system instead

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

    // Get Redis store for the new attachment system
    let redis_store = get_redis_store().await
        .ok_or_else(|| anyhow::anyhow!("Failed to get Redis attachment store"))?;

    // Process attachments concurrently with a limit of 20 at a time
    let update_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let processed_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    use futures_util::{StreamExt, stream};

    stream::iter(attachments)
        .map(|attachment| {
            let pool_ref = pool;
            let pg_schema = pg_schema;
            let update_count = update_count.clone();
            let processed_count = processed_count.clone();
            let redis_store = &redis_store;

            async move {
                processed_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let current_processed = processed_count.load(std::sync::atomic::Ordering::Relaxed);

                if current_processed % 100 == 0 {
                    tracing::info!("Processed {} attachments so far", current_processed);
                }

                // Look up the attachment record from Redis using URL
                if let Ok(Some(record)) = redis_store.get_by_url(&attachment.attachment_url).await {
                    if let Some(current_version) = record.current_version() {
                        let hash_string = current_version.content_hash.to_string();
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
                        tracing::debug!("Found attachment in Redis but no current version for URL: {}", attachment.attachment_url);
                    }
                } else {
                    tracing::debug!("No attachment record found in Redis for URL: {}", attachment.attachment_url);
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
    // Attachment index commands removed - use Redis-based attachment system instead
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
        // Removed attachment index commands - use Redis-based attachment system instead
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

