use anyhow::Result;
use clap::{Parser, Subcommand};
use rust_data_transforms::jurisdiction_schema_mapping::FixedJurisdiction;
use rust_data_transforms::sql_ingester_tasks::dokito_sql_connection::get_dokito_pool;
use rust_data_transforms::sql_ingester_tasks::recreate_dokito_table_schema::recreate_schema;
use rust_data_transforms::attachments::postgres_migration_utils::{
    update_attachment_hashes_from_redis, migrate_attachments_to_redis
};
use serde_json;
use sqlx::{FromRow, query_as};
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
    /// Migrate PostgreSQL attachment data to Redis attachment system
    MigrateAttachmentsToRedis {
        #[arg(long, value_enum, help = "Fixed jurisdiction to migrate attachments for")]
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
        Commands::MigrateAttachmentsToRedis { fixed_jur } => {
            tracing::info!(
                "Starting PostgreSQL to Redis attachment migration for jurisdiction: {}",
                fixed_jur.get_postgres_schema_name()
            );

            migrate_attachments_to_redis(fixed_jur).await?;

            tracing::info!("Successfully completed attachment migration operation");
        }
    }

    Ok(())
}

