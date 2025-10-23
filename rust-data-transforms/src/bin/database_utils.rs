use anyhow::Result;
use clap::{Parser, Subcommand};
use rust_data_transforms::jurisdiction_schema_mapping::FixedJurisdiction;
use rust_data_transforms::sql_ingester_tasks::recreate_dokito_table_schema::recreate_schema;
use rust_data_transforms::sql_ingester_tasks::dokito_sql_connection::get_dokito_pool;
use tracing_subscriber;
use sqlx::{query_as, FromRow};
use serde_json;

#[derive(FromRow)]
struct DocketId {
    docket_govid: String,
}

async fn list_docket_ids_for_jurisdiction(fixed_jur: FixedJurisdiction) -> Result<Vec<String>> {
    let pool = get_dokito_pool().await.map_err(|e| anyhow::anyhow!("Failed to get database pool: {}", e))?;
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
            tracing::info!("Starting nuke and reconfigure database operation for jurisdiction: {}", fixed_jur.get_postgres_schema_name());

            recreate_schema(fixed_jur).await?;

            tracing::info!("Successfully completed nuke and reconfigure database operation");
            println!("Database schema for {} has been successfully nuked and reconfigured", fixed_jur.get_postgres_schema_name());
        }
        Commands::ListDocketIds { fixed_jur } => {
            tracing::info!("Listing docket IDs for jurisdiction: {}", fixed_jur.get_postgres_schema_name());

            let docket_ids = list_docket_ids_for_jurisdiction(fixed_jur).await?;

            let json_output = serde_json::to_string(&docket_ids)?;
            println!("{}", json_output);
        }
    }

    Ok(())
}