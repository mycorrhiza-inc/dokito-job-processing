use anyhow::Result;
use clap::{Parser, Subcommand};
use rust_data_transforms::jurisdiction_schema_mapping::FixedJurisdiction;
use rust_data_transforms::sql_ingester_tasks::recreate_dokito_table_schema::recreate_schema;
use tracing_subscriber;

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
    }

    Ok(())
}