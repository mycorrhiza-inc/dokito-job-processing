use anyhow::Result;
use clap::Parser;
use rust_data_transforms::cli_input_types::CliProcessedDockets;
use rust_data_transforms::data_processing_traits::DownloadIncomplete;
use rust_data_transforms::jurisdiction_schema_mapping::FixedJurisdiction;
use rust_data_transforms::attachments::OpenscrapersExtraData;
use rust_data_transforms::types::env_vars::DIGITALOCEAN_S3;
use rust_data_transforms::types::processed::{ProcessedGenericAttachment, ProcessedGenericDocket};
use rust_data_transforms::sql_ingester_tasks::dokito_sql_connection::get_dokito_pool;
use mycorrhiza_common::file_extension::FileExtension;
use std::io::{self, Read, Write};
use std::str::FromStr;
use uuid::Uuid;

#[derive(Parser)]
#[command(name = "download-attachments")]
#[command(about = "Downloads attachments for processed generic dockets and fills in hashes")]
struct Cli {
    #[arg(
        long,
        value_enum,
        help = "Fixed jurisdiction (unused but kept for consistency)",
        default_value = "new_york_puc"
    )]
    fixed_jur: FixedJurisdiction,

    #[arg(
        long,
        help = "Download all missing attachments from PostgreSQL instead of using stdin"
    )]
    all_missing_in_postgres: bool,
}

#[derive(sqlx::FromRow)]
struct MissingAttachmentRecord {
    uuid: Uuid,
    name: String,
    url: String,
    attachment_type: String,
    attachment_subtype: String,
    document_extension: String,
    index_in_filing: i64,
    attachment_govid: String,
}

async fn fetch_missing_attachments_from_postgres(
    fixed_jur: FixedJurisdiction,
) -> Result<Vec<ProcessedGenericAttachment>> {
    let pool = get_dokito_pool().await?;
    let pg_schema = fixed_jur.get_postgres_schema_name();

    let query = format!(
        "SELECT uuid, name, url, attachment_type, attachment_subtype, document_extension,
                index_in_filing, attachment_govid
         FROM {pg_schema}.attachments
         WHERE file_hash_if_downloaded IS NULL OR file_hash_if_downloaded = ''"
    );

    let records: Vec<MissingAttachmentRecord> = sqlx::query_as(&query)
        .fetch_all(&*pool)
        .await?;

    let mut attachments = Vec::new();
    for record in records {
        let document_extension = FileExtension::from_str(&record.document_extension)
            .unwrap_or_else(|_| FileExtension::Unknown(record.document_extension.clone()));

        let attachment = ProcessedGenericAttachment {
            name: record.name,
            index_in_filing: record.index_in_filing as u64,
            document_extension,
            object_uuid: record.uuid,
            attachment_govid: record.attachment_govid,
            url: record.url,
            attachment_type: record.attachment_type,
            attachment_subtype: record.attachment_subtype,
            extra_metadata: Default::default(),
            hash: None,
        };
        attachments.push(attachment);
    }

    Ok(attachments)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_max_level(tracing::Level::INFO)
        .init();

    let cli = Cli::parse();

    let s3_client = DIGITALOCEAN_S3.make_s3_client().await;
    let jurisdiction_info = cli.fixed_jur.into();
    let extra_data = OpenscrapersExtraData {
        s3_client,
        jurisdiction_info,
        fixed_jurisdiction: cli.fixed_jur,
    };

    if cli.all_missing_in_postgres {
        // Fetch missing attachments from PostgreSQL and process them
        let mut missing_attachments = fetch_missing_attachments_from_postgres(cli.fixed_jur).await?;

        tracing::info!("Found {} missing attachments in PostgreSQL", missing_attachments.len());

        for attachment in missing_attachments.iter_mut() {
            let _res = attachment
                .download_incomplete(extra_data.clone())
                .await?;
        }

        tracing::info!("Completed downloading {} attachments", missing_attachments.len());

        // Output the processed attachments
        let result = serde_json::to_string(&missing_attachments)?;
        io::stdout().write_all(result.as_bytes())?;
    } else {
        // Original stdin-based processing
        let mut input = String::new();
        io::stdin().read_to_string(&mut input)?;

        if input.trim().is_empty() {
            anyhow::bail!("No input provided on stdin");
        }

        let cli_processed_dockets: CliProcessedDockets = serde_json::from_str(&input)?;
        let mut processed_dockets: Vec<_> = cli_processed_dockets.into();

        for processed_docket in processed_dockets.iter_mut() {
            let _res = processed_docket
                .download_incomplete(extra_data.clone())
                .await?;
        }

        // This returns a list even if only one was imported just to make the output json schema
        // consistent.
        let result = serde_json::to_string(&processed_dockets)?;
        io::stdout().write_all(result.as_bytes())?;
    }

    io::stdout().flush()?;

    Ok(())
}
