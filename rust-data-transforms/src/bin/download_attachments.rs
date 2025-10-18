use clap::Parser;
use rust_data_transforms::jurisdiction_schema_mapping::FixedJurisdiction;
use rust_data_transforms::processing::attachments::OpenscrapersExtraData;
use rust_data_transforms::data_processing_traits::DownloadIncomplete;
use rust_data_transforms::cli_input_types::CliProcessedDockets;
use rust_data_transforms::types::env_vars::DIGITALOCEAN_S3;
use serde_json;
use std::io::{self, Read, Write};
use anyhow::Result;

#[derive(Parser)]
#[command(name = "download-attachments")]
#[command(about = "Downloads attachments for processed generic dockets and fills in hashes")]
struct Cli {
    #[arg(long, value_enum, help = "Fixed jurisdiction (unused but kept for consistency)", default_value = "new_york_puc")]
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

    let s3_client = DIGITALOCEAN_S3.make_s3_client().await;
    let jurisdiction_info = cli.fixed_jur.into();
    let extra_data = OpenscrapersExtraData {
        s3_client,
        jurisdiction_info,
        fixed_jurisdiction: cli.fixed_jur,
    };

    let cli_processed_dockets: CliProcessedDockets = serde_json::from_str(&input)?;
    let mut processed_dockets: Vec<_> = cli_processed_dockets.into();

    for processed_docket in processed_dockets.iter_mut() {
        for filing in processed_docket.filings.iter_mut() {
            for attachment in filing.attachments.iter_mut() {
                if attachment.hash.is_none() {
                    let _ = attachment.download_incomplete(extra_data.clone()).await?;
                }
            }
        }
    }

    let result = if processed_dockets.len() == 1 {
        serde_json::to_string(&processed_dockets[0])?
    } else {
        serde_json::to_string(&processed_dockets)?
    };
    io::stdout().write_all(result.as_bytes())?;

    io::stdout().flush()?;

    Ok(())
}