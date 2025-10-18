use clap::Parser;
use rust_data_transforms::jurisdiction_schema_mapping::FixedJurisdiction;
use rust_data_transforms::processing::attachments::OpenscrapersExtraData;
use rust_data_transforms::data_processing_traits::DownloadIncomplete;
use rust_data_transforms::types::processed::ProcessedGenericDocket;
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

    if input.trim_start().starts_with('[') {
        let mut processed_dockets: Vec<ProcessedGenericDocket> = serde_json::from_str(&input)?;

        for processed_docket in processed_dockets.iter_mut() {
            for filing in processed_docket.filings.iter_mut() {
                for attachment in filing.attachments.iter_mut() {
                    if attachment.hash.is_none() {
                        let _ = attachment.download_incomplete(extra_data.clone()).await?;
                    }
                }
            }
        }

        let result = serde_json::to_string(&processed_dockets)?;
        io::stdout().write_all(result.as_bytes())?;
    } else {
        let mut processed_docket: ProcessedGenericDocket = serde_json::from_str(&input)?;

        for filing in processed_docket.filings.iter_mut() {
            for attachment in filing.attachments.iter_mut() {
                if attachment.hash.is_none() {
                    let _ = attachment.download_incomplete(extra_data.clone()).await?;
                }
            }
        }

        let result = serde_json::to_string(&processed_docket)?;
        io::stdout().write_all(result.as_bytes())?;
    };

    io::stdout().flush()?;

    Ok(())
}