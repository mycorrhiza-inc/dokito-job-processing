use anyhow::anyhow;
use futures_util::join;
use mycorrhiza_common::s3_generic::S3Credentials;
use mycorrhiza_common::s3_generic::cannonical_location::{
    CannonicalS3ObjectLocation, download_openscrapers_object, get_openscrapers_json_key,
};
use mycorrhiza_common::s3_generic::fetchers_and_getters::{S3Addr, S3DirectoryAddr};
use non_empty_string::non_empty_string;
use tracing::{debug, info};

// RawAttachment removed - using Redis-based attachment system instead
use crate::env_vars::{DIGITALOCEAN_S3, OPENSCRAPERS_S3_OBJECT_BUCKET};
use crate::raw::JurisdictionInfo;
use aws_sdk_s3::Client as S3Client;
use mycorrhiza_common::hash::Blake2bHash;

pub fn get_raw_attach_file_key(hash: Blake2bHash) -> String {
    let key = format!("raw/file/{hash}");
    debug!(%hash, "Generated raw attachment file key: {}", key);
    key
}

// RawAttachment CannonicalS3ObjectLocation implementation removed - using Redis-based attachment system instead

pub struct DocketAddress {
    pub docket_govid: String,
    pub jurisdiction: JurisdictionInfo,
}

pub async fn make_s3_client() -> S3Client {
    DIGITALOCEAN_S3.make_s3_client().await
}

// Fetching stuff for attachments, seperate from all the other object stuff

pub async fn fetch_attachment_file_from_s3(
    s3_client: &S3Client,
    hash: Blake2bHash,
) -> anyhow::Result<Vec<u8>> {
    info!(%hash, "Fetching attachment file from S3");
    let key = get_raw_attach_file_key(hash);
    S3Addr::new(s3_client, &OPENSCRAPERS_S3_OBJECT_BUCKET, &key)
        .download_bytes()
        .await
}

// fetch_attachment_file_from_s3_with_filename removed - use Redis-based attachment system for metadata

pub fn get_jurisdiction_prefix(jurisdiction: &JurisdictionInfo) -> String {
    let country = &*jurisdiction.country;
    let state = &*jurisdiction.state;
    let jurisdiction_name = &*jurisdiction.jurisdiction;
    let key = format!("objects/{country}/{state}/{jurisdiction_name}");
    key
}

// does_openscrapers_attachment_exist removed - use Redis-based attachment system for metadata

pub async fn list_processed_cases_for_jurisdiction(
    s3_client: &S3Client,
    JurisdictionInfo {
        jurisdiction,
        state,
        country,
    }: &JurisdictionInfo,
) -> anyhow::Result<Vec<String>> {
    info!(
        jurisdiction,
        state, country, "Listing cases for jurisdiction"
    );
    let prefix = format!("objects/{country}/{state}/{jurisdiction}/");
    info!("Listing cases with prefix: {}", prefix);
    let mut matches = S3DirectoryAddr::new(s3_client, &OPENSCRAPERS_S3_OBJECT_BUCKET, &prefix)
        .list_all()
        .await?;
    for val in matches.iter_mut() {
        if let Some(stripped_json) = val.strip_suffix(".json")
            && let Some(stripped) = stripped_json.strip_prefix(&prefix)
        {
            *val = stripped.to_string();
        };
    }
    Ok(matches)
}

pub async fn list_raw_cases_for_jurisdiction(
    s3_client: &S3Client,
    JurisdictionInfo {
        jurisdiction,
        state,
        country,
    }: &JurisdictionInfo,
) -> anyhow::Result<Vec<String>> {
    info!(
        jurisdiction,
        state, country, "Listing cases for jurisdiction"
    );
    let prefix = format!("objects_raw/{country}/{state}/{jurisdiction}/");
    info!("Listing cases with prefix: {}", prefix);
    let mut matches = S3DirectoryAddr::new(s3_client, &OPENSCRAPERS_S3_OBJECT_BUCKET, &prefix)
        .list_all()
        .await?;
    for val in matches.iter_mut() {
        if let Some(stripped_json) = val.strip_suffix(".json")
            && let Some(stripped) = stripped_json.strip_prefix(&prefix)
        {
            *val = stripped.to_string();
        };
    }
    Ok(matches)
}

// push_raw_attach_file_to_s3 removed - use direct file upload in downloading_logic.rs
