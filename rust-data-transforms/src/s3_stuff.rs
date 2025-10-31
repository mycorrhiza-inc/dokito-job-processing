use mycorrhiza_common::s3_generic::fetchers_and_getters::{S3Addr, S3DirectoryAddr};
use mycorrhiza_common::s3_generic::s3_uri::S3LocationWithCredentials;
use tracing::{debug, info};

use crate::types::env_vars::{DIGITALOCEAN_S3, OPENSCRAPERS_S3_OBJECT_BUCKET};
use crate::types::processed::ProcessedGenericDocket;
use crate::types::raw::JurisdictionInfo;
use crate::types::raw::RawGenericDocket;
use aws_sdk_s3::Client as S3Client;
use mycorrhiza_common::hash::Blake2bHash;



pub struct DocketAddress {
    pub docket_govid: String,
    pub jurisdiction: JurisdictionInfo,
}

impl CannonicalS3ObjectLocation for RawGenericDocket {
    type AddressInfo = DocketAddress;

    fn generate_object_key(addr: &Self::AddressInfo) -> String {
        let country = &*addr.jurisdiction.country;
        let state = &*addr.jurisdiction.state;
        let jurisdiction = &*addr.jurisdiction.jurisdiction;
        let case_name = &*addr.docket_govid;
        format!("objects_raw/{country}/{state}/{jurisdiction}/{case_name}")
    }
}
impl CannonicalS3ObjectLocation for ProcessedGenericDocket {
    type AddressInfo = DocketAddress;

    fn generate_object_key(addr: &Self::AddressInfo) -> String {
        let country = &*addr.jurisdiction.country;
        let state = &*addr.jurisdiction.state;
        let jurisdiction = &*addr.jurisdiction.jurisdiction;
        let case_name = &*addr.docket_govid;
        format!("objects/{country}/{state}/{jurisdiction}/{case_name}")
    }
}

pub trait CannonicalS3ObjectLocation: serde::Serialize + serde::de::DeserializeOwned {
    type AddressInfo;
    fn generate_object_key(addr: &Self::AddressInfo) -> String;
}

pub fn get_openscrapers_json_key<T: CannonicalS3ObjectLocation>(addr: &T::AddressInfo) -> String {
    T::generate_object_key(addr) + ".json"
}

pub fn get_s3_json_uri<T: CannonicalS3ObjectLocation>(addr: &T::AddressInfo) -> String {
    let bucket = &**OPENSCRAPERS_S3_OBJECT_BUCKET;
    let key = get_openscrapers_json_key::<T>(addr);
    let credentials = &*DIGITALOCEAN_S3;
    S3LocationWithCredentials::from_key_bucket_and_credentials(&key, bucket, credentials)
        .to_string()
}

pub async fn download_openscrapers_object<T: CannonicalS3ObjectLocation>(
    s3_client: &S3Client,
    addr: &T::AddressInfo,
) -> anyhow::Result<T> {
    let key = get_openscrapers_json_key::<T>(addr);
    let bucket = &**OPENSCRAPERS_S3_OBJECT_BUCKET;
    S3Addr::new(s3_client, bucket, &key).download_json().await
}

pub async fn upload_object<T: CannonicalS3ObjectLocation>(
    s3_client: &S3Client,
    addr: &T::AddressInfo,
    object: &T,
) -> anyhow::Result<()> {
    let key = get_openscrapers_json_key::<T>(addr);
    let bucket = &**OPENSCRAPERS_S3_OBJECT_BUCKET;
    S3Addr::new(s3_client, bucket, &key)
        .upload_json(&object)
        .await
}

pub async fn delete_openscrapers_s3_object<T: CannonicalS3ObjectLocation>(
    s3_client: &S3Client,
    addr: &T::AddressInfo,
) -> anyhow::Result<()> {
    let key = get_openscrapers_json_key::<T>(addr);
    let bucket = &**OPENSCRAPERS_S3_OBJECT_BUCKET;
    S3Addr::new(s3_client, bucket, &key).delete_file().await
}

pub fn generate_s3_object_uri_from_key(key: &str) -> String {
    let bucket = &**OPENSCRAPERS_S3_OBJECT_BUCKET;
    let credentials = &*DIGITALOCEAN_S3;
    let uri = S3LocationWithCredentials::from_key_bucket_and_credentials(key, bucket, credentials)
        .to_string();
    debug!(key, "Generated S3 object URI: {}", uri);
    uri
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
    let key = format!("raw/file/{hash}");
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
