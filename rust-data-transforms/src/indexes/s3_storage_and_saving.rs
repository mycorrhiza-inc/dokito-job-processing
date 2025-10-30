use std::{
    collections::BTreeMap,
    path::Path,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use crate::attachments::RawAttachment;
use crate::types::env_vars::{DIGITALOCEAN_S3, OPENSCRAPERS_S3_OBJECT_BUCKET};
use crate::utils::progress_reporter::{log_completion_stats, start_progress_reporter};
use aws_sdk_s3::Client;
use mycorrhiza_common::{
    hash::Blake2bHash,
    s3_generic::{
        S3Credentials,
        cannonical_location::{CannonicalS3ObjectLocation, download_openscrapers_object},
        fetchers_and_getters::S3DirectoryAddr,
    },
};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::indexes::attachment_url_index::AttachIndex;

const MAX_RETRY_ATTEMPTS: usize = 3;

async fn download_attachment_with_retries(
    s3_client: &Client,
    hash: &Blake2bHash,
) -> anyhow::Result<RawAttachment> {
    let mut last_error = None;

    for attempt in 1..=MAX_RETRY_ATTEMPTS {
        match download_openscrapers_object::<RawAttachment>(s3_client, hash).await {
            Ok(attachment) => return Ok(attachment),
            Err(e) => {
                if attempt < MAX_RETRY_ATTEMPTS {
                    debug!(%hash, attempt = %attempt, error = %e, "Download attempt failed, retrying");
                } else {
                    last_error = Some(e);
                }
            }
        }
    }

    Err(last_error.unwrap())
}

async fn get_all_attachment_hashes(s3_client: &Client) -> anyhow::Result<Vec<Blake2bHash>> {
    let dir = "raw/metadata/";
    let bucket: &'static str = &OPENSCRAPERS_S3_OBJECT_BUCKET;
    let attach_folder = S3DirectoryAddr {
        s3_client,
        bucket,
        prefix: dir.into(),
    };
    let prefixes = attach_folder.list_all().await?;

    let mut hashes = Vec::with_capacity(prefixes.len());
    for prefix in prefixes {
        let path = Path::new(prefix.trim());
        let stem = path.file_stem().unwrap_or_default().to_string_lossy();
        let stripped_filekey = stem.strip_prefix(dir).unwrap_or(&stem);
        let stripped_filekey = stripped_filekey
            .strip_suffix(".json")
            .unwrap_or(stripped_filekey);
        if let Ok(hash) = Blake2bHash::from_str(stripped_filekey) {
            hashes.push(hash);
        } else {
            warn!(%stripped_filekey,"Encountered file name that could not be converted to a hash.")
        }
    }
    Ok(hashes)
}

pub async fn pull_index_from_s3() -> AttachIndex {
    let s3_client = DIGITALOCEAN_S3.make_s3_client().await;
    if let Ok(fetched_index) =
        download_openscrapers_object::<CanonAttachIndex>(&s3_client, &()).await
    {
        return fetched_index.0;
    };
    info!("Failed fetching from s3, setting index as empty.");

    BTreeMap::new()
}

// async fn generate_attachment_url_index() -> anyhow::Result<AttachIndex> {
//     let s3_client = Arc::new(DIGITALOCEAN_S3.make_s3_client().await);
//     let hashlist = get_all_attachment_hashes(&s3_client).await?;
//     let results = stream::iter(hashlist.iter())
//         .map(|hash| {
//             let s3_clone = s3_client.clone();
//             async move {
//                 let val = download_openscrapers_object::<RawAttachment>(&*s3_clone, hash).await;
//                 val
//             }
//         })
//         .buffer_unordered(20)
//         .collect::<Vec<_>>()
//         .await;
//     let map = results
//         .into_iter()
//         .filter_map(|r| match r {
//             Ok(att) => Some((att.url.clone(), att)),
//             Err(_err) => None,
//         })
//         .collect();
//     Ok(map)
// }

pub async fn generate_attachment_url_index() -> anyhow::Result<AttachIndex> {
    info!("Starting attachment index generation");
    let s3_client = Arc::new(DIGITALOCEAN_S3.make_s3_client().await);
    let hashlist = get_all_attachment_hashes(&s3_client).await?;
    let total_attachments = hashlist.len();
    info!(hashlist_length = %total_attachments,"Got all hashes from directory.");

    // Progress tracking
    let completed_count = Arc::new(AtomicUsize::new(0));

    // Start progress reporting task
    let (progress_handle, start_time) = start_progress_reporter(
        completed_count.clone(),
        total_attachments,
        "attachment_index_generation".to_string(),
    );

    // Limit concurrency to 20
    let semaphore = Arc::new(Semaphore::new(20));
    let mut handles = Vec::with_capacity(hashlist.len());

    for hash in hashlist {
        let s3_clone = s3_client.clone();
        let sem_clone = semaphore.clone();
        let completed_count_clone = completed_count.clone();
        // Spawn each task
        let handle = tokio::spawn(async move {
            // Acquire a permit before starting
            let _permit = sem_clone.acquire().await.unwrap();
            let res = download_attachment_with_retries(&s3_clone, &hash).await;
            if let Err(e) = &res {
                warn!(%hash, error=%e, "Failed to download attachment after {} attempts", MAX_RETRY_ATTEMPTS);
            };
            // Increment counter regardless of success/failure
            completed_count_clone.fetch_add(1, Ordering::Relaxed);
            res
        });

        handles.push(handle);
    }

    // Collect unordered results
    let mut map = AttachIndex::new();
    for handle in handles {
        match handle.await? {
            Ok(att) => {
                map.insert(att.url.clone(), att);
            }
            Err(_err) => {
                // Ignore errors just like in your filter_map
            }
        }
    }

    // Stop progress reporting
    progress_handle.abort();

    log_completion_stats(
        "attachment_index_generation",
        total_attachments,
        start_time,
        Some(map.len()),
    );

    Ok(map)
}

#[derive(Deserialize, Serialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct CanonAttachIndex(pub AttachIndex);

impl CannonicalS3ObjectLocation for CanonAttachIndex {
    type AddressInfo = ();
    fn generate_object_key(_: &Self::AddressInfo) -> String {
        "indexes/global/attachment_urls".to_string()
    }
    fn generate_bucket(_: &Self::AddressInfo) -> &'static str {
        &OPENSCRAPERS_S3_OBJECT_BUCKET
    }
    fn get_credentials(_: &Self::AddressInfo) -> &'static S3Credentials {
        &DIGITALOCEAN_S3
    }
}
