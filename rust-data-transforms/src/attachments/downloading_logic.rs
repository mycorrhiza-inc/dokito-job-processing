//! Next-generation attachment downloading with history tracking

use super::redis_store::{RedisAttachmentStore, UpdateRedisAttachment};
use super::tags::AttachmentTag;
use super::types::{AttachmentLocator, AttachmentRecord, AttachmentVersion};
use crate::data_processing_traits::{DownloadIncomplete, RevalidationOutcome};
use crate::jurisdiction_schema_mapping::FixedJurisdiction;
use crate::processing::file_fetching::{FileDownloadError, FileDownloadResult, InternetFileFetch};
// S3 functions are imported via the old downloading module
use crate::sql_ingester_tasks::dokito_sql_connection::get_dokito_pool;
use crate::types::processed::ProcessedGenericAttachment;
use crate::types::raw::JurisdictionInfo;
use aws_sdk_s3::Client as S3Client;
use mycorrhiza_common::file_extension::FileExtension;
use mycorrhiza_common::hash::Blake2bHash;
use non_empty_string::{NonEmptyString, non_empty_string};
// SQL functions are imported via the old downloading module
use std::collections::HashMap;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::{debug, info, warn};
// UUID imported via other modules
use anyhow::Result;

const ATTACHMENT_DOWNLOAD_TRIES: usize = 2;
const DOWNLOAD_RETRY_DELAY_SECONDS: u64 = 2;

/// Extra data for attachment processing
#[derive(Clone)]
pub struct OpenscrapersExtraData {
    pub s3_client: S3Client,
    pub jurisdiction_info: JurisdictionInfo,
    pub fixed_jurisdiction: FixedJurisdiction,
}

/// Implementation of DownloadIncomplete using the new attachment system
pub struct AttachmentProcessor;

impl AttachmentProcessor {
    /// Process attachment download with attachment system
    pub async fn download_incomplete(
        attachment: &mut ProcessedGenericAttachment,
        extra_data: OpenscrapersExtraData,
    ) -> anyhow::Result<RevalidationOutcome> {
        if attachment.hash.is_some() {
            return Ok(RevalidationOutcome::NoChanges);
        }

        let name = NonEmptyString::from_str(&attachment.name)
            .unwrap_or_else(|_| non_empty_string!("unknown_filename"));

        let pool = get_dokito_pool().await?;
        let locator = AttachmentLocator::Url(attachment.url.clone());

        // Check Redis cache first
        if let Some(cached_record) = lookup_attachment_from_redis(&locator).await
            && let Some(current_version) = cached_record.current_version() {
                attachment.hash = Some(current_version.content_hash);
                let _update_pg_result = attempt_to_update_hash_of_postgres_attachment(
                    attachment,
                    extra_data.fixed_jurisdiction,
                    pool,
                )
                .await?;

                // Mark as checked in system
                if let Err(e) = mark_attachment_checked(&locator).await {
                    warn!("Failed to mark attachment as checked: {}", e);
                }

                return Ok(RevalidationOutcome::DidChange);
            }

        debug!(url=%attachment.url, "Downloading attachment file with attachment system");
        let extension = &attachment.document_extension;

        let download_start = Instant::now();

        let FileDownloadResult {
            data: file_contents,
            filename: server_filename,
        } = download_file_content_validated_with_retries(&attachment.url, extension).await?;
        let download_time_seconds = Instant::now().duration_since(download_start).as_secs_f64();
        let upload_start = Instant::now();
        let hash = Blake2bHash::from_bytes(&file_contents);
        debug!(%hash, url=%attachment.url, "Successfully downloaded file with attachment system");

        let mut metadata = HashMap::new();
        if let Some(server_name) = server_filename {
            metadata.insert("server_filename".to_string(), server_name);
        }

        // Create new attachment version
        let version = AttachmentVersion::new(
            hash,
            file_contents.len() as u64,
            name.clone(),
            extension.clone(),
        )
        .with_metadata(
            "download_time_seconds".to_string(),
            download_time_seconds.to_string(),
        );

        // Add any additional metadata
        let version = metadata
            .clone()
            .into_iter()
            .fold(version, |v, (k, val)| v.with_metadata(k, val));

        // Upload file content to S3 (metadata is now stored in Redis)
        let file_key = format!("raw/file/{hash}");
        use mycorrhiza_common::s3_generic::fetchers_and_getters::S3Addr;
        use crate::types::env_vars::OPENSCRAPERS_S3_OBJECT_BUCKET;

        S3Addr::new(&extra_data.s3_client, &OPENSCRAPERS_S3_OBJECT_BUCKET, &file_key)
            .upload_bytes(file_contents)
            .await?;

        attachment.hash = Some(hash);

        // Update postgres
        let update_pg_result = attempt_to_update_hash_of_postgres_attachment(
            attachment,
            extra_data.fixed_jurisdiction,
            pool,
        )
        .await;
        if let Err(err) = update_pg_result {
            warn!(%err, url=%attachment.url, %hash, "Updating attachment hash in postgres encountered an error");
        }

        // Store in Redis system
        if let Err(err) =
            store_attachment_in_redis(&locator, extra_data.fixed_jurisdiction, version).await
        {
            warn!(%err, url=%attachment.url, %hash, "Failed to store attachment in Redis system");
        }

        let upload_time_seconds = Instant::now().duration_since(upload_start).as_secs_f64();
        info!(%hash, url=%attachment.url, %download_time_seconds, %upload_time_seconds, "Successfully downloaded attachment with attachment system");
        Ok(RevalidationOutcome::DidChange)
    }
}

/// Trait implementation for backward compatibility
impl DownloadIncomplete for ProcessedGenericAttachment {
    type ExtraData = OpenscrapersExtraData;
    async fn download_incomplete(
        &mut self,
        extra_data: Self::ExtraData,
    ) -> anyhow::Result<RevalidationOutcome> {
        AttachmentProcessor::download_incomplete(self, extra_data).await
    }
}

/// Lookup attachment from Redis system
async fn lookup_attachment_from_redis(locator: &AttachmentLocator) -> Option<AttachmentRecord> {
    let store = RedisAttachmentStore::new().ok()?;
    store.get(locator).await.ok().flatten()
}

/// Store attachment in Redis system
async fn store_attachment_in_redis(
    locator: &AttachmentLocator,
    jurisdiction: FixedJurisdiction,
    version: AttachmentVersion,
) -> Result<()> {
    let store = RedisAttachmentStore::new()?;

    // Check if record already exists
    if let Some(mut existing_record) = store.get(locator).await? {
        // Track the previous tag before any modifications
        let previous_tag = AttachmentTag::new(existing_record.jurisdiction, existing_record.is_downloaded());

        // Check if this is a new version or same content
        if let Some(current_version) = existing_record.current_version()
            && current_version.same_content(&version) {
                // Same content, just mark as checked
                existing_record.mark_checked();
                let update = UpdateRedisAttachment {
                    record: existing_record,
                    previous_tag,
                };
                store.update(update).await?;
                return Ok(());
            }

        // New version, add it to history
        existing_record.add_version(version);
        let update = UpdateRedisAttachment {
            record: existing_record,
            previous_tag,
        };
        store.update(update).await?;
    } else {
        // Create new record
        let mut record = AttachmentRecord::new(locator.clone(), jurisdiction);
        record.add_version(version);

        // Store as downloaded since we just downloaded it
        let tag = AttachmentTag::new(jurisdiction, true);
        store.store(&record, tag).await?;
    }

    Ok(())
}

/// Mark attachment as checked in system
async fn mark_attachment_checked(locator: &AttachmentLocator) -> Result<()> {
    let store = RedisAttachmentStore::new()?;
    store.mark_checked(locator).await
}


// Supporting types and functions
pub enum PGUpdateOutcome {
    DidNotExist,
    Updated,
}

#[derive(sqlx::FromRow, Clone)]
struct AttachmentHashRecord {
    uuid: uuid::Uuid,
    file_hash_if_downloaded: String,
}

pub async fn attempt_to_update_hash_of_postgres_attachment(
    proc_attach: &ProcessedGenericAttachment,
    fixed_jur: FixedJurisdiction,
    pool: &sqlx::PgPool,
) -> Result<PGUpdateOutcome, anyhow::Error> {
    let hash_string = if let Some(val) = proc_attach.hash {
        val.to_string()
    } else {
        return Ok(PGUpdateOutcome::DidNotExist);
    };

    let pg_schema = fixed_jur.get_postgres_schema_name();

    let attach_uuid_guess = proc_attach.object_uuid;
    let optional_record: Option<AttachmentHashRecord> = sqlx::query_as::<_, AttachmentHashRecord>(
        &format!("SELECT uuid, file_hash_if_downloaded FROM {pg_schema}.attachments WHERE uuid=$1"),
    )
    .bind(attach_uuid_guess)
    .fetch_optional(pool)
    .await?;
    if let Some(record) = optional_record {
        let downloaded_hash = &*record.file_hash_if_downloaded;
        if downloaded_hash != hash_string {
            let _res = sqlx::query(&format!(
                "UPDATE {pg_schema}.attachments SET file_hash_if_downloaded=$1, updated_at=NOW() WHERE uuid=$2"
            ))
            .bind(&hash_string)
            .bind(attach_uuid_guess)
            .execute(pool)
            .await?;
        }
        Ok(PGUpdateOutcome::Updated)
    } else {
        Ok(PGUpdateOutcome::DidNotExist)
    }
}


static MAXIMUM_EXTERNAL_FILE_DOWNLOADS: Semaphore = Semaphore::const_new(10);

async fn download_file_content_validated_with_retries<T: InternetFileFetch + ?Sized>(
    to_fetch: &T,
    extension: &FileExtension,
) -> Result<FileDownloadResult, FileDownloadError> {
    let permit = MAXIMUM_EXTERNAL_FILE_DOWNLOADS.acquire().await.unwrap();
    let mut last_error: Option<FileDownloadError> = None;
    for _ in 0..ATTACHMENT_DOWNLOAD_TRIES {
        match to_fetch
            .download_file_with_timeout(Duration::from_secs(60))
            .await
        {
            Ok(file_contents) => {
                if let Err(err) = extension.is_valid_file_contents(&file_contents.data) {
                    tracing::error!(%extension, ?to_fetch, %err, "Downloaded file did not match extension");
                    last_error = Some(FileDownloadError::InvalidReturnData(err))
                } else {
                    return Ok(file_contents);
                }
            }
            Err(err) => {
                tracing::error!(?to_fetch, %err, "Encountered error downloading file");
                if !err.is_retryable() {
                    return Err(err);
                };
                last_error = Some(err);
            }
        };
        sleep(Duration::from_secs(DOWNLOAD_RETRY_DELAY_SECONDS)).await;
    }

    tracing::error!(%extension, ?to_fetch, "Could not download file from url despite retries");
    drop(permit);

    Err(last_error.unwrap())
}

