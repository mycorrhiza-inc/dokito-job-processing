use anyhow::Result;
use mycorrhiza_common::file_extension::{FileExtension, StaticExtension};
use mycorrhiza_common::hash::Blake2bHash;
use non_empty_string::NonEmptyString;
use sqlx::FromRow;
use uuid::Uuid;

use crate::attachments::{
    AttachmentLocator, AttachmentRecord, AttachmentTag, AttachmentVersion, RedisAttachmentStore,
    get_redis_store,
};
use crate::jurisdiction_schema_mapping::FixedJurisdiction;
use crate::sql_ingester_tasks::dokito_sql_connection::get_dokito_pool;

#[derive(FromRow)]
struct PgAttachmentRecord {
    uuid: Uuid,
    attachment_url: String,
}

#[derive(FromRow)]
pub struct PgAttachmentFull {
    pub uuid: Uuid,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub file_hash_if_downloaded: String,
    pub attachment_file_extension: String,
    pub attachment_file_name: String,
    pub attachment_title: String,
    pub attachment_url: String,
}

pub async fn update_attachment_hashes_from_redis(fixed_jur: FixedJurisdiction) -> Result<()> {
    let pool = get_dokito_pool()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get database pool: {}", e))?;
    let pg_schema = fixed_jur.get_postgres_schema_name();

    tracing::info!(
        "Querying attachments without hashes for jurisdiction: {}",
        pg_schema
    );

    let attachments = sqlx::query_as::<_, PgAttachmentRecord>(&format!(
        "SELECT uuid, attachment_url FROM {}.attachments WHERE file_hash_if_downloaded = '' OR file_hash_if_downloaded IS NULL",
        pg_schema
    ))
    .fetch_all(pool)
    .await?;

    tracing::info!("Found {} attachments without hashes", attachments.len());

    if attachments.is_empty() {
        tracing::info!("No attachments found without hashes, exiting");
        return Ok(());
    }

    let redis_store = get_redis_store()
        .await
        .ok_or_else(|| anyhow::anyhow!("Failed to get Redis attachment store"))?;

    let update_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let processed_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    use futures_util::{StreamExt, stream};

    stream::iter(attachments)
        .map(|attachment| {
            let pool_ref = pool;
            let pg_schema = pg_schema;
            let update_count = update_count.clone();
            let processed_count = processed_count.clone();
            let redis_store = &redis_store;

            async move {
                processed_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let current_processed = processed_count.load(std::sync::atomic::Ordering::Relaxed);

                if current_processed % 100 == 0 {
                    tracing::info!("Processed {} attachments so far", current_processed);
                }

                if let Ok(Some(record)) = redis_store.get_by_url(&attachment.attachment_url).await {
                    if let Some(current_version) = record.current_version() {
                        let hash_string = current_version.content_hash.to_string();
                        match sqlx::query(&format!(
                            "UPDATE {}.attachments SET file_hash_if_downloaded = $1 WHERE uuid = $2",
                            pg_schema
                        ))
                        .bind(&hash_string)
                        .bind(&attachment.uuid)
                        .execute(pool_ref)
                        .await
                        {
                            Ok(_) => {
                                update_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                tracing::debug!("Updated hash for attachment {} (URL: {})", attachment.uuid, attachment.attachment_url);
                            }
                            Err(e) => {
                                tracing::warn!("Failed to update attachment {}: {}", attachment.uuid, e);
                            }
                        }
                    } else {
                        tracing::debug!("Found attachment in Redis but no current version for URL: {}", attachment.attachment_url);
                    }
                } else {
                    tracing::debug!("No attachment record found in Redis for URL: {}", attachment.attachment_url);
                }
            }
        })
        .buffer_unordered(20)
        .collect::<Vec<_>>()
        .await;

    let final_update_count = update_count.load(std::sync::atomic::Ordering::Relaxed);
    let final_processed_count = processed_count.load(std::sync::atomic::Ordering::Relaxed);

    tracing::info!(
        "Completed processing {} attachments. Updated {} attachments with hashes from Redis cache",
        final_processed_count,
        final_update_count
    );

    Ok(())
}

pub async fn migrate_attachments_to_redis(fixed_jur: FixedJurisdiction) -> Result<()> {
    let pool = get_dokito_pool()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get database pool: {}", e))?;
    let pg_schema = fixed_jur.get_postgres_schema_name();

    tracing::info!(
        "Starting attachment migration to Redis for jurisdiction: {}",
        pg_schema
    );

    let redis_store = get_redis_store()
        .await
        .ok_or_else(|| anyhow::anyhow!("Failed to get Redis attachment store"))?;

    let attachments = sqlx::query_as::<_, PgAttachmentFull>(&format!(
        "SELECT uuid, created_at, updated_at, file_hash_if_downloaded, attachment_file_extension, attachment_file_name, attachment_title, attachment_url FROM {}.attachments ORDER BY created_at",
        pg_schema
    ))
    .fetch_all(pool)
    .await?;

    tracing::info!("Found {} attachments to migrate", attachments.len());

    let mut created_count = 0;
    let mut updated_count = 0;
    let mut skipped_count = 0;
    let mut error_count = 0;

    for (index, pg_attachment) in attachments.iter().enumerate() {
        if index % 100 == 0 && index > 0 {
            tracing::info!("Processed {} attachments so far", index);
        }

        if pg_attachment.attachment_url.trim().is_empty() {
            tracing::debug!("Skipping attachment {} with empty URL", pg_attachment.uuid);
            skipped_count += 1;
            continue;
        }

        let locator = AttachmentLocator::Url(pg_attachment.attachment_url.clone());

        match process_single_attachment(&redis_store, pg_attachment, &locator, fixed_jur).await {
            Ok(MigrationResult::Created) => created_count += 1,
            Ok(MigrationResult::Updated) => updated_count += 1,
            Ok(MigrationResult::Skipped) => skipped_count += 1,
            Err(e) => {
                tracing::warn!("Failed to process attachment {}: {}", pg_attachment.uuid, e);
                error_count += 1;
            }
        }
    }

    tracing::info!(
        "Migration completed for jurisdiction {}. Created: {}, Updated: {}, Skipped: {}, Errors: {}",
        pg_schema,
        created_count,
        updated_count,
        skipped_count,
        error_count
    );

    Ok(())
}

#[derive(Debug)]
enum MigrationResult {
    Created,
    Updated,
    Skipped,
}

async fn process_single_attachment(
    redis_store: &RedisAttachmentStore,
    pg_attachment: &PgAttachmentFull,
    locator: &AttachmentLocator,
    fixed_jur: FixedJurisdiction,
) -> Result<MigrationResult> {
    let existing_record = redis_store.get(locator).await?;

    match existing_record {
        Some(mut record) => {
            if record.is_downloaded() {
                tracing::debug!(
                    "Attachment {} already exists with history, skipping",
                    pg_attachment.uuid
                );
                return Ok(MigrationResult::Skipped);
            } else if !pg_attachment.file_hash_if_downloaded.trim().is_empty() {
                let hash = pg_attachment
                    .file_hash_if_downloaded
                    .parse::<Blake2bHash>()
                    .map_err(|e| anyhow::anyhow!("Invalid hash format in PG: {}", e))?;

                let version = create_attachment_version_from_pg(pg_attachment, hash)?;
                record.add_version(version);

                redis_store.update(&record).await?;
                tracing::debug!(
                    "Added version to existing attachment {}",
                    pg_attachment.uuid
                );
                return Ok(MigrationResult::Updated);
            } else {
                tracing::debug!(
                    "Attachment {} exists in Redis but no hash in PG",
                    pg_attachment.uuid
                );
                return Ok(MigrationResult::Skipped);
            }
        }
        None => {
            let mut new_record = AttachmentRecord::new(locator.clone(), fixed_jur);
            new_record.created_at = pg_attachment.created_at;
            new_record.updated_at = pg_attachment.updated_at;

            if !pg_attachment.file_hash_if_downloaded.trim().is_empty() {
                let hash = pg_attachment
                    .file_hash_if_downloaded
                    .parse::<Blake2bHash>()
                    .map_err(|e| anyhow::anyhow!("Invalid hash format in PG: {}", e))?;

                let version = create_attachment_version_from_pg(pg_attachment, hash)?;
                new_record.add_version(version);
            }

            let tag = AttachmentTag::new(
                fixed_jur,
                !pg_attachment.file_hash_if_downloaded.trim().is_empty(),
            );
            redis_store.store(&new_record, tag).await?;
            tracing::debug!("Created new attachment record for {}", pg_attachment.uuid);
            return Ok(MigrationResult::Created);
        }
    }
}

fn create_attachment_version_from_pg(
    pg_attachment: &PgAttachmentFull,
    hash: Blake2bHash,
) -> Result<AttachmentVersion> {
    let extension = if !pg_attachment.attachment_file_extension.trim().is_empty() {
        pg_attachment
            .attachment_file_extension
            .parse::<FileExtension>()
            .unwrap_or(FileExtension::Static(StaticExtension::Pdf))
    } else {
        let filename = if !pg_attachment.attachment_file_name.trim().is_empty() {
            &pg_attachment.attachment_file_name
        } else {
            &pg_attachment.attachment_url
        };

        if let Some(ext) = filename.split('.').last() {
            ext.parse::<FileExtension>()
                .unwrap_or(FileExtension::Static(StaticExtension::Pdf))
        } else {
            FileExtension::Static(StaticExtension::Pdf)
        }
    };

    let name = if !pg_attachment.attachment_file_name.trim().is_empty() {
        NonEmptyString::new(pg_attachment.attachment_file_name.clone())
            .map_err(|_| anyhow::anyhow!("Empty attachment file name"))?
    } else if !pg_attachment.attachment_title.trim().is_empty() {
        NonEmptyString::new(pg_attachment.attachment_title.clone())
            .map_err(|_| anyhow::anyhow!("Empty attachment title"))?
    } else {
        NonEmptyString::new("Unknown Attachment".to_string())
            .map_err(|_| anyhow::anyhow!("Failed to create default name"))?
    };

    let mut version = AttachmentVersion::new(hash, 0, name, extension);

    version.first_seen_at = pg_attachment.created_at;
    version.last_checked_at = pg_attachment.updated_at;

    Ok(version)
}
