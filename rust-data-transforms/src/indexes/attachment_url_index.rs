use std::collections::BTreeMap;

use crate::types::{attachments::RawAttachment, env_vars::DIGITALOCEAN_S3};
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Duration;
use mycorrhiza_common::{
    s3_generic::cannonical_location::upload_object,
    tasks::{ExecuteUserTask, display_error_as_json},
};
use redis::AsyncCommands;
use serde_json;
use sha2::{Digest, Sha256};

use crate::indexes::s3_storage_and_saving::{CanonAttachIndex, generate_attachment_url_index};

pub type AttachIndex = BTreeMap<String, RawAttachment>;

/// Cache TTL in seconds (4 days for attachment index)
const ATTACHMENT_CACHE_TTL: i64 = Duration::days(4).num_seconds();

/// Generate consistent cache key for attachment URLs
fn attachment_cache_key(url: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(url.as_bytes());
    let url_hash = hex::encode(hasher.finalize());
    format!("attachment_url:{}", url_hash)
}

/// Get Redis connection using the existing infrastructure
async fn get_redis_connection() -> Option<redis::aio::MultiplexedConnection> {
    use redis::Client;
    use std::env;
    use std::sync::OnceLock;
    use tokio::sync::Mutex;

    static REDIS_CLIENT: OnceLock<Mutex<Option<Client>>> = OnceLock::new();

    let client_mutex = REDIS_CLIENT.get_or_init(|| Mutex::new(None));
    let mut client_guard = client_mutex.lock().await;

    // Initialize client if not already done
    if client_guard.is_none() {
        let redis_url =
            env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
        match Client::open(redis_url.as_str()) {
            Ok(client) => {
                // Test the connection
                if let Ok(mut conn) = client.get_multiplexed_async_connection().await {
                    let _: Result<String, _> = redis::cmd("PING").query_async(&mut conn).await;
                    *client_guard = Some(client);
                }
            }
            Err(_) => {
                // Redis unavailable, return None
                return None;
            }
        }
    }

    if let Some(client) = client_guard.as_ref() {
        client.get_multiplexed_async_connection().await.ok()
    } else {
        None
    }
}

/// Get cached attachment from Redis
async fn get_cached_attachment(url: &str) -> Option<RawAttachment> {
    let mut conn = get_redis_connection().await?;
    let key = attachment_cache_key(url);

    match conn.get::<_, String>(&key).await {
        Ok(cached_data) => match serde_json::from_str::<RawAttachment>(&cached_data) {
            Ok(attachment) => {
                tracing::debug!("Cache hit for attachment URL: {}", url);
                Some(attachment)
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to deserialize cached attachment for URL {}: {}",
                    url,
                    e
                );
                None
            }
        },
        Err(_) => {
            tracing::debug!("Cache miss for attachment URL: {}", url);
            None
        }
    }
}

/// Cache attachment data in Redis
async fn cache_attachment(url: &str, attachment: &RawAttachment) -> Result<()> {
    let mut conn = match get_redis_connection().await {
        Some(conn) => conn,
        None => return Ok(()), // Gracefully handle Redis unavailability
    };

    let key = attachment_cache_key(url);
    let serialized = serde_json::to_string(attachment)
        .context("Failed to serialize attachment for Redis cache")?;

    let _: () = conn
        .set_ex(&key, &serialized, ATTACHMENT_CACHE_TTL as u64)
        .await
        .context("Failed to cache attachment data")?;

    tracing::debug!("Cached attachment for URL: {}", url);
    Ok(())
}

/// Store the entire attachment index in Redis as a hash
async fn store_full_index_in_redis(index: &AttachIndex) -> Result<()> {
    let mut conn = match get_redis_connection().await {
        Some(conn) => conn,
        None => return Ok(()), // Gracefully handle Redis unavailability
    };

    tracing::info!("Storing {} attachments in Redis cache", index.len());

    // Store each attachment individually for efficient lookups
    for (url, attachment) in index.iter() {
        if let Err(e) = cache_attachment(url, attachment).await {
            tracing::warn!("Failed to cache attachment for URL {}: {}", url, e);
        }
    }

    // Also store a metadata key to track when the index was last updated
    let metadata_key = "attachment_index:metadata";
    let metadata = serde_json::json!({
        "last_updated": chrono::Utc::now().to_rfc3339(),
        "total_count": index.len()
    });
    let _: () = conn
        .set_ex(
            &metadata_key,
            metadata.to_string(),
            ATTACHMENT_CACHE_TTL as u64,
        )
        .await
        .context("Failed to store index metadata")?;

    tracing::info!("Successfully stored attachment index in Redis");
    Ok(())
}

pub async fn regenrate_url_attach_index() -> anyhow::Result<()> {
    let attach_index = generate_attachment_url_index().await?;

    // Store in Redis
    store_full_index_in_redis(&attach_index).await?;

    // Keep S3 as backup storage
    let s3_client = DIGITALOCEAN_S3.make_s3_client().await;
    let canon_object = CanonAttachIndex(attach_index);
    let _res = upload_object(&s3_client, &(), &canon_object).await;

    Ok(())
}

#[derive(Default, Clone, Copy)]
pub struct RegenerateUrlAttachIndex {}
#[async_trait]
impl ExecuteUserTask for RegenerateUrlAttachIndex {
    async fn execute_task(self: Box<Self>) -> Result<serde_json::Value, serde_json::Value> {
        let res = regenrate_url_attach_index().await;
        match res {
            Ok(_) => Ok("Task Succeeded".into()),
            Err(err) => Err(display_error_as_json(&err)),
        }
    }
    fn get_task_label_static() -> &'static str
    where
        Self: Sized,
    {
        "regenrate_url_attach_index"
    }
    fn get_task_label(&self) -> &'static str {
        "regenrate_url_attach_index"
    }
}

pub async fn lookup_hash_from_url(url: &str) -> Option<RawAttachment> {
    // First, try Redis cache
    if let Some(cached_attachment) = get_cached_attachment(url).await {
        return Some(cached_attachment);
    }

    None
}
