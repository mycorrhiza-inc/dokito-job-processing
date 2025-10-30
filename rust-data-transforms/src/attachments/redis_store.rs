use super::tags::AttachmentTag;
use crate::types::attachments::RawAttachment;
use anyhow::{Context, Result};
use redis::{AsyncCommands, Client};
use serde_json;
use sha2::{Digest, Sha256};
use std::env;
use std::sync::OnceLock;
use tokio::sync::Mutex;
use tracing::{debug, warn};

/// Redis store for RawAttachment data with tag-based organization
#[derive(Clone)]
pub struct RedisAttachmentStore {
    client: Client,
}

impl RedisAttachmentStore {
    /// Create a new Redis attachment store
    pub fn new() -> Result<Self> {
        let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
        let client = Client::open(redis_url.as_str())
            .context("Failed to create Redis client")?;

        Ok(Self { client })
    }

    /// Get a Redis connection
    async fn get_connection(&self) -> Result<redis::aio::MultiplexedConnection> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .context("Failed to get Redis connection")
    }

    /// Generate consistent cache key for attachment URLs
    fn url_to_key(url: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(url.as_bytes());
        let url_hash = hex::encode(hasher.finalize());
        url_hash
    }

    /// Get the Redis key for storing attachment data
    fn attachment_key(url: &str) -> String {
        format!("raw_attachment:{}", Self::url_to_key(url))
    }

    /// Store a RawAttachment with the given tag
    pub async fn store(&self, attachment: &RawAttachment, tag: AttachmentTag) -> Result<()> {
        let mut conn = self.get_connection().await?;

        // Serialize attachment to JSON
        let data = serde_json::to_string(attachment)
            .context("Failed to serialize attachment")?;

        let key = Self::attachment_key(&attachment.url);
        let url_hash = Self::url_to_key(&attachment.url);

        // Use pipeline for atomic operation
        let mut pipe = redis::pipe();
        pipe.atomic();

        // Store the attachment data
        pipe.hset(&key, "data", &data);

        // Add URL hash to the tag set
        pipe.sadd(tag.redis_key(), &url_hash);

        let _: () = pipe.query_async(&mut conn).await
            .context("Failed to store attachment")?;

        debug!("Stored attachment with URL: {} and tag: {}", attachment.url, tag);
        Ok(())
    }

    /// Get a RawAttachment by URL
    pub async fn get(&self, url: &str) -> Result<Option<RawAttachment>> {
        let mut conn = self.get_connection().await?;
        let key = Self::attachment_key(url);

        let data: Option<String> = conn.hget(&key, "data").await
            .context("Failed to get attachment data")?;

        match data {
            Some(json_data) => {
                let attachment = serde_json::from_str::<RawAttachment>(&json_data)
                    .context("Failed to deserialize attachment")?;
                Ok(Some(attachment))
            }
            None => Ok(None),
        }
    }

    /// Get all RawAttachments with the specified tag using SORT with GET
    pub async fn get_all_by_tag(&self, tag: AttachmentTag) -> Result<Vec<RawAttachment>> {
        let mut conn = self.get_connection().await?;

        // Use SORT command with GET pattern to fetch all attachment data in one Redis call
        let results: Vec<String> = redis::cmd("SORT")
            .arg(tag.redis_key())
            .arg("GET")
            .arg("raw_attachment:*->data")
            .query_async(&mut conn)
            .await
            .context("Failed to get attachments by tag")?;

        let mut attachments = Vec::new();

        for result in results {
            if result.is_empty() {
                // Skip empty results (can happen if attachment was deleted but tag wasn't cleaned up)
                continue;
            }

            match serde_json::from_str::<RawAttachment>(&result) {
                Ok(attachment) => attachments.push(attachment),
                Err(e) => {
                    warn!("Failed to deserialize attachment from Redis: {}", e);
                    // Continue processing other attachments
                }
            }
        }

        debug!("Retrieved {} attachments for tag: {}", attachments.len(), tag);
        Ok(attachments)
    }

    /// Change the tag of an attachment atomically
    pub async fn change_tag(&self, url: &str, from_tag: AttachmentTag, to_tag: AttachmentTag) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let url_hash = Self::url_to_key(url);

        // Use pipeline for atomic operation
        let mut pipe = redis::pipe();
        pipe.atomic();

        // Remove from old tag
        pipe.srem(from_tag.redis_key(), &url_hash);

        // Add to new tag
        pipe.sadd(to_tag.redis_key(), &url_hash);

        let _: () = pipe.query_async(&mut conn).await
            .context("Failed to change tag")?;

        debug!("Changed tag for URL: {} from {} to {}", url, from_tag, to_tag);
        Ok(())
    }

    /// List URL hashes for a specific tag (without fetching the full attachment data)
    pub async fn list_url_hashes_by_tag(&self, tag: AttachmentTag) -> Result<Vec<String>> {
        let mut conn = self.get_connection().await?;

        let url_hashes: Vec<String> = conn.smembers(tag.redis_key()).await
            .context("Failed to list URL hashes by tag")?;

        Ok(url_hashes)
    }

    /// Delete an attachment and remove it from all tags
    pub async fn delete(&self, url: &str) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let key = Self::attachment_key(url);
        let url_hash = Self::url_to_key(url);

        // Use pipeline for atomic operation
        let mut pipe = redis::pipe();
        pipe.atomic();

        // Delete the attachment data
        pipe.del(&key);

        // Remove from all possible tags
        for tag in AttachmentTag::all_tags() {
            pipe.srem(tag.redis_key(), &url_hash);
        }

        let _: () = pipe.query_async(&mut conn).await
            .context("Failed to delete attachment")?;

        debug!("Deleted attachment with URL: {}", url);
        Ok(())
    }

    /// Check if an attachment exists for the given URL
    pub async fn exists(&self, url: &str) -> Result<bool> {
        let mut conn = self.get_connection().await?;
        let key = Self::attachment_key(url);

        let exists: bool = conn.hexists(&key, "data").await
            .context("Failed to check if attachment exists")?;

        Ok(exists)
    }

    /// Get count of attachments for a specific tag
    pub async fn count_by_tag(&self, tag: AttachmentTag) -> Result<usize> {
        let mut conn = self.get_connection().await?;

        let count: usize = conn.scard(tag.redis_key()).await
            .context("Failed to count attachments by tag")?;

        Ok(count)
    }

    /// Get all tags that contain a specific URL
    pub async fn get_tags_for_url(&self, url: &str) -> Result<Vec<AttachmentTag>> {
        let mut conn = self.get_connection().await?;
        let url_hash = Self::url_to_key(url);
        let mut tags = Vec::new();

        for tag in AttachmentTag::all_tags() {
            let is_member: bool = conn.sismember(tag.redis_key(), &url_hash).await
                .context("Failed to check tag membership")?;

            if is_member {
                tags.push(tag);
            }
        }

        Ok(tags)
    }
}

/// Global Redis store instance (following the pattern from attachment_url_index.rs)
static REDIS_STORE: OnceLock<Mutex<Option<RedisAttachmentStore>>> = OnceLock::new();

/// Get or create the global Redis store instance
pub async fn get_redis_store() -> Option<RedisAttachmentStore> {
    let store_mutex = REDIS_STORE.get_or_init(|| Mutex::new(None));
    let mut store_guard = store_mutex.lock().await;

    // Initialize store if not already done
    if store_guard.is_none() {
        match RedisAttachmentStore::new() {
            Ok(store) => {
                // Test the connection
                if let Ok(mut conn) = store.get_connection().await {
                    let _: Result<String, _> = redis::cmd("PING").query_async(&mut conn).await;
                    *store_guard = Some(store);
                }
            }
            Err(e) => {
                warn!("Failed to create Redis store: {}", e);
                return None;
            }
        }
    }

    store_guard.clone()
}