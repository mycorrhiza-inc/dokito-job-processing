//! Redis operations for the next-generation attachment system

use super::tags::AttachmentTag;
use super::types::{AttachmentLocator, AttachmentRecord, AttachmentVersion};
use anyhow::{Context, Result};
use redis::{AsyncCommands, Client};
use serde_json;
use std::collections::HashMap;
use std::env;
use std::sync::{LazyLock, OnceLock};
use tokio::sync::{Mutex, OnceCell};
use tracing::{debug, warn};


pub static DOKITO_INGEST_REDIS: LazyLock<String> = LazyLock::new(|| {
    env::var("DOKITO_INGEST_REDIS")
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "redis://127.0.0.1:6379".to_string())
});

static GLOBAL_REDIS_CLIENT: LazyLock<Result<Client, anyhow::Error>> = LazyLock::new(|| {
    let redis_url = &**DOKITO_INGEST_REDIS;
    Client::open(redis_url).context("Failed to create Redis client")
});

fn get_global_redis_client() -> Result<&'static Client> {
    GLOBAL_REDIS_CLIENT.as_ref().map_err(|e| anyhow::anyhow!("{}", e))
}

/// Redis store for AttachmentRecord data with tag-based organization
#[derive(Clone)]
pub struct RedisAttachmentStore {
    client: &'static Client,
}

impl RedisAttachmentStore {
    /// Create a new Redis attachment store
    pub fn new() -> Result<Self> {
        Ok(Self {
            client: get_global_redis_client()?,
        })
    }

    /// Get a Redis connection (cheap to clone, can be used concurrently)
    async fn get_connection(&self) -> Result<redis::aio::MultiplexedConnection> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                tracing::error!("Redis connection error details: {}", e);
                anyhow::anyhow!("Failed to get Redis connection: {}", e)
            })
    }

    /// Get the Redis key for storing attachment record data
    fn record_key(locator: &AttachmentLocator) -> String {
        format!("v2_attachment:{}", locator.cache_key())
    }

    /// Store an AttachmentRecord with the given tag
    pub async fn store(&self, record: &AttachmentRecord, tag: AttachmentTag) -> Result<()> {
        self.store_bulk(std::slice::from_ref(record).to_vec()).await
    }

    /// Store multiple AttachmentRecords in bulk (tags derived from records)
    pub async fn store_bulk(&self, records: Vec<AttachmentRecord>) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        let mut conn = self.get_connection().await?;
        let mut pipe = redis::pipe();
        pipe.atomic();

        for record in &records {
            let data =
                serde_json::to_string(record).context("Failed to serialize attachment record")?;
            let key = Self::record_key(&record.locator);
            let cache_key = record.locator.cache_key();
            let tag = AttachmentTag::new(record.jurisdiction, record.is_downloaded());

            pipe.hset(&key, "data", &data);
            pipe.sadd(tag.redis_key(), &cache_key);
        }

        let _: () = pipe
            .query_async(&mut conn)
            .await
            .context("Failed to bulk store attachment records")?;

        debug!("Bulk stored {} attachment records", records.len());
        Ok(())
    }

    /// Get an AttachmentRecord by locator
    pub async fn get(&self, locator: &AttachmentLocator) -> Result<Option<AttachmentRecord>> {
        let mut map = self.get_bulk(std::slice::from_ref(locator)).await?;
        Ok(map.remove(locator))
    }

    /// Get multiple AttachmentRecords by locators in bulk
    pub async fn get_bulk(
        &self,
        locators: &[AttachmentLocator],
    ) -> Result<HashMap<AttachmentLocator, AttachmentRecord>> {
        if locators.is_empty() {
            return Ok(HashMap::new());
        }

        let mut conn = self.get_connection().await?;
        let mut pipe = redis::pipe();

        for locator in locators {
            let key = Self::record_key(locator);
            pipe.hget(&key, "data");
        }

        let results: Vec<Option<String>> = pipe
            .query_async(&mut conn)
            .await
            .context("Failed to bulk get attachment records")?;

        let mut map = HashMap::new();
        for (locator, data) in locators.iter().zip(results.iter()) {
            if let Some(json_data) = data {
                match serde_json::from_str::<AttachmentRecord>(json_data) {
                    Ok(record) => {
                        map.insert(locator.clone(), record);
                    }
                    Err(e) => {
                        warn!("Failed to deserialize attachment record from Redis: {}", e);
                    }
                }
            }
        }

        debug!("Bulk retrieved {} attachment records", map.len());
        Ok(map)
    }

    /// Get an AttachmentRecord by URL (convenience method)
    pub async fn get_by_url(&self, url: &str) -> Result<Option<AttachmentRecord>> {
        let locator = AttachmentLocator::Url(url.to_string());
        self.get(&locator).await
    }

    /// Get all AttachmentRecords with the specified tag using SORT with GET
    pub async fn get_all_by_tag(&self, tag: AttachmentTag) -> Result<Vec<AttachmentRecord>> {
        let mut conn = self.get_connection().await?;

        // Use SORT command with GET pattern to fetch all attachment data in one Redis call
        let results: Vec<String> = redis::cmd("SORT")
            .arg(tag.redis_key())
            .arg("GET")
            .arg("v2_attachment:*->data")
            .query_async(&mut conn)
            .await
            .context("Failed to get attachment records by tag")?;

        let mut records = Vec::new();

        for result in results {
            if result.is_empty() {
                // Skip empty results (can happen if attachment was deleted but tag wasn't cleaned up)
                continue;
            }

            match serde_json::from_str::<AttachmentRecord>(&result) {
                Ok(record) => records.push(record),
                Err(e) => {
                    warn!("Failed to deserialize attachment record from Redis: {}", e);
                    // Continue processing other records
                }
            }
        }

        debug!(
            "Retrieved {} attachment records for tag: {}",
            records.len(),
            tag
        );
        Ok(records)
    }

    /// Change the tag of an attachment atomically
    pub async fn change_tag(
        &self,
        locator: &AttachmentLocator,
        from_tag: AttachmentTag,
        to_tag: AttachmentTag,
    ) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let cache_key = locator.cache_key();

        // Use pipeline for atomic operation
        let mut pipe = redis::pipe();
        pipe.atomic();

        // Remove from old tag
        pipe.srem(from_tag.redis_key(), &cache_key);

        // Add to new tag
        pipe.sadd(to_tag.redis_key(), &cache_key);

        let _: () = pipe
            .query_async(&mut conn)
            .await
            .context("Failed to change tag")?;

        debug!(
            "Changed tag for locator: {:?} from {} to {}",
            locator, from_tag, to_tag
        );
        Ok(())
    }

    /// Update an existing AttachmentRecord (preserving its current tag)
    pub async fn update(&self, record: &AttachmentRecord) -> Result<()> {
        self.update_bulk(std::slice::from_ref(record).to_vec())
            .await
    }

    /// Update multiple existing AttachmentRecords in bulk
    pub async fn update_bulk(&self, records: Vec<AttachmentRecord>) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        let mut conn = self.get_connection().await?;
        let mut pipe = redis::pipe();
        pipe.atomic();

        for record in &records {
            let data =
                serde_json::to_string(record).context("Failed to serialize attachment record")?;
            let key = Self::record_key(&record.locator);
            pipe.hset(&key, "data", &data);
        }

        let _: () = pipe
            .query_async(&mut conn)
            .await
            .context("Failed to bulk update attachment records")?;

        debug!("Bulk updated {} attachment records", records.len());
        Ok(())
    }

    /// Add a new version to an existing attachment record
    pub async fn add_version(
        &self,
        locator: &AttachmentLocator,
        version: AttachmentVersion,
    ) -> Result<()> {
        // Get the current record
        let mut record = match self.get(locator).await? {
            Some(record) => record,
            None => {
                return Err(anyhow::anyhow!(
                    "Attachment record not found for locator: {:?}",
                    locator
                ));
            }
        };

        // Add the new version
        record.add_version(version);

        // Update the record
        self.update(&record).await
    }

    /// Mark an attachment as checked (update last_checked_at)
    pub async fn mark_checked(&self, locator: &AttachmentLocator) -> Result<()> {
        // Get the current record
        let mut record = match self.get(locator).await? {
            Some(record) => record,
            None => {
                return Err(anyhow::anyhow!(
                    "Attachment record not found for locator: {:?}",
                    locator
                ));
            }
        };

        // Mark as checked
        record.mark_checked();

        // Update the record
        self.update(&record).await
    }

    /// List cache keys for a specific tag (without fetching the full record data)
    pub async fn list_cache_keys_by_tag(&self, tag: AttachmentTag) -> Result<Vec<String>> {
        let mut conn = self.get_connection().await?;

        let cache_keys: Vec<String> = conn
            .smembers(tag.redis_key())
            .await
            .context("Failed to list cache keys by tag")?;

        Ok(cache_keys)
    }

    /// Delete an attachment record and remove it from all tags
    pub async fn delete(&self, locator: &AttachmentLocator) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let key = Self::record_key(locator);
        let cache_key = locator.cache_key();

        // Use pipeline for atomic operation
        let mut pipe = redis::pipe();
        pipe.atomic();

        // Delete the attachment record data
        pipe.del(&key);

        // Remove from all possible tags
        for tag in AttachmentTag::all_tags() {
            pipe.srem(tag.redis_key(), &cache_key);
        }

        let _: () = pipe
            .query_async(&mut conn)
            .await
            .context("Failed to delete attachment record")?;

        debug!("Deleted attachment record with locator: {:?}", locator);
        Ok(())
    }

    /// Check if an attachment record exists for the given locator
    pub async fn exists(&self, locator: &AttachmentLocator) -> Result<bool> {
        let mut conn = self.get_connection().await?;
        let key = Self::record_key(locator);

        let exists: bool = conn
            .hexists(&key, "data")
            .await
            .context("Failed to check if attachment record exists")?;

        Ok(exists)
    }

    /// Get count of attachment records for a specific tag
    pub async fn count_by_tag(&self, tag: AttachmentTag) -> Result<usize> {
        let mut conn = self.get_connection().await?;

        let count: usize = conn
            .scard(tag.redis_key())
            .await
            .context("Failed to count attachment records by tag")?;

        Ok(count)
    }

    /// Get all tags that contain a specific locator
    pub async fn get_tags_for_locator(
        &self,
        locator: &AttachmentLocator,
    ) -> Result<Vec<AttachmentTag>> {
        let mut conn = self.get_connection().await?;
        let cache_key = locator.cache_key();
        let mut tags = Vec::new();

        for tag in AttachmentTag::all_tags() {
            let is_member: bool = conn
                .sismember(tag.redis_key(), &cache_key)
                .await
                .context("Failed to check tag membership")?;

            if is_member {
                tags.push(tag);
            }
        }

        Ok(tags)
    }
}

/// Global Redis store instance (following the pattern from existing code)
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
                match store.get_connection().await {
                    Ok(mut conn) => match redis::cmd("PING").query_async::<()>(&mut conn).await {
                        Ok(_) => {
                            *store_guard = Some(store);
                        }
                        Err(e) => {
                            warn!("Failed to ping Redis server: {}", e);
                            return None;
                        }
                    },
                    Err(e) => {
                        warn!("Failed to connect to Redis: {}", e);
                        return None;
                    }
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
