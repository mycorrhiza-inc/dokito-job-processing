use anyhow::{Context, Result};
use redis::{AsyncCommands, Client};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::env;
use std::sync::OnceLock;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::jurisdiction_schema_mapping::FixedJurisdiction;

static REDIS_CLIENT: OnceLock<Mutex<Option<Client>>> = OnceLock::new();

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedHuman {
    pub uuid: Uuid,
    pub western_first_name: String,
    pub western_last_name: String,
    pub contact_emails: Vec<String>,
    pub contact_phone_numbers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedOrganization {
    pub uuid: Uuid,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuplicateTracker {
    pub canonical_uuid: Uuid,
    pub duplicate_uuids: Vec<Uuid>,
}

/// Initialize Redis connection from environment variables
/// Falls back gracefully if Redis is unavailable
pub async fn init_redis_client() -> Result<()> {
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

    match Client::open(redis_url.as_str()) {
        Ok(client) => {
            // Test the connection
            if let Ok(mut conn) = client.get_multiplexed_async_connection().await {
                let _: String = redis::cmd("PING").query_async(&mut conn).await.context("Redis ping failed")?;
                tracing::info!("Redis connection established successfully");

                let client_mutex = REDIS_CLIENT.get_or_init(|| Mutex::new(None));
                *client_mutex.lock().await = Some(client);
                Ok(())
            } else {
                tracing::warn!("Redis connection failed, falling back to database-only mode");
                Ok(())
            }
        }
        Err(e) => {
            tracing::warn!("Redis client creation failed: {}, falling back to database-only mode", e);
            Ok(())
        }
    }
}

/// Get Redis connection if available
async fn get_redis_connection() -> Option<redis::aio::MultiplexedConnection> {
    let client_mutex = REDIS_CLIENT.get_or_init(|| Mutex::new(None));
    let client_guard = client_mutex.lock().await;

    if let Some(client) = client_guard.as_ref() {
        client.get_multiplexed_async_connection().await.ok()
    } else {
        None
    }
}

/// Generate consistent cache key for human names
fn human_cache_key(name: &str, jurisdiction: FixedJurisdiction) -> String {
    let mut hasher = Sha256::new();
    hasher.update(name.as_bytes());
    let name_hash = hex::encode(hasher.finalize());
    format!("human:{}:{}", jurisdiction.get_postgres_schema_name(), name_hash)
}

/// Generate consistent cache key for organization names
fn org_cache_key(name: &str, jurisdiction: FixedJurisdiction) -> String {
    let mut hasher = Sha256::new();
    hasher.update(name.as_bytes());
    let name_hash = hex::encode(hasher.finalize());
    format!("org:{}:{}", jurisdiction.get_postgres_schema_name(), name_hash)
}

/// Generate cache key for duplicate tracking
fn duplicate_key(name: &str, jurisdiction: FixedJurisdiction, entity_type: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(name.as_bytes());
    let name_hash = hex::encode(hasher.finalize());
    format!("{}dedup:{}:{}", entity_type, jurisdiction.get_postgres_schema_name(), name_hash)
}

/// Cache TTL in seconds (1 hour)
const CACHE_TTL: i64 = 3600;

/// Get cached human by name
pub async fn get_cached_human(name: &str, jurisdiction: FixedJurisdiction) -> Option<CachedHuman> {
    let mut conn = get_redis_connection().await?;
    let key = human_cache_key(name, jurisdiction);

    match conn.get::<_, String>(&key).await {
        Ok(cached_data) => {
            match serde_json::from_str::<CachedHuman>(&cached_data) {
                Ok(human) => {
                    tracing::debug!("Cache hit for human: {}", name);
                    Some(human)
                }
                Err(e) => {
                    tracing::warn!("Failed to deserialize cached human {}: {}", name, e);
                    None
                }
            }
        }
        Err(_) => {
            tracing::debug!("Cache miss for human: {}", name);
            None
        }
    }
}

/// Cache human data
pub async fn cache_human(human: &CachedHuman, name: &str, jurisdiction: FixedJurisdiction) -> Result<()> {
    let mut conn = match get_redis_connection().await {
        Some(conn) => conn,
        None => return Ok(()), // Gracefully handle Redis unavailability
    };

    let key = human_cache_key(name, jurisdiction);
    let serialized = serde_json::to_string(human)?;

    let _: () = conn.set_ex(&key, &serialized, CACHE_TTL as u64).await
        .context("Failed to cache human data")?;

    tracing::debug!("Cached human: {}", name);
    Ok(())
}

/// Get cached organization by name
pub async fn get_cached_organization(name: &str, jurisdiction: FixedJurisdiction) -> Option<CachedOrganization> {
    let mut conn = get_redis_connection().await?;
    let key = org_cache_key(name, jurisdiction);

    match conn.get::<_, String>(&key).await {
        Ok(cached_data) => {
            match serde_json::from_str::<CachedOrganization>(&cached_data) {
                Ok(org) => {
                    tracing::debug!("Cache hit for organization: {}", name);
                    Some(org)
                }
                Err(e) => {
                    tracing::warn!("Failed to deserialize cached organization {}: {}", name, e);
                    None
                }
            }
        }
        Err(_) => {
            tracing::debug!("Cache miss for organization: {}", name);
            None
        }
    }
}

/// Cache organization data
pub async fn cache_organization(org: &CachedOrganization, name: &str, jurisdiction: FixedJurisdiction) -> Result<()> {
    let mut conn = match get_redis_connection().await {
        Some(conn) => conn,
        None => return Ok(()), // Gracefully handle Redis unavailability
    };

    let key = org_cache_key(name, jurisdiction);
    let serialized = serde_json::to_string(org)?;

    let _: () = conn.set_ex(&key, &serialized, CACHE_TTL as u64).await
        .context("Failed to cache organization data")?;

    tracing::debug!("Cached organization: {}", name);
    Ok(())
}

/// Get duplicate tracking info
pub async fn get_duplicate_tracker(name: &str, jurisdiction: FixedJurisdiction, entity_type: &str) -> Option<DuplicateTracker> {
    let mut conn = get_redis_connection().await?;
    let key = duplicate_key(name, jurisdiction, entity_type);

    match conn.get::<_, String>(&key).await {
        Ok(cached_data) => {
            match serde_json::from_str::<DuplicateTracker>(&cached_data) {
                Ok(tracker) => Some(tracker),
                Err(e) => {
                    tracing::warn!("Failed to deserialize duplicate tracker for {}: {}", name, e);
                    None
                }
            }
        }
        Err(_) => None
    }
}

/// Cache duplicate tracking info
pub async fn cache_duplicate_tracker(tracker: &DuplicateTracker, name: &str, jurisdiction: FixedJurisdiction, entity_type: &str) -> Result<()> {
    let mut conn = match get_redis_connection().await {
        Some(conn) => conn,
        None => return Ok(()), // Gracefully handle Redis unavailability
    };

    let key = duplicate_key(name, jurisdiction, entity_type);
    let serialized = serde_json::to_string(tracker)?;

    let _: () = conn.set_ex(&key, &serialized, CACHE_TTL as u64).await
        .context("Failed to cache duplicate tracker")?;

    Ok(())
}

/// Clear all cached data for a jurisdiction (useful for testing)
pub async fn clear_jurisdiction_cache(jurisdiction: FixedJurisdiction) -> Result<()> {
    let mut conn = match get_redis_connection().await {
        Some(conn) => conn,
        None => return Ok(()),
    };

    let pattern = format!("*:{}:*", jurisdiction.get_postgres_schema_name());
    let keys: Vec<String> = conn.keys(&pattern).await?;

    if !keys.is_empty() {
        let _: () = conn.del(&keys).await?;
        tracing::info!("Cleared {} cache entries for jurisdiction {}", keys.len(), jurisdiction.get_postgres_schema_name());
    }

    Ok(())
}