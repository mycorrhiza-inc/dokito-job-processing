//! Legacy bridge to maintain compatibility with old attachment_url_index code

use super::{AttachmentTag, RedisAttachmentStore};
use crate::jurisdiction_schema_mapping::FixedJurisdiction;
use crate::attachments::types::RawAttachment;
use anyhow::Result;
use tracing::warn;

/// Compatibility function for old `lookup_hash_from_url` function
/// This bridges the old caching system to the new Redis-based tag system
pub async fn lookup_hash_from_url(url: &str) -> Option<RawAttachment> {
    let store = match RedisAttachmentStore::new() {
        Ok(store) => store,
        Err(e) => {
            warn!("Failed to create Redis store for lookup: {}", e);
            return None;
        }
    };

    match store.get(url).await {
        Ok(Some(attachment)) => Some(attachment),
        Ok(None) => None,
        Err(e) => {
            warn!("Failed to lookup attachment from Redis: {}", e);
            None
        }
    }
}

/// Compatibility function for old `cache_attachment` function
/// This bridges the old caching system to the new Redis-based tag system
///
/// Since the old function didn't specify jurisdiction, we'll try to determine it from the attachment
/// or default to a reasonable fallback based on the attachment's jurisdiction_info
pub async fn cache_attachment(_url: &str, attachment: &RawAttachment) -> Result<()> {
    let store = RedisAttachmentStore::new()?;

    // Try to determine the jurisdiction from the attachment's jurisdiction_info
    let jurisdiction = match FixedJurisdiction::try_from(&attachment.jurisdiction_info) {
        Ok(jur) => jur,
        Err(_) => {
            warn!(
                "Could not determine FixedJurisdiction from attachment jurisdiction_info, defaulting to NewYorkPuc"
            );
            FixedJurisdiction::NewYorkPuc
        }
    };

    // Since we're caching an attachment, assume it's downloaded
    let tag = AttachmentTag::new(jurisdiction, true);
    store.store(attachment, tag).await?;

    Ok(())
}

