//! Convenience functions for common attachment operations that integrate with existing processing code

use super::{AttachmentTag, RedisAttachmentStore};
use crate::jurisdiction_schema_mapping::FixedJurisdiction;
use crate::attachments::types::RawAttachment;
use anyhow::Result;
use tracing::warn;

/// Store an attachment and mark it as undownloaded for the given jurisdiction
pub async fn store_undownloaded_attachment(
    attachment: &RawAttachment,
    jurisdiction: FixedJurisdiction,
) -> Result<()> {
    let store = match RedisAttachmentStore::new() {
        Ok(store) => store,
        Err(e) => {
            warn!("Failed to create Redis store: {}", e);
            return Ok(()); // Gracefully handle Redis unavailability
        }
    };

    let tag = AttachmentTag::new(jurisdiction, false);
    store.store(attachment, tag).await
}

/// Mark an attachment as downloaded for the given jurisdiction
pub async fn mark_attachment_downloaded(
    url: &str,
    jurisdiction: FixedJurisdiction,
) -> Result<()> {
    let store = match RedisAttachmentStore::new() {
        Ok(store) => store,
        Err(e) => {
            warn!("Failed to create Redis store: {}", e);
            return Ok(()); // Gracefully handle Redis unavailability
        }
    };

    let from_tag = AttachmentTag::new(jurisdiction, false);
    let to_tag = AttachmentTag::new(jurisdiction, true);

    store.change_tag(url, from_tag, to_tag).await
}

/// Get all undownloaded attachments for a jurisdiction
pub async fn get_undownloaded_attachments(
    jurisdiction: FixedJurisdiction,
) -> Result<Vec<RawAttachment>> {
    let store = match RedisAttachmentStore::new() {
        Ok(store) => store,
        Err(e) => {
            warn!("Failed to create Redis store: {}", e);
            return Ok(Vec::new()); // Return empty vec if Redis unavailable
        }
    };

    let tag = AttachmentTag::new(jurisdiction, false);
    store.get_all_by_tag(tag).await
}

/// Get all downloaded attachments for a jurisdiction
pub async fn get_downloaded_attachments(
    jurisdiction: FixedJurisdiction,
) -> Result<Vec<RawAttachment>> {
    let store = match RedisAttachmentStore::new() {
        Ok(store) => store,
        Err(e) => {
            warn!("Failed to create Redis store: {}", e);
            return Ok(Vec::new()); // Return empty vec if Redis unavailable
        }
    };

    let tag = AttachmentTag::new(jurisdiction, true);
    store.get_all_by_tag(tag).await
}

/// Get attachment statistics for a jurisdiction
pub async fn get_attachment_stats(jurisdiction: FixedJurisdiction) -> Result<AttachmentStats> {
    let store = match RedisAttachmentStore::new() {
        Ok(store) => store,
        Err(e) => {
            warn!("Failed to create Redis store: {}", e);
            return Ok(AttachmentStats::default()); // Return default stats if Redis unavailable
        }
    };

    let downloaded_tag = AttachmentTag::new(jurisdiction, true);
    let undownloaded_tag = AttachmentTag::new(jurisdiction, false);

    let downloaded_count = store.count_by_tag(downloaded_tag).await.unwrap_or(0);
    let undownloaded_count = store.count_by_tag(undownloaded_tag).await.unwrap_or(0);

    Ok(AttachmentStats {
        jurisdiction,
        downloaded_count,
        undownloaded_count,
        total_count: downloaded_count + undownloaded_count,
    })
}

/// Statistics for attachments in a jurisdiction
#[derive(Debug, Clone)]
pub struct AttachmentStats {
    pub jurisdiction: FixedJurisdiction,
    pub downloaded_count: usize,
    pub undownloaded_count: usize,
    pub total_count: usize,
}

impl Default for AttachmentStats {
    fn default() -> Self {
        Self {
            jurisdiction: FixedJurisdiction::NewYorkPuc, // Default jurisdiction
            downloaded_count: 0,
            undownloaded_count: 0,
            total_count: 0,
        }
    }
}

/// Get statistics for all jurisdictions
pub async fn get_all_attachment_stats() -> Result<Vec<AttachmentStats>> {
    let mut stats = Vec::new();

    for jurisdiction in FixedJurisdiction::all_jurisdictions() {
        match get_attachment_stats(*jurisdiction).await {
            Ok(stat) => stats.push(stat),
            Err(e) => {
                warn!("Failed to get stats for jurisdiction {:?}: {}", jurisdiction, e);
            }
        }
    }

    Ok(stats)
}

/// Check if an attachment exists and get its download status for a jurisdiction
pub async fn get_attachment_status(
    url: &str,
    jurisdiction: FixedJurisdiction,
) -> Result<Option<bool>> {
    let store = match RedisAttachmentStore::new() {
        Ok(store) => store,
        Err(e) => {
            warn!("Failed to create Redis store: {}", e);
            return Ok(None);
        }
    };

    if !store.exists(url).await? {
        return Ok(None);
    }

    let tags = store.get_tags_for_url(url).await?;

    // Find the tag for this jurisdiction
    for tag in tags {
        if tag.jurisdiction == jurisdiction {
            return Ok(Some(tag.is_downloaded));
        }
    }

    Ok(None)
}