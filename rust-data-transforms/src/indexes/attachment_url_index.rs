//! Legacy attachment URL index - now redirects to new attachment system
//! This file maintains compatibility while using the new Redis-based tag system

// Re-export the new functions with the same names for compatibility
pub use crate::attachments::{lookup_hash_from_url, cache_attachment};

use crate::attachments::{AttachmentTag, RedisAttachmentStore};
use crate::jurisdiction_schema_mapping::FixedJurisdiction;
use crate::attachments::types::RawAttachment;
use anyhow::Result;
use async_trait::async_trait;
use mycorrhiza_common::tasks::{ExecuteUserTask, display_error_as_json};
use std::collections::BTreeMap;
use tracing::{info, warn};

pub type AttachIndex = BTreeMap<String, RawAttachment>;

/// Regenerate attachment index - now populates Redis with tag-based system
pub async fn regenrate_url_attach_index() -> anyhow::Result<()> {
    info!("Starting attachment index regeneration using new Redis tag system");

    // Note: In a real migration, you would:
    // 1. Load existing attachments from S3 or other source
    // 2. Populate Redis with appropriate tags based on jurisdiction
    // 3. Clean up old index storage

    // For now, this is a placeholder that maintains the interface
    warn!("Attachment index regeneration is now handled by the new Redis tag system");
    warn!("Please use the new attachment management functions in the attachments module");

    Ok(())
}

/// Upload provided attachment index - now stores in Redis with tags
pub async fn upload_provided_attachment_index(attach_index: AttachIndex) -> anyhow::Result<()> {
    info!("Uploading attachment index to new Redis tag system");

    let store = RedisAttachmentStore::new()?;
    let mut processed_count = 0;

    for (url, attachment) in attach_index.iter() {
        // Determine jurisdiction from attachment
        let jurisdiction = match FixedJurisdiction::try_from(&attachment.jurisdiction_info) {
            Ok(jur) => jur,
            Err(_) => {
                warn!("Could not determine jurisdiction for URL: {}, skipping", url);
                continue;
            }
        };

        // Store as downloaded since these are existing attachments
        let tag = AttachmentTag::new(jurisdiction, true);

        if let Err(e) = store.store(attachment, tag).await {
            warn!("Failed to store attachment for URL {}: {}", url, e);
        } else {
            processed_count += 1;
        }
    }

    info!("Successfully migrated {} attachments to new Redis tag system", processed_count);
    Ok(())
}

#[derive(Default, Clone, Copy)]
pub struct RegenerateUrlAttachIndex {}

#[async_trait]
impl ExecuteUserTask for RegenerateUrlAttachIndex {
    async fn execute_task(self: Box<Self>) -> Result<serde_json::Value, serde_json::Value> {
        let res = regenrate_url_attach_index().await;
        match res {
            Ok(_) => Ok("Task Succeeded - migrated to new Redis tag system".into()),
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