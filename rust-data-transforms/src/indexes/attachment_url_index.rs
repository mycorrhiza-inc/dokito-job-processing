//! Stub for old attachment URL index - now replaced by v2 attachment system

use anyhow::Result;
use async_trait::async_trait;
use mycorrhiza_common::tasks::{ExecuteUserTask, display_error_as_json};
use std::collections::BTreeMap;
use tracing::warn;

use crate::attachments::types::RawAttachment;

pub type AttachIndex = BTreeMap<String, RawAttachment>;

/// Stub for old lookup function - now redirects users to v2 system
pub async fn lookup_hash_from_url(_url: &str) -> Option<RawAttachment> {
    warn!("lookup_hash_from_url is deprecated - use v2 attachment system instead");
    None
}

/// Stub for old cache function - now redirects users to v2 system
pub async fn cache_attachment(_url: &str, _attachment: &RawAttachment) -> Result<()> {
    warn!("cache_attachment is deprecated - use v2 attachment system instead");
    Ok(())
}

/// Regenerate attachment index - now redirects to v2 system
pub async fn regenrate_url_attach_index() -> anyhow::Result<()> {
    warn!("regenrate_url_attach_index is deprecated");
    warn!("Use V2AttachmentProcessor and populate from PostgreSQL instead");
    Ok(())
}

/// Upload provided attachment index - now redirects to v2 system
pub async fn upload_provided_attachment_index(_attach_index: AttachIndex) -> anyhow::Result<()> {
    warn!("upload_provided_attachment_index is deprecated");
    warn!("Use V2AttachmentProcessor and populate from PostgreSQL instead");
    Ok(())
}

#[derive(Default, Clone, Copy)]
pub struct RegenerateUrlAttachIndex {}

#[async_trait]
impl ExecuteUserTask for RegenerateUrlAttachIndex {
    async fn execute_task(self: Box<Self>) -> Result<serde_json::Value, serde_json::Value> {
        let res = regenrate_url_attach_index().await;
        match res {
            Ok(_) => Ok("Task deprecated - use v2 attachment system".into()),
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