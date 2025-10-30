//! Stub for old attachment URL index - now replaced by v2 attachment system

use anyhow::Result;
use async_trait::async_trait;
use mycorrhiza_common::tasks::{ExecuteUserTask, display_error_as_json};
use tracing::warn;

// All attachment index functions removed - use Redis-based attachment system instead

/// Regenerate attachment index - now redirects to v2 system
pub async fn regenrate_url_attach_index() -> anyhow::Result<()> {
    warn!("regenrate_url_attach_index is deprecated");
    warn!("Use AttachmentProcessor and populate from PostgreSQL instead");
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