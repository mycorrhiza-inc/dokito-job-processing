use crate::jurisdiction_schema_mapping::FixedJurisdiction;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Attachment tag combining jurisdiction and download status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AttachmentTag {
    pub jurisdiction: FixedJurisdiction,
    pub is_downloaded: bool,
}

impl AttachmentTag {
    /// Create a new attachment tag
    pub fn new(jurisdiction: FixedJurisdiction, is_downloaded: bool) -> Self {
        Self {
            jurisdiction,
            is_downloaded,
        }
    }

    /// Get the Redis key for this tag's set
    pub fn redis_key(&self) -> String {
        format!("tag:{}", self.as_str())
    }

    /// Get the string representation of the tag
    pub fn as_str(&self) -> String {
        let status = if self.is_downloaded { "downloaded" } else { "undownloaded" };
        format!("{}_{}", self.jurisdiction.get_postgres_schema_name(), status)
    }

    /// Parse a tag from string
    pub fn from_str(s: &str) -> Option<Self> {
        if let Some((schema_part, status_part)) = s.rsplit_once('_') {
            let is_downloaded = match status_part {
                "downloaded" => true,
                "undownloaded" => false,
                _ => return None,
            };

            // Find jurisdiction by postgres schema name
            let jurisdiction = FixedJurisdiction::all_jurisdictions()
                .iter()
                .find(|jur| jur.get_postgres_schema_name() == schema_part)
                .copied()?;

            Some(AttachmentTag::new(jurisdiction, is_downloaded))
        } else {
            None
        }
    }

    /// Get all possible tags for all jurisdictions
    pub fn all_tags() -> Vec<AttachmentTag> {
        let mut tags = Vec::new();
        for jurisdiction in FixedJurisdiction::all_jurisdictions() {
            tags.push(AttachmentTag::new(*jurisdiction, true));
            tags.push(AttachmentTag::new(*jurisdiction, false));
        }
        tags
    }

    /// Get tags for a specific jurisdiction
    pub fn tags_for_jurisdiction(jurisdiction: FixedJurisdiction) -> [AttachmentTag; 2] {
        [
            AttachmentTag::new(jurisdiction, true),
            AttachmentTag::new(jurisdiction, false),
        ]
    }
}

impl fmt::Display for AttachmentTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FixedJurisdiction {
    /// Get all fixed jurisdictions (helper method)
    pub fn all_jurisdictions() -> &'static [FixedJurisdiction] {
        &[
            FixedJurisdiction::NewYorkPuc,
            FixedJurisdiction::ColoradoPuc,
            FixedJurisdiction::CaliforniaPuc,
            FixedJurisdiction::UtahDogmCoal,
        ]
    }
}