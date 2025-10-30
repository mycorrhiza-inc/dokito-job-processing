//! Next-generation attachment types with stable cache keys and history tracking

use chrono::{DateTime, Utc};
use mycorrhiza_common::{file_extension::FileExtension, hash::Blake2bHash};
use non_empty_string::NonEmptyString;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, fmt::Display};

use crate::jurisdiction_schema_mapping::FixedJurisdiction;

// Next-generation attachment types

/// Attachment locator that provides stable cache keys
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, JsonSchema)]
pub enum AttachmentLocator {
    /// Public URL attachment
    Url(String),
}

impl AttachmentLocator {
    /// Generate a stable cache key that won't change when enum variants are added
    pub fn cache_key(&self) -> String {
        match self {
            AttachmentLocator::Url(url) => {
                // Moving this so that the url is directly used to improve debugability
                format!("url:{}", url)
            }
        }
    }

    /// Get the raw URL string if this is a URL locator
    pub fn as_url(&self) -> Option<&str> {
        match self {
            AttachmentLocator::Url(url) => Some(url),
        }
    }
}

/// Complete attachment record with history tracking
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
pub struct AttachmentRecord {
    /// How to locate this attachment
    pub locator: AttachmentLocator,
    /// Jurisdiction this attachment belongs to
    pub jurisdiction: FixedJurisdiction,
    /// Version history (head of vec is current version)
    pub history: Vec<AttachmentVersion>,
    /// When this attachment was first discovered
    pub created_at: DateTime<Utc>,
    /// When this record was last modified
    pub updated_at: DateTime<Utc>,
}

impl AttachmentRecord {
    /// Create a new attachment record
    pub fn new(locator: AttachmentLocator, jurisdiction: FixedJurisdiction) -> Self {
        let now = Utc::now();
        Self {
            locator,
            jurisdiction,
            history: Vec::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Get the current (most recent) version
    pub fn current_version(&self) -> Option<&AttachmentVersion> {
        self.history.last()
    }

    /// Get the current version mutably
    pub fn current_version_mut(&mut self) -> Option<&mut AttachmentVersion> {
        self.history.last_mut()
    }

    /// Add a new version to the history
    pub fn add_version(&mut self, version: AttachmentVersion) {
        self.history.push(version);
        self.updated_at = Utc::now();
    }

    /// Update the last_checked_at time for the current version
    pub fn mark_checked(&mut self) {
        if let Some(current) = self.current_version_mut() {
            current.last_checked_at = Utc::now();
            self.updated_at = Utc::now();
        }
    }

    /// Check if this attachment has been downloaded (has at least one version)
    pub fn is_downloaded(&self) -> bool {
        !self.history.is_empty()
    }

    /// Get how long the current version has existed
    pub fn current_version_age(&self) -> Option<chrono::Duration> {
        self.current_version().map(|v| Utc::now() - v.first_seen_at)
    }

    /// Get how long since the current version was last checked
    pub fn time_since_last_check(&self) -> Option<chrono::Duration> {
        self.current_version()
            .map(|v| Utc::now() - v.last_checked_at)
    }
}

/// A specific version of an attachment's content
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
pub struct AttachmentVersion {
    /// Hash of the file content
    pub content_hash: Blake2bHash,

    /// Hash of extracted text (if any)
    pub text_hash: Option<Blake2bHash>,

    /// When this version was first discovered
    pub first_seen_at: DateTime<Utc>,

    /// When this version was last verified to still be current
    pub last_checked_at: DateTime<Utc>,

    /// Size of the file in bytes
    pub file_size_bytes: u64,

    /// Display name of the attachment
    pub name: NonEmptyString,

    /// File extension
    pub extension: FileExtension,

    /// Additional metadata (server filename, content-type, etc.)
    pub metadata: HashMap<String, String>,
}

impl AttachmentVersion {
    /// Create a new attachment version
    pub fn new(
        content_hash: Blake2bHash,
        file_size_bytes: u64,
        name: NonEmptyString,
        extension: FileExtension,
    ) -> Self {
        let now = Utc::now();
        Self {
            content_hash,
            text_hash: None,
            first_seen_at: now,
            last_checked_at: now,
            file_size_bytes,
            name,
            extension,
            metadata: HashMap::new(),
        }
    }

    /// Set the text hash for this version
    pub fn with_text_hash(mut self, text_hash: Blake2bHash) -> Self {
        self.text_hash = Some(text_hash);
        self
    }

    /// Add metadata to this version
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }

    /// Check if this version is the same content as another
    pub fn same_content(&self, other: &AttachmentVersion) -> bool {
        self.content_hash == other.content_hash
    }
}
