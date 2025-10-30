use chrono::{DateTime, Utc};
use mycorrhiza_common::{file_extension::FileExtension, hash::Blake2bHash};
use non_empty_string::NonEmptyString;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display};
use uuid::Uuid;

use crate::{jurisdiction_schema_mapping::FixedJurisdiction, types::raw::JurisdictionInfo};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, JsonSchema)]
pub enum AttachmentTextQuality {
    #[serde(rename = "low")]
    Low,
    #[serde(rename = "high")]
    High,
}
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
pub struct RawAttachmentText {
    pub quality: AttachmentTextQuality,
    pub language: NonEmptyString,
    pub text: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema, Clone)]
pub struct RawAttachment {
    pub hash: Blake2bHash,
    pub jurisdiction_info: JurisdictionInfo,
    pub name: NonEmptyString,
    pub extension: FileExtension,
    pub text_objects: Vec<RawAttachmentText>,
    pub date_added: chrono::DateTime<Utc>,
    pub date_updated: chrono::DateTime<Utc>,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub extra_metadata: HashMap<String, String>,
    #[serde(default)]
    pub file_size_bytes: u64,
}

// So part of the reason I am wanting to use this kinda weird method for calculating the unique
// location of an attachment is because we want to support multiple kinds of sources of
// attachments, and because we are dealing with government documents, we are going to have to deal
// with things that are not publicly availible but still have a unique location. I dont know what
// of the following we could support, I am listing them as pure hypotheticals right now
// 1) Attachments publicly availible on a government website, accessed via a specific URL.
// 2) Attachments acessible in an application, but require some kind of specific navigation with a
//    playwright browser and or a specific entry of a credential at a previous point.
// 3) Documents gotten via a public records request like FOIA, but are otherwise not availible
//    publicly.
// 4) Government documents that exist, but have some kind of copyright protection or other
//    confidentiality clause that prevents them from being exhibited publicly.
// However, right now I have no clue what of the following applications this attachment system is
// going to handle, and over the next 8 months I know for certain its only going to handle the URL
// encoding methodology, but the entire system with redis and attachment hashes should still work.
//
// Currently the thing I was thinking about is implementing a to_string method for the
// UniversalAttachmentLocator. That would take a URL type, then convert it to url_{url_string}, and
// as a catch all for other methods it could do the following:
// 1) convert the UniversalAttachmentLocator to a Vec<u8> using RKYV (to keep the list of bytes as
//    small as possible)
// 2) base64 encode the list of bytes as a string
// 3) output the following enum_{base64_encoded_rkyv_bytes}
// But I want to know your thoughts on other ways this could be handled. Be brutally honest, does
// this approach not work?

pub enum UniversalAttachmentLocator {
    Url(String),
}

impl Display for UniversalAttachmentLocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UniversalAttachmentLocator::Url(url_content) => write!(f, "url_{url_content}"),
        }
    }
}

// Some hypothetical changes to the way that raw attachments are stored and handled.
pub struct RawAttachmentMetadata {
    pub url: UniversalAttachmentLocator,
    pub current_file_hash: Option<Blake2bHash>,
    pub current_text_hash: Option<Blake2bHash>,
    pub text_generation_time: Option<DateTime<Utc>>,
    pub extension: FileExtension,
    pub last_queried_time: Option<DateTime<Utc>>,
    pub fixed_jurisdiction: FixedJurisdiction,
    pub history: Vec<RawAttachmentHistoryPoint>,
}

pub struct RawAttachmentHistoryPoint {
    pub first_queried_time: DateTime<Utc>,
    pub file_hash: Blake2bHash,
    pub file_text_hash: Option<Blake2bHash>,
    pub text_generation_time: Option<DateTime<Utc>>,
}
