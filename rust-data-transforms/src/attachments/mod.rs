//! Redis-based RawAttachment storage with fast URL lookups and tag-based querying
pub mod convenience;
pub mod downloading;
pub mod legacy_bridge;
pub mod redis_store;
pub mod tags;
pub mod types;

pub use convenience::*;
pub use downloading::{OpenscrapersExtraData, DirectAttachmentProcessInfo, DirectAttachmentReturnInfo, process_attachment_with_direct_request};
pub use legacy_bridge::{lookup_hash_from_url, cache_attachment};
pub use redis_store::RedisAttachmentStore;
pub use tags::AttachmentTag;
pub use types::{RawAttachment, RawAttachmentText, AttachmentTextQuality};