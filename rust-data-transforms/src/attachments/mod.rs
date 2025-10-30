//! Next-generation attachment system with Redis storage and history tracking
pub mod downloading;
pub mod tags;
pub mod types;

// V2 attachment system modules
pub mod v2_types;
pub mod v2_redis_store;
pub mod v2_downloading;

// Legacy types for S3 compatibility
pub use types::{RawAttachment, RawAttachmentText, AttachmentTextQuality};
pub use tags::AttachmentTag;

// Legacy downloading functionality (still needed by processing layer)
pub use downloading::{OpenscrapersExtraData, DirectAttachmentProcessInfo, DirectAttachmentReturnInfo, process_attachment_with_direct_request};

// V2 system exports
pub use v2_types::{AttachmentLocator, AttachmentRecord, AttachmentVersion};
pub use v2_redis_store::{V2RedisAttachmentStore, get_v2_redis_store};
pub use v2_downloading::{V2AttachmentProcessor, V2OpenscrapersExtraData};