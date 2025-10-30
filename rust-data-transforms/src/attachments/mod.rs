//! Next-generation attachment system with Redis storage and history tracking
pub mod downloading_logic;
pub mod redis_store;
pub mod tags;
pub mod types;

// Core attachment system exports
pub use downloading_logic::{AttachmentProcessor, OpenscrapersExtraData};
pub use redis_store::{RedisAttachmentStore, get_redis_store};
pub use tags::AttachmentTag;
pub use types::{
    AttachmentLocator, AttachmentRecord, AttachmentVersion,
};

