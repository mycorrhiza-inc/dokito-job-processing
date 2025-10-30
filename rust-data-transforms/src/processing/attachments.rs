//! Re-exports for attachment processing functionality
//! This module now points to the new attachments directory structure

// Legacy re-exports for compatibility
pub use crate::attachments::{
    DirectAttachmentProcessInfo, DirectAttachmentReturnInfo, OpenscrapersExtraData,
    process_attachment_with_direct_request,
};

