use std::collections::HashMap;

use uuid::Uuid;

use crate::types::{
    processed::{ProcessedGenericAttachment, ProcessedGenericFiling},
    raw::{RawGenericAttachment, RawGenericFiling},
};

pub fn match_raw_attaches_to_processed_attaches(
    raw_attaches: Vec<RawGenericAttachment>,
    processed_attaches: Option<Vec<ProcessedGenericAttachment>>,
) -> Vec<(RawGenericAttachment, Option<ProcessedGenericAttachment>)> {
    // the raw generic attachments and the processed attachments each
    let Some(processed_attaches) = processed_attaches else {
        return raw_attaches.into_iter().map(|att| (att, None)).collect();
    };
    let mut processed_attach_map = processed_attaches
        .into_iter()
        .map(|att| (att.object_uuid, att))
        .collect();
    let mut return_vec = Vec::with_capacity(raw_attaches.len());
    for attach in raw_attaches {
        let processed_option = match_individual_attach(&attach, &processed_attach_map);
        if let Some(processed_actual) = processed_option {
            let attach_uuid = processed_actual.object_uuid;
            let proc_attach_owned = processed_attach_map.remove(&attach_uuid);
            return_vec.push((attach, proc_attach_owned))
        } else {
            return_vec.push((attach, None))
        }
    }
    return_vec
}

fn match_individual_attach<'a>(
    raw_attachment: &RawGenericAttachment,
    processed_attaches: &'a HashMap<Uuid, ProcessedGenericAttachment>,
) -> Option<&'a ProcessedGenericAttachment> {
    let govid = &*raw_attachment.attachment_govid;
    if !govid.is_empty() {
        // iterate through the list and find the correct result.
        let found_result = processed_attaches.iter().find_map(|(_, attach)| {
            match attach.attachment_govid == govid {
                false => None,
                true => Some(attach),
            }
        });
        return found_result;
    }
    None
}
pub fn match_raw_filings_to_processed_filings(
    raw_filings: Vec<RawGenericFiling>,
    processed_filings: Option<Vec<ProcessedGenericFiling>>,
) -> Vec<(RawGenericFiling, Option<ProcessedGenericFiling>)> {
    let Some(processed_filings) = processed_filings.filter(|v| !v.is_empty()) else {
        return raw_filings
            .into_iter()
            .map(|filing| (filing, None))
            .collect();
    };
    let mut processed_filing_map = processed_filings
        .into_iter()
        .map(|f| (f.object_uuid, f))
        .collect();
    let mut return_vec = Vec::with_capacity(raw_filings.len());
    for attach in raw_filings {
        let processed_option = match_individual_filing(&attach, &processed_filing_map);

        if let Some(processed_actual) = processed_option {
            let attach_id = processed_actual.object_uuid;
            let proc_attach_owned = processed_filing_map.remove(&attach_id);
            return_vec.push((attach, proc_attach_owned))
        } else {
            return_vec.push((attach, None))
        }
    }
    return_vec
}
fn match_individual_filing<'a>(
    raw_filing: &RawGenericFiling,
    processed_filings: &'a HashMap<Uuid, ProcessedGenericFiling>,
) -> Option<&'a ProcessedGenericFiling> {
    let govid = &*raw_filing.filing_govid;
    if !govid.is_empty() {
        // iterate through the hashmap and find the correct result.
        let found_result = processed_filings.iter().find_map(|(_, filing)| {
            match filing.filing_govid == govid {
                false => None,
                true => Some(filing),
            }
        });
        return found_result;
    }
    None
}
