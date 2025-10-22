use crate::data_processing_traits::{
    DownloadIncomplete, ProcessFrom, Revalidate, RevalidationOutcome,
};
use crate::jurisdiction_schema_mapping::FixedJurisdiction;
use crate::processing::attachments::OpenscrapersExtraData;
use crate::s3_stuff::{DocketAddress, download_openscrapers_object, make_s3_client, upload_object};
use crate::types::raw::JurisdictionInfo;
use crate::types::processed::{ProcessedGenericAttachment, ProcessedGenericDocket};
use crate::types::raw::RawGenericDocket;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::{StreamExt, stream};
use mycorrhiza_common::tasks::{ExecuteUserTask, map_err_as_json};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::info;

pub mod attachments;
pub mod file_fetching;
pub mod llm_prompts;
pub mod match_raw_processed;
pub mod reparse_all;

#[derive(Serialize)]
struct CrimsonPDFIngestParamsS3 {
    s3_uri: String,
}

#[derive(Deserialize, Debug)]
struct CrimsonInitialResponse {
    request_check_leaf: String,
}

#[derive(Deserialize, Debug)]
struct CrimsonStatusResponse {
    completed: bool,
    success: bool,
    markdown: Option<String>,
    error: Option<String>,
}

pub fn make_reflist_of_attachments_without_hash(
    case: &mut ProcessedGenericDocket,
) -> Vec<&mut ProcessedGenericAttachment> {
    let mut case_refs = Vec::with_capacity(case.filings.len());
    for filing in case.filings.iter_mut() {
        for attachment in filing.attachments.iter_mut() {
            if attachment.hash.is_none() {
                case_refs.push(attachment);
            }
        }
    }
    case_refs
}

impl DownloadIncomplete for ProcessedGenericDocket {
    type ExtraData = OpenscrapersExtraData;
    async fn download_incomplete(
        &mut self,
        extra: Self::ExtraData,
    ) -> anyhow::Result<RevalidationOutcome> {
        let _ = self.revalidate().await;
        // info!(govid=%self.case_govid, jurisdiction=%extra.jurisdiction_info.jurisdiction, opened_date = %self.opened_date, uuid = %self.object_uuid,"Attempting to download attachments for docket");
        let attachment_refs = make_reflist_of_attachments_without_hash(self);
        let futures_stream = stream::iter(attachment_refs.into_iter().map(|val| {
            let extra_clone = extra.clone();
            async move {
                DownloadIncomplete::download_incomplete(val, extra_clone).await
            }
        }));
        const CONCURRENT_ATTACHMENTS: usize = 10;
        let change_results = futures_stream
            .buffer_unordered(CONCURRENT_ATTACHMENTS)
            .collect::<Vec<_>>()
            .await;
        let total_change_count = change_results
            .iter()
            .map(|val| match val {
                Ok(RevalidationOutcome::DidChange) => 1,
                _ => 0,
            })
            .sum();
        info!(govid=%self.case_govid, jurisdiction=%extra.jurisdiction_info.jurisdiction, opened_date = %self.opened_date, uuid = %self.object_uuid, attachments_downloaded = %total_change_count,"Successfully downloaded all attachments for docket");
        let did_docket_change = match total_change_count {
            0 => RevalidationOutcome::NoChanges,
            _ => RevalidationOutcome::DidChange,
        };
        Ok(did_docket_change)
    }
}

pub async fn process_case(
    raw_case: RawGenericDocket,
    extra_data: OpenscrapersExtraData,
) -> anyhow::Result<ProcessedGenericDocket> {
    let s3_client = &extra_data.s3_client;
    let jur_info = &extra_data.jurisdiction_info;
    tracing::info!(
        case_num=%raw_case.case_govid,
        state=%jur_info.state,
        jurisdiction=%jur_info.jurisdiction,
        "Finished all attachments, pushing case to db."
    );
    let docket_address = DocketAddress {
        docket_govid: raw_case.case_govid.to_string(),
        jurisdiction: jur_info.to_owned(),
    };
    let s3_result = upload_object(s3_client, &docket_address, &raw_case).await;
    if let Err(err) = s3_result {
        tracing::error!(
            case_num=%raw_case.case_govid, 
            %err, 
            state=%jur_info.state,
            jurisdiction=%jur_info.jurisdiction,
            "Failed to push raw case to S3/DB");
        return Err(err);
    }
    let processed_case_cache =
        download_openscrapers_object::<ProcessedGenericDocket>(s3_client, &docket_address)
            .await
            .ok();

    let mut processed_case =
        ProcessedGenericDocket::process_from(raw_case, processed_case_cache, extra_data.fixed_jurisdiction).await?;
    let _outcome = processed_case.revalidate().await;

    upload_object(s3_client, &docket_address, &processed_case).await?;

    tracing::info!(
        case_num=%processed_case.case_govid,

        state=%jur_info.state,
        jurisdiction=%jur_info.jurisdiction,
        "Successfully pushed case to db."
    );
    Ok(processed_case)
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ReprocessDocketInfo {
    pub docket_govid: String,
    pub jurisdiction: JurisdictionInfo,
    pub only_process_missing: bool,
    pub ignore_cachced_if_older_than: Option<DateTime<Utc>>,
}
#[async_trait]
impl ExecuteUserTask for ReprocessDocketInfo {
    async fn execute_task(self: Box<Self>) -> Result<serde_json::Value, serde_json::Value> {
        let Ok(fixed_jurisdiction) = FixedJurisdiction::try_from(&self.jurisdiction) else {
            return Err(serde_json::Value::String(
                "Jurisdiction did not match one that is stored in the database, aborting."
                    .to_string(),
            ));
        };
        // let self = *self;
        let s3_client = make_s3_client().await;
        let docket_address = DocketAddress {
            jurisdiction: self.jurisdiction,
            docket_govid: self.docket_govid,
        };
        let Ok(raw_case) =
            download_openscrapers_object::<RawGenericDocket>(&s3_client, &docket_address).await
        else {
            return Err("Could not find raw case information".into());
        };
        let mut cached_docket =
            download_openscrapers_object::<ProcessedGenericDocket>(&s3_client, &docket_address)
                .await
                .ok();
        if let Some(skip_date) = self.ignore_cachced_if_older_than
            && let Some(cached_date) = cached_docket.as_ref().map(|d| d.processed_at)
            && cached_date < skip_date
        {
            cached_docket = None;
        }
        if cached_docket.is_some() && self.only_process_missing {
            return Ok("Found cached case, skipping".into());
        };
        let Ok(processed_case) =
            ProcessedGenericDocket::process_from(raw_case, cached_docket, fixed_jurisdiction).await;
        tracing::info!(docket_govid=%processed_case.case_govid,"Successfully processed case");
        let upload_res = upload_object(&s3_client, &docket_address, &processed_case).await;
        map_err_as_json(upload_res)?;
        Ok("Successfully processed task".into())
    }
    fn get_task_label_static() -> &'static str
    where
        Self: Sized,
    {
        "reprocess_case"
    }
    fn get_task_label(&self) -> &'static str {
        "reprocess_case"
    }
}

#[cfg(test)]
mod test;
