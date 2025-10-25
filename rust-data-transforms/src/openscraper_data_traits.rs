use std::convert::Infallible;

use crate::types::processed::ProcessedGenericHuman;
use crate::types::raw::{RawArtificalPersonType, RawGenericParty};
use chrono::{NaiveDate, Utc};
use futures::future::join_all;
use futures::join;
use futures_util::{StreamExt, stream};
use non_empty_string::NonEmptyString;
use thiserror::Error;
use tokio::sync::Semaphore;
use tracing::warn;
use uuid::Uuid;

use crate::data_processing_traits::{ProcessFrom, Revalidate, RevalidationOutcome};
use crate::jurisdiction_schema_mapping::FixedJurisdiction;
use crate::processing::llm_prompts::{
    clean_up_organization_name_list, split_and_fix_organization_names_blob,
};
use crate::processing::match_raw_processed::{
    match_raw_attaches_to_processed_attaches, match_raw_filings_to_processed_filings,
};
use crate::sql_ingester_tasks::database_author_association::{
    associate_individual_author_with_name_cached, associate_organization_with_name_cached,
};
use crate::sql_ingester_tasks::dokito_sql_connection::get_dokito_pool;
use crate::sql_ingester_tasks::redis_author_cache::init_redis_client;
use crate::types::processed::{
    ProcessedGenericAttachment, ProcessedGenericDocket, ProcessedGenericFiling,
};
use crate::types::raw::{RawGenericAttachment, RawGenericDocket, RawGenericFiling};

impl Revalidate for ProcessedGenericDocket {
    async fn revalidate(&mut self) -> RevalidationOutcome {
        let mut did_change = RevalidationOutcome::NoChanges;
        if self.object_uuid.is_nil() {
            self.object_uuid = Uuid::new_v4();
            did_change = RevalidationOutcome::DidChange
        }
        if self.case_subtype.is_empty() {
            let x = self.case_type.split(" - ").collect::<Vec<_>>();
            if x.len() == 2 {
                let case_type = x[0].trim().to_string();
                let case_subtype = x[1].trim().to_string();
                self.case_type = case_type;
                self.case_subtype = case_subtype;
                did_change = RevalidationOutcome::DidChange;
            }
        };
        for filing in self.filings.iter_mut() {
            let did_filing_change = filing.revalidate().await;
            did_change = did_change.or(&did_filing_change);
        }
        did_change
    }
}

impl Revalidate for ProcessedGenericFiling {
    async fn revalidate(&mut self) -> RevalidationOutcome {
        let mut did_change = RevalidationOutcome::NoChanges;
        // Chance of this happening is around 1 in 3 quadrillion
        if self.object_uuid.is_nil() {
            self.object_uuid = Uuid::new_v4();
            did_change = RevalidationOutcome::DidChange
        }
        // Name stuff
        if self.name.is_empty() {
            for attach in self.attachments.iter() {
                if !attach.name.is_empty() {
                    self.name = attach.name.clone();
                    did_change = RevalidationOutcome::DidChange;
                    break;
                }
            }
        }

        for attachment in self.attachments.iter_mut() {
            let did_attachment_change = attachment.revalidate().await;
            did_change = did_change.or(&did_attachment_change);
        }
        did_change
    }
}

impl Revalidate for ProcessedGenericAttachment {
    async fn revalidate(&mut self) -> RevalidationOutcome {
        let mut did_change = RevalidationOutcome::NoChanges;
        if self.object_uuid.is_nil() {
            self.object_uuid = Uuid::new_v4();
            did_change = RevalidationOutcome::DidChange
        }
        if self.hash.is_none() && !self.url.is_empty() {
            // Skip hash lookup for process-dockets binary to avoid potential delays
            // This should be handled by the coordination framework instead
            tracing::debug!("Skipping hash lookup for attachment URL in process-dockets binary");
        }
        did_change
    }
}
impl ProcessFrom<RawGenericDocket> for ProcessedGenericDocket {
    type ParseError = Infallible;
    type ExtraData = FixedJurisdiction;
    async fn process_from(
        input: RawGenericDocket,
        cached: Option<Self>,
        fixed_jurisdiction: Self::ExtraData,
    ) -> Result<Self, Self::ParseError> {
        let object_uuid = cached
            .as_ref()
            .map(|v| v.object_uuid)
            .unwrap_or_else(Uuid::new_v4);
        let opened_date_from_filings = {
            let original_date = input.opened_date;
            let mut min_date = original_date;
            for filing_date in input.filings.iter().filter_map(|filing| filing.filed_date) {
                if let Some(real_min_date) = min_date
                    && filing_date < real_min_date
                {
                    min_date = Some(filing_date);
                    if let Some(real_original_date) = original_date {
                        warn!(docket_opened_date =%real_original_date, oldest_date_found=%filing_date,"Found filing with date older then the docket opened date");
                    };
                };
            }
            // This should almost never happen, because the chances of corruption happening on the
            // docket date, and all the filing dates are very small.
            min_date.unwrap_or(NaiveDate::MAX)
        };
        let cached_filings = cached.map(|d| d.filings);
        let matched_filings = match_raw_filings_to_processed_filings(input.filings, cached_filings);

        // Process filings in batches to avoid memory exhaustion and connection pool issues
        // For large dockets (>100 filings), process in smaller batches
        tracing::info!(
            total_filings = matched_filings.len(),
            "Starting filing processing"
        );

        let mut processed_filings = {
            const BATCH_SIZE: usize = 50;
            if matched_filings.len() > BATCH_SIZE {
                let total_batches = (matched_filings.len() + BATCH_SIZE - 1) / BATCH_SIZE; // Round up division

                tracing::info!(
                    total_batches = total_batches,
                    batch_size = BATCH_SIZE,
                    "Processing large docket in batches"
                );
            };
            let mut results = Vec::with_capacity(matched_filings.len());

            // Process in batches of 50 to avoid overwhelming memory/connections
            for (batch_offset, batch) in matched_filings.chunks(BATCH_SIZE).enumerate() {
                let batch_futures = batch
                    .iter()
                    .enumerate()
                    .map(|(idx, (f_raw, f_cached))| {
                        let filing_index_data = IndexExtraData {
                            index: (batch_offset * 50 + idx) as u64,
                            jurisdiction: fixed_jurisdiction,
                        };
                        let f_raw_clone = f_raw.clone();
                        let f_cached_clone = f_cached.clone();
                        async move {
                            let res = ProcessedGenericFiling::process_from(
                                f_raw_clone,
                                f_cached_clone,
                                filing_index_data,
                            )
                            .await;
                            let Ok(val) = res;
                            val
                        }
                    })
                    .collect::<Vec<_>>();

                let mut batch_results = join_all(batch_futures).await;
                results.append(&mut batch_results);

                // Brief pause between batches to allow cleanup
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }

            tracing::info!(
                total_processed = results.len(),
                "Completed all filings with batch processing"
            );
            results
        };

        fn raw_party_to_human(rawparty: RawGenericParty) -> Option<ProcessedGenericHuman> {
            let raw_name = &*rawparty.name;

            // Case 1: not human
            if rawparty.artifical_person_type != RawArtificalPersonType::Human {
                let party_type = rawparty.artifical_person_type;
                warn!(?party_type, %raw_name, "Encountered non-human party, skipping.");
                return None;
            }

            // Case 2: human but name is invalid
            let Ok(nonempty_name) = NonEmptyString::try_from(raw_name.to_string()) else {
                let party_type = rawparty.artifical_person_type;
                let first_name = &*rawparty.western_human_first_name;
                let last_name = &*rawparty.western_human_last_name;
                warn!(
                    ?party_type,
                    %raw_name,
                    %first_name,
                    %last_name,
                    "Encountered human with invalid or missing name."
                );
                return None;
            };

            let final_title = rawparty.human_title;

            let processed_party_huamn = ProcessedGenericHuman {
                object_uuid: Uuid::nil(),
                human_name: nonempty_name,
                western_first_name: rawparty.western_human_last_name,
                western_last_name: rawparty.western_human_first_name,
                contact_emails: vec![rawparty.contact_email],
                contact_phone_numbers: vec![rawparty.contact_phone],
                contact_addresses: vec![rawparty.contact_address],
                representing_company: None,
                employed_by: None,
                title: final_title,
            };
            Some(processed_party_huamn)
        }
        let actual_industry = if input.industry.starts_with("Matter Number:") {
            "".to_string()
        } else {
            input.industry
        };
        let raw_parties = input.case_parties;
        let raw_parties_length = raw_parties.len();
        tracing::info!(%raw_parties_length,"Raw Case has a certain amount of input.case_parites");

        let mut processed_parties = raw_parties
            .into_iter()
            .filter_map(raw_party_to_human)
            .collect::<Vec<_>>();
        let pool = get_dokito_pool().await.unwrap();

        // Initialize Redis cache if not already done
        let _ = init_redis_client().await;

        // Concurrent processing of parties using cached lookups
        let party_futures = processed_parties.iter_mut().map(|party| {
            associate_individual_author_with_name_cached(party, fixed_jurisdiction, pool)
        });
        let _party_results = join_all(party_futures).await;
        // Note: processed_parties may be fewer than raw_parties due to filtering
        // Only human parties are kept, so this assertion is removed
        tracing::debug!(
            raw_count = raw_parties_length,
            processed_count = processed_parties.len(),
            "Party processing completed"
        );
        processed_filings.sort_by_key(|v| v.index_in_docket);
        let llmed_petitioner_list = if !input.petitioner.trim().is_empty() {
            let raw_petitioner = &*input.petitioner;
            split_and_fix_organization_names_blob(raw_petitioner).await
        } else {
            vec![]
        };
        let final_processed_docket = ProcessedGenericDocket {
            object_uuid,
            case_parties: processed_parties,
            processed_at: Utc::now(),
            case_govid: input.case_govid,
            filings: processed_filings,
            opened_date: opened_date_from_filings,
            case_name: input.case_name,
            case_url: input.case_url,
            industry: actual_industry,
            case_type: input.case_type,
            case_subtype: input.case_subtype,
            indexed_at: input.indexed_at,
            closed_date: input.closed_date,
            description: input.description,
            extra_metadata: input.extra_metadata,
            hearing_officer: input.hearing_officer,
            petitioner_list: llmed_petitioner_list,
        };
        Ok(final_processed_docket)
    }
}

#[derive(Error, Debug)]
enum ProcessError {
    #[error("Encountered error syncing data with postgres.")]
    PostgresError,
}

// Semaphore to limit concurrent file processing to prevent resource exhaustion
static GLOBAL_SIMULTANEOUS_FILE_PROCESSING: Semaphore = Semaphore::const_new(25);
// Semaphore to limit concurrent LLM calls
pub(crate) static GLOBAL_SIMULTANEOUS_LLM_CALLS: Semaphore = Semaphore::const_new(10);

fn processed_human_from_blob_name(name: &str) -> Option<ProcessedGenericHuman> {
    // Trim and ensure the name isn't empty
    let trimmed = name.trim();
    let nonempty_name = NonEmptyString::try_from(trimmed).ok()?;

    // Split by whitespace
    let mut parts = trimmed.split_whitespace();
    let first = parts.next().unwrap_or("");
    let last = parts.next().unwrap_or("");

    // If there is only one part, treat both as empty
    let (first_name, last_name) = if last.is_empty() {
        ("".to_string(), "".to_string())
    } else {
        (first.to_string(), last.to_string())
    };

    Some(ProcessedGenericHuman {
        human_name: nonempty_name,
        contact_emails: Vec::new(),
        contact_phone_numbers: Vec::new(),
        contact_addresses: Vec::new(),
        object_uuid: Uuid::nil(),
        representing_company: None,
        western_first_name: first_name,
        western_last_name: last_name,
        employed_by: None,
        title: "".to_string(),
    })
}
impl ProcessFrom<RawGenericFiling> for ProcessedGenericFiling {
    type ParseError = Infallible;
    type ExtraData = IndexExtraData;
    async fn process_from(
        input: RawGenericFiling,
        cached: Option<Self>,
        index_data: Self::ExtraData,
    ) -> Result<Self, Self::ParseError> {
        let _permit = GLOBAL_SIMULTANEOUS_FILE_PROCESSING.acquire().await.unwrap();

        tracing::info!(
            filing_index = index_data.index,
            filing_id = %input.filing_govid,
            attachments_count = input.attachments.len(),
            org_authors_count = input.organization_authors.len(),
            org_blob_length = input.organization_authors_blob.len(),
            individual_authors_count = input.individual_authors.len(),
            "Processing filing"
        );
        let object_uuid = cached
            .as_ref()
            .map(|v| v.object_uuid)
            .unwrap_or_else(Uuid::new_v4);
        let _pg_pool = get_dokito_pool().await.unwrap_or_else(|e| {
            panic!("Cannot proceed without database connection: {e}");
        });
        let (processed_attach_map, cached_orgauthorlist, cached_individualauthorllist) =
            match cached {
                Some(filing) => (
                    Some(filing.attachments),
                    Some(filing.organization_authors),
                    Some(filing.individual_authors),
                ),
                None => (None, None, None),
            };

        let matched_attach_list =
            match_raw_attaches_to_processed_attaches(input.attachments, processed_attach_map);
        // Async match the raw attachments with the cached versions, and process them async 5 at a
        // time.
        let mut processed_attachments = stream::iter(matched_attach_list.into_iter())
            .enumerate()
            .map(|(attach_index, (raw_attach, cached_attach))| {
                let attach_index_data = IndexExtraData {
                    index: attach_index as u64,
                    jurisdiction: index_data.jurisdiction,
                };
                async {
                    let res = ProcessedGenericAttachment::process_from(
                        raw_attach,
                        cached_attach,
                        attach_index_data,
                    )
                    .await;
                    let Ok(val) = res;
                    val
                }
            })
            .buffer_unordered(5)
            .collect::<Vec<_>>()
            .await;
        processed_attachments.sort_by_key(|att| att.index_in_filing);
        // Process org and individual author names.
        let mut organization_authors = {
            if let Some(org_authors) = cached_orgauthorlist {
                org_authors
            } else if input.organization_authors.is_empty() {
                vec![]
            } else {
                clean_up_organization_name_list(input.organization_authors)
            }
        };

        let mut individual_authors = {
            if let Some(cached_authors) = cached_individualauthorllist {
                cached_authors
            } else if input.individual_authors.is_empty() {
                vec![]
            } else {
                input
                    .individual_authors
                    .iter()
                    .filter_map(|name| processed_human_from_blob_name(name))
                    .collect()
            }
        };
        let fixed_jur = index_data.jurisdiction;
        let pool = get_dokito_pool().await.unwrap();

        tracing::info!(
            filing_index = index_data.index,
            org_authors_final = organization_authors.len(),
            individual_authors_final = individual_authors.len(),
            "Starting DB author association"
        );

        let org_futures = organization_authors
            .iter_mut()
            .map(|org| associate_organization_with_name_cached(org, fixed_jur, pool));
        let human_futures = individual_authors
            .iter_mut()
            .map(|human| associate_individual_author_with_name_cached(human, fixed_jur, pool));
        let _res = join!(join_all(org_futures), join_all(human_futures));

        let proc_filing = Self {
            object_uuid,
            filed_date: input.filed_date,
            index_in_docket: index_data.index,
            attachments: processed_attachments,
            name: input.name.clone(),
            filing_govid: input.filing_govid.clone(),
            filing_url: input.filing_url.clone(),
            filing_type: input.filing_type.clone(),
            description: input.description.clone(),
            // Super hacky workaround until I can change the input type.
            extra_metadata: input.extra_metadata.clone().into_iter().collect(),
            organization_authors,
            individual_authors,
        };
        Ok(proc_filing)
    }
}

pub struct IndexExtraData {
    index: u64,
    jurisdiction: FixedJurisdiction,
}
impl ProcessFrom<RawGenericAttachment> for ProcessedGenericAttachment {
    type ParseError = Infallible;
    type ExtraData = IndexExtraData;
    async fn process_from(
        input: RawGenericAttachment,
        cached: Option<Self>,
        index_data: Self::ExtraData,
    ) -> Result<Self, Self::ParseError> {
        let uuid = cached
            .as_ref()
            .map(|val| val.object_uuid)
            .unwrap_or_else(Uuid::new_v4);
        let hash = (input.hash).or_else(|| cached.and_then(|v| v.hash));
        let return_res = Self {
            object_uuid: uuid,
            index_in_filing: index_data.index,
            name: input.name.clone(),
            document_extension: input.document_extension.clone(),
            attachment_govid: input.attachment_govid.clone(),
            attachment_type: input.attachment_type.clone(),
            attachment_subtype: input.attachment_subtype.clone(),
            url: input.url.clone(),
            extra_metadata: input.extra_metadata.clone().into_iter().collect(),
            hash,
        };
        Ok(return_res)
    }
}
