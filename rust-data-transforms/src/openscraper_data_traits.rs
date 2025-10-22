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
use crate::indexes::attachment_url_index::lookup_hash_from_url;
use crate::jurisdiction_schema_mapping::FixedJurisdiction;
use crate::processing::llm_prompts::{
    clean_up_organization_name_list, split_and_fix_organization_names_blob,
};
use crate::processing::match_raw_processed::{
    match_raw_attaches_to_processed_attaches, match_raw_fillings_to_processed_fillings,
};
use crate::sql_ingester_tasks::database_author_association::{
    associate_individual_author_with_name, associate_organization_with_name,
};
use crate::sql_ingester_tasks::dokito_sql_connection::get_dokito_pool;
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
        for filling in self.filings.iter_mut() {
            let did_filling_change = filling.revalidate().await;
            did_change = did_change.or(&did_filling_change);
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
            let url = &*self.url;
            let opt_raw_attach = lookup_hash_from_url(url).await;
            if let Some(raw_attach) = opt_raw_attach {
                self.hash = Some(raw_attach.hash);
            }
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
        let opened_date_from_fillings = {
            let original_date = input.opened_date;
            let mut min_date = original_date;
            for filling_date in input
                .filings
                .iter()
                .filter_map(|filling| filling.filed_date)
            {
                if let Some(real_min_date) = min_date
                    && filling_date < real_min_date
                {
                    min_date = Some(filling_date);
                    if let Some(real_original_date) = original_date {
                        warn!(docket_opened_date =%real_original_date, oldest_date_found=%filling_date,"Found filling with date older then the docket opened date");
                    };
                };
            }
            // This should almost never happen, because the chances of corruption happening on the
            // docket date, and all the filling dates are very small.
            min_date.unwrap_or(NaiveDate::MAX)
        };
        let cached_fillings = cached.map(|d| d.filings);
        let matched_fillings =
            match_raw_fillings_to_processed_fillings(input.filings, cached_fillings);
        let processed_fillings_futures =
            matched_fillings
                .into_iter()
                .enumerate()
                .map(async |(index, (f_raw, f_cached))| {
                    let filling_index_data = IndexExtraData {
                        index: index as u64,
                        jurisdiction: fixed_jurisdiction,
                    };
                    let res =
                        ProcessedGenericFiling::process_from(f_raw, f_cached, filling_index_data)
                            .await;
                    let Ok(val) = res;
                    val
                });
        // Everything gets processed at once since the limiting factor on fillings is global. This
        // is to make it so that it doesnt overwhelm the system trying to process 5 dockets with
        // 10,000 fillings, but it can process 60 dockets at the same time with one filling each.
        let mut processed_fillings = join_all(processed_fillings_futures).await;

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
        tracing::info!(case_parties_length = %processed_parties.len(),"Processed parties has final length");
        let pool = get_dokito_pool().await.unwrap();

        // TODO: This could be made concurrent if its a bottleneck
        for party in processed_parties.iter_mut() {
            let _res = associate_individual_author_with_name(party, fixed_jurisdiction, pool).await;
        }
        assert_eq!(
            raw_parties_length,
            processed_parties.len(),
            "raw parties should have the same length as the final parties"
        );
        processed_fillings.sort_by_key(|v| v.index_in_docket);
        let llmed_petitioner_list = split_and_fix_organization_names_blob(&input.petitioner).await;
        let final_processed_docket = ProcessedGenericDocket {
            object_uuid,
            case_parties: processed_parties,
            processed_at: Utc::now(),
            case_govid: input.case_govid,
            filings: processed_fillings,
            opened_date: opened_date_from_fillings,
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

// TODO: Might be a good idea to have a semaphore for each
static GLOBAL_SIMULTANEOUS_FILE_PROCESSING: Semaphore = Semaphore::const_new(50);

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
        let object_uuid = cached
            .as_ref()
            .map(|v| v.object_uuid)
            .unwrap_or_else(Uuid::new_v4);
        let _pg_pool = get_dokito_pool().await.unwrap_or_else(|e| {
            eprintln!("FATAL: Failed to initialize database connection during processing");
            eprintln!("Error: {:?}", e);
            eprintln!("This is likely due to missing or incorrect database environment variables:");
            eprintln!("  - POSTGRES_CONNECTION or DATABASE_URL must be set");
            eprintln!("  - Database must be accessible and credentials must be correct");
            panic!("Cannot proceed without database connection");
        });
        let (processed_attach_map, cached_orgauthorlist, cached_individualauthorllist) =
            match cached {
                Some(filling) => (
                    Some(filling.attachments),
                    Some(filling.organization_authors),
                    Some(filling.individual_authors),
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
        processed_attachments.sort_by_key(|att| att.index_in_filling);
        // Process org and individual author names.
        let mut organization_authors = {
            if let Some(org_authors) = cached_orgauthorlist {
                org_authors
            } else if input.organization_authors.is_empty() {
                split_and_fix_organization_names_blob(&input.organization_authors_blob).await
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

        let org_futures = organization_authors
            .iter_mut()
            .map(|org| associate_organization_with_name(org, fixed_jur, pool));
        let human_futures = individual_authors
            .iter_mut()
            .map(|human| associate_individual_author_with_name(human, fixed_jur, pool));
        let _res = join!(join_all(org_futures), join_all(human_futures));

        let proc_filling = Self {
            object_uuid,
            filed_date: input.filed_date,
            index_in_docket: index_data.index,
            attachments: processed_attachments,
            name: input.name.clone(),
            filling_govid: input.filling_govid.clone(),
            filling_url: input.filling_url.clone(),
            filing_type: input.filing_type.clone(),
            description: input.description.clone(),
            // Super hacky workaround until I can change the input type.
            extra_metadata: input.extra_metadata.clone().into_iter().collect(),
            organization_authors,
            individual_authors,
        };
        Ok(proc_filling)
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
            index_in_filling: index_data.index,
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
