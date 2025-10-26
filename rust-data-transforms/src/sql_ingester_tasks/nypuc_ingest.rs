use std::{
    collections::HashSet,
    hash::{DefaultHasher, Hash, Hasher},
    mem::take,
};

use crate::types::{
    env_vars::DIGITALOCEAN_S3,
    processed::{ProcessedGenericDocket, ProcessedGenericFiling, ProcessedGenericOrganization},
    raw::JurisdictionInfo,
    s3_stuff::{DocketAddress, list_raw_cases_for_jurisdiction},
};
use async_trait::async_trait;
use futures::future::join_all;
use rand::{SeedableRng, rngs::SmallRng, seq::SliceRandom};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::Value;
use sqlx::{PgPool, Pool, Postgres, query_scalar, types::Uuid};

use mycorrhiza_common::tasks::ExecuteUserTask;
use tokio::sync::Semaphore;
use tracing::{info, warn};

use crate::{
    jurisdiction_schema_mapping::FixedJurisdiction,
    sql_ingester_tasks::{
        database_author_association::*, dokito_sql_connection::get_dokito_pool,
        recreate_dokito_table_schema::delete_all_data,
    },
};

fn generate_hash(x: &impl Hash) -> u64 {
    let mut hasher = DefaultHasher::new();
    x.hash(&mut hasher);
    hasher.finish()
}

pub async fn ingest_sql_case_with_retries(
    case: &mut ProcessedGenericDocket,
    fixed_jur: FixedJurisdiction,
    pool: &Pool<Postgres>,
    ignore_existing: bool,
    tries: usize,
) -> anyhow::Result<()> {
    let _initial_hash = generate_hash(&*case);
    let mut return_res = Ok(());
    let pg_schema = fixed_jur.get_postgres_schema_name();
    for remaining_tries in (0..tries).rev() {
        match ingest_sql_fixed_jurisdiction_case(case, fixed_jur, pool, ignore_existing).await {
            Ok(val) => {
                return Ok(val);
            }
            Err(err) => {
                let mut error_debug = format!("{:?}", &err);
                error_debug.truncate(200);
                warn!(docket_govid=%case.case_govid, %remaining_tries, %err, err_debug=%error_debug,"Encountered error while processing docket, retrying.");
                return_res = Err(err);
                let existing_docket: Option<Uuid> = query_scalar(&format!(
                    "SELECT uuid FROM {pg_schema}.dockets WHERE docket_govid = $1"
                ))
                .bind(case.case_govid.as_str())
                .fetch_optional(pool)
                .await?;

                if let Some(docket_uuid) = existing_docket {
                    sqlx::query(&format!("DELETE FROM {pg_schema}.dockets WHERE uuid = $1"))
                        .bind(docket_uuid)
                        .execute(pool)
                        .await?;
                    info!(%docket_uuid, %case.case_govid,"Successfully deleted corrupted case data");
                } else {
                    info!(%case.case_govid,"Case ingest errored, but could not find corrupt case data.");
                }
            }
        }
    }
    return_res
}

pub fn bubble_error<T, E, I>(results: I) -> Result<(), E>
where
    I: IntoIterator<Item = Result<T, E>>,
{
    for res in results {
        res?; // early-return on the first Err
    }
    Ok(())
}
pub async fn ingest_sql_fixed_jurisdiction_case(
    case: &mut ProcessedGenericDocket,
    fixed_jur: FixedJurisdiction,
    pool: &Pool<Postgres>,
    _ignore_existing: bool,
) -> anyhow::Result<()> {
    let pg_schema = fixed_jur.get_postgres_schema_name();
    let petitioner_list: &mut [ProcessedGenericOrganization] = &mut case.petitioner_list;
    let petitioner_strings = petitioner_list
        .iter()
        .map(|n| n.truncated_org_name.to_string())
        .collect::<Vec<_>>();
    let mut case_type = case.case_type.clone();
    let mut case_subtype = case.case_subtype.clone();
    if let Some(actual_subtype_value) = case.extra_metadata.get("matter_subtype")
        && let Some(actual_subtype) = actual_subtype_value.as_str()
        && let Some(actual_type_value) = case.extra_metadata.get("matter_type")
        && let Some(actual_type) = actual_type_value.as_str()
    {
        case_type = actual_type.to_string();
        case_subtype = actual_subtype.to_string();
    }

    // Upsert docket
    let docket_uuid: Uuid = query_scalar(
        &format!("INSERT INTO {pg_schema}.dockets (uuid, docket_govid, docket_description, docket_title, industry, hearing_officer, opened_date, closed_date, petitioner_strings, docket_type, docket_subtype )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
         ON CONFLICT (uuid) DO UPDATE SET
         docket_govid = EXCLUDED.docket_govid,
         docket_description = EXCLUDED.docket_description,
         docket_title = EXCLUDED.docket_title,
         industry = EXCLUDED.industry,
         hearing_officer = EXCLUDED.hearing_officer,
         opened_date = EXCLUDED.opened_date,
         closed_date = EXCLUDED.closed_date,
         petitioner_strings = EXCLUDED.petitioner_strings,
         docket_type = EXCLUDED.docket_type,
         docket_subtype = EXCLUDED.docket_subtype
         RETURNING uuid")
    )
    .bind(case.object_uuid)
    .bind(case.case_govid.as_str())
    .bind(&case.description)
    .bind(&case.case_name)
    .bind(&case.industry)
    .bind(&case.hearing_officer)
    .bind(case.opened_date)
    .bind(case.closed_date)
    .bind(&petitioner_strings)
    .bind(case_type)
    .bind(case_subtype)
    .fetch_one(pool)
    .await?;
    if docket_uuid != case.object_uuid {
        info!("Created new uuid for docket.")
    }

    let simultaneous_party_and_individuals = Semaphore::new(4);
    let petitioner_futures = petitioner_list.iter_mut().map(async |petitioner| {
        let _permit = simultaneous_party_and_individuals.acquire().await;
        upload_docket_petitioner_org_connection(petitioner, docket_uuid, fixed_jur, pool).await
    });
    let petitioner_results = join_all(petitioner_futures).await;
    bubble_error(petitioner_results.into_iter())?;

    let party_futures = case.case_parties.iter_mut().map(async |party| {
        let _permit = simultaneous_party_and_individuals.acquire().await;
        upload_docket_party_human_connection(party, docket_uuid, fixed_jur, pool).await
    });
    let party_results = join_all(party_futures).await;
    bubble_error(party_results)?;

    let process_filing_closure =
        async |filing: &mut ProcessedGenericFiling| -> Result<(), anyhow::Error> {
            let individual_author_strings = filing
                .individual_authors
                .iter()
                .map(|s| s.human_name.to_string())
                .collect::<Vec<_>>();
            let organization_author_strings = filing
                .organization_authors
                .iter()
                .map(|s| s.truncated_org_name.to_string())
                .collect::<Vec<_>>();
            let filing_uuid: Uuid = query_scalar(
            &format!("INSERT INTO {pg_schema}.filings (uuid, docket_uuid, docket_govid, individual_author_strings, organization_author_strings, filed_date, filing_type, filing_name, filing_description, openscrapers_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             ON CONFLICT (uuid) DO UPDATE SET
             docket_uuid = EXCLUDED.docket_uuid,
             docket_govid = EXCLUDED.docket_govid,
             individual_author_strings = EXCLUDED.individual_author_strings,
             organization_author_strings = EXCLUDED.organization_author_strings,
             filed_date = EXCLUDED.filed_date,
             filing_type = EXCLUDED.filing_type,
             filing_name = EXCLUDED.filing_name,
             filing_description = EXCLUDED.filing_description,
             openscrapers_id = EXCLUDED.openscrapers_id
             RETURNING uuid")
        )
        .bind(filing.object_uuid)
        .bind(docket_uuid)
        .bind(case.case_govid.as_str())
        .bind(&individual_author_strings)
        .bind(&organization_author_strings)
        .bind(filing.filed_date)
        .bind(&filing.filing_type)
        .bind(&filing.name)
        .bind(&filing.description)
        .bind(filing.object_uuid.to_string())
        .fetch_one(pool)
        .await?;
            if filing_uuid != filing.object_uuid {
                info!(%filing_uuid, "Set filing to have new uuid");
                filing.object_uuid = filing_uuid;
            }

            // Associate individual authors using the proper association functions
            for individual_author in filing.individual_authors.iter_mut() {
                upload_filing_human_author(individual_author, filing_uuid, fixed_jur, pool).await?;
            }

            // Associate organization authors using the proper association functions
            for org_author in filing.organization_authors.iter_mut() {
                upload_filing_organization_author(org_author, filing_uuid, fixed_jur, pool).await?;
            }

            for attachment in filing.attachments.iter_mut() {
                let hashstr = attachment
                    .hash
                    .map(|h| h.to_string())
                    .unwrap_or_else(|| "".to_string());
                let attachment_uuid: Uuid = query_scalar(
                &format!("INSERT INTO {pg_schema}.attachments (uuid, parent_filing_uuid, file_hash_if_downloaded, attachment_file_extension, attachment_file_name, attachment_title, attachment_url, openscrapers_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (uuid) DO UPDATE SET
                    parent_filing_uuid = EXCLUDED.parent_filing_uuid,
                    file_hash_if_downloaded = EXCLUDED.file_hash_if_downloaded,
                    attachment_file_extension = EXCLUDED.attachment_file_extension,
                    attachment_file_name = EXCLUDED.attachment_file_name,
                    attachment_title = EXCLUDED.attachment_title,
                    attachment_url = EXCLUDED.attachment_url,
                    openscrapers_id = EXCLUDED.openscrapers_id
                    RETURNING uuid")
            )
            .bind(attachment.object_uuid)
            .bind(filing_uuid)
            .bind(hashstr)
            .bind(&*attachment.document_extension.to_string())
            .bind(&attachment.name)
            .bind(&attachment.name)
            .bind(&attachment.url)
            .bind(&*attachment.object_uuid.to_string())
            .fetch_one(pool)
            .await?;
                if attachment_uuid != attachment.object_uuid {
                    info!(%attachment_uuid, "Set attachment to have new uuid");
                    attachment.object_uuid = attachment_uuid;
                }
            }
            Ok(())
        };
    let simultaneous_file_uploads = Semaphore::new(3);
    let filing_futures = case.filings.iter_mut().map(async |filing| {
        let _permit = simultaneous_file_uploads.acquire().await?;
        process_filing_closure(filing).await
    });
    let filing_results = join_all(filing_futures).await;
    bubble_error(filing_results.into_iter())?;

    tracing::info!(govid=%case.case_govid, uuid=%docket_uuid,"Successfully processed case with no errors");
    Ok(())
}
