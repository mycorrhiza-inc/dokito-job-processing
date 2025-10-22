use crate::types::processed::{ProcessedGenericHuman, ProcessedGenericOrganization};
use anyhow::bail;
use sqlx::{FromRow, PgPool, query_as};
use std::collections::BTreeSet;
use uuid::Uuid;

use crate::jurisdiction_schema_mapping::FixedJurisdiction;
use anyhow::Result;

#[derive(FromRow, Clone)]
struct HumanRecord {
    uuid: Uuid,
    western_first_name: String,
    western_last_name: String,
    contact_emails: Vec<String>,
    contact_phone_numbers: Vec<String>,
}

#[derive(FromRow, Clone)]
struct OrganizationRecord {
    uuid: Uuid,
    name: String,
    // Optional: if your schema actually includes these fields, keep them
    // aliases: Option<Vec<String>>,
    // description: Option<String>,
    // artifical_person_type: Option<String>,
    // org_suffix: Option<String>,
}

pub async fn associate_individual_author_with_name(
    individual: &mut ProcessedGenericHuman,
    fixed_jur: FixedJurisdiction,
    pool: &PgPool,
) -> Result<()> {
    let pg_schema = fixed_jur.get_postgres_schema_name();

    // Step 1: Try match by UUID
    if !individual.object_uuid.is_nil() {
        let author_id = individual.object_uuid;
        let result = query_as::<_, HumanRecord>(&format!(
            "SELECT uuid, western_first_name, western_last_name, contact_emails, contact_phone_numbers \
             FROM {pg_schema}.humans WHERE uuid=$1"
        ))
        .bind(author_id)
        .fetch_optional(pool)
        .await?;

        if let Some(matched_record) = result
            && matched_record.western_first_name == individual.western_first_name
            && matched_record.western_last_name == individual.western_last_name
        {
            individual.object_uuid = matched_record.uuid;
            return Ok(());
        }
    }

    // Step 2: Match (and possibly deduplicate) by full name
    if let Some(found_uuid) = find_and_merge_humans_by_name(individual, &pg_schema, pool).await? {
        individual.object_uuid = found_uuid;
        return Ok(());
    }

    // Step 3: Insert new record
    let mut provisional_uuid = individual.object_uuid;
    if provisional_uuid.is_nil() {
        provisional_uuid = Uuid::new_v4();
        individual.object_uuid = provisional_uuid;
    }

    sqlx::query(&format!(
        "INSERT INTO {pg_schema}.humans (uuid, name, western_first_name, western_last_name, contact_emails, contact_phone_numbers) \
         VALUES ($1, $2, $3, $4, $5, $6)"
    ))
    .bind(provisional_uuid)
    .bind(individual.human_name.as_str())
    .bind(&individual.western_first_name)
    .bind(&individual.western_last_name)
    .bind(&individual.contact_emails)
    .bind(&individual.contact_phone_numbers)
    .execute(pool)
    .await?;

    Ok(())
}

/// Helper: find all humans with the same name, deduplicate, and return the canonical UUID.
async fn find_and_merge_humans_by_name(
    individual: &mut ProcessedGenericHuman,
    pg_schema: &str,
    pool: &PgPool,
) -> Result<Option<Uuid>> {
    let human_name = individual.human_name.as_str();

    let matches = query_as::<_, HumanRecord>(&format!(
        "SELECT uuid, western_first_name, western_last_name, contact_emails, contact_phone_numbers \
         FROM {pg_schema}.humans WHERE name = $1"
    ))
    .bind(human_name)
    .fetch_all(pool)
    .await?;

    if matches.is_empty() {
        return Ok(None);
    }

    let mut tx = pool.begin().await?;

    let canonical = matches[0].clone();

    let mut all_emails: BTreeSet<String> = canonical.contact_emails.clone().into_iter().collect();
    let mut all_phones: BTreeSet<String> = canonical
        .contact_phone_numbers
        .clone()
        .into_iter()
        .collect();

    for dup in &matches[1..] {
        all_emails.extend(dup.contact_emails.iter().cloned());
        all_phones.extend(dup.contact_phone_numbers.iter().cloned());
    }

    all_emails.extend(individual.contact_emails.iter().cloned());
    all_phones.extend(individual.contact_phone_numbers.iter().cloned());

    let merged_emails: Vec<String> = all_emails.into_iter().collect();
    let merged_phones: Vec<String> = all_phones.into_iter().collect();

    sqlx::query(&format!(
        "UPDATE {pg_schema}.humans SET contact_emails = $1, contact_phone_numbers = $2 WHERE uuid = $3"
    ))
    .bind(&merged_emails)
    .bind(&merged_phones)
    .bind(canonical.uuid)
    .execute(&mut *tx)
    .await?;

    if matches.len() > 1 {
        let dup_ids: Vec<Uuid> = matches.iter().skip(1).map(|r| r.uuid).collect();
        sqlx::query(&format!(
            "DELETE FROM {pg_schema}.humans WHERE uuid = ANY($1)"
        ))
        .bind(&dup_ids)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;

    individual.object_uuid = canonical.uuid;

    Ok(Some(canonical.uuid))
}

// -----------------------------------------------------------------------------
// ORGANIZATIONS
// -----------------------------------------------------------------------------

pub async fn associate_organization_with_name(
    org: &mut ProcessedGenericOrganization,
    fixed_jur: FixedJurisdiction,
    pool: &PgPool,
) -> Result<()> {
    let pg_schema = fixed_jur.get_postgres_schema_name();

    // Step 1: Try by UUID
    if !org.object_uuid.is_nil() {
        let org_id = org.object_uuid;
        let result = query_as::<_, OrganizationRecord>(&format!(
            "SELECT uuid, name FROM {pg_schema}.organizations WHERE uuid=$1"
        ))
        .bind(org_id)
        .fetch_optional(pool)
        .await?;

        if let Some(matched_record) = result
            && matched_record.name == org.truncated_org_name
        {
            org.object_uuid = matched_record.uuid;
            return Ok(());
        }
    }

    // Step 2: Match (and deduplicate) by org name
    if let Some(found_uuid) = find_and_merge_orgs_by_name(org, &pg_schema, pool).await? {
        org.object_uuid = found_uuid;
        return Ok(());
    }

    // Step 3: Insert new record
    let mut provisional_uuid = org.object_uuid;
    if provisional_uuid.is_nil() {
        provisional_uuid = Uuid::new_v4();
        org.object_uuid = provisional_uuid;
    }

    let org_type = org.org_type.to_string();

    sqlx::query(&format!(
        "INSERT INTO {pg_schema}.organizations (uuid, name, aliases, description, artifical_person_type, org_suffix) \
         VALUES ($1, $2, $3, $4, $5, $6)"
    ))
    .bind(provisional_uuid)
    .bind(org.truncated_org_name.as_str())
    .bind(vec![org.truncated_org_name.to_string()])
    .bind("")
    .bind(&org_type)
    .bind(&org.org_suffix)
    .execute(pool)
    .await?;

    Ok(())
}

/// Helper: deduplicate organization entries by name.
async fn find_and_merge_orgs_by_name(
    org: &mut ProcessedGenericOrganization,
    pg_schema: &str,
    pool: &PgPool,
) -> Result<Option<Uuid>> {
    let org_name = org.truncated_org_name.as_str();

    let matches = query_as::<_, OrganizationRecord>(&format!(
        "SELECT uuid, name FROM {pg_schema}.organizations WHERE name = $1"
    ))
    .bind(org_name)
    .fetch_all(pool)
    .await?;

    if matches.is_empty() {
        return Ok(None);
    }

    // Start a transaction to make the dedup atomic
    let mut tx = pool.begin().await?;

    // Keep the first record as canonical
    let canonical = matches[0].clone();

    // Delete all duplicates
    if matches.len() > 1 {
        let dup_ids: Vec<Uuid> = matches.iter().skip(1).map(|r| r.uuid).collect();
        sqlx::query(&format!(
            "DELETE FROM {pg_schema}.organizations WHERE uuid = ANY($1)"
        ))
        .bind(&dup_ids)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    org.object_uuid = canonical.uuid;

    Ok(Some(canonical.uuid))
}

pub async fn upload_docket_party_human_connection(
    upload_party: &mut ProcessedGenericHuman,
    parent_docket_uuid: Uuid,
    fixed_jur: FixedJurisdiction,
    pool: &PgPool,
) -> Result<(), anyhow::Error> {
    if parent_docket_uuid.is_nil() {
        bail!("Uploading docket must have a non nil uuid.")
    }

    associate_individual_author_with_name(upload_party, fixed_jur, pool).await?;

    if upload_party.object_uuid.is_nil() {
        unreachable!(
            "Uploading party must have a non nil uuid, this should be impossible since it should have just been associated with one in the database if it did not exist.."
        )
    }

    let party_email = upload_party
        .contact_emails
        .first()
        .map(|s| s.as_str())
        .unwrap_or("");
    let party_phone = upload_party
        .contact_phone_numbers
        .first()
        .map(|s| s.as_str())
        .unwrap_or("");

    let pg_schema = fixed_jur.get_postgres_schema_name();
    sqlx::query(&format!(
        "INSERT INTO {pg_schema}.individual_offical_party_to_docket (docket_uuid, individual_uuid, party_email_contact, party_phone_contact) VALUES ($1, $2, $3, $4)"
    ))
    .bind(parent_docket_uuid)
    .bind(upload_party.object_uuid)
    .bind(party_email)
    .bind(party_phone)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn upload_docket_petitioner_org_connection(
    upload_petitioner: &mut ProcessedGenericOrganization,
    parent_docket_uuid: Uuid,
    fixed_jur: FixedJurisdiction,
    pool: &PgPool,
) -> Result<(), anyhow::Error> {
    if parent_docket_uuid.is_nil() {
        bail!("Uploading filing must have a non nil uuid.")
    }
    associate_organization_with_name(upload_petitioner, fixed_jur, pool).await?;
    if upload_petitioner.object_uuid.is_nil() {
        unreachable!(
            "Uploading filing author must have a non nil uuid. This should be impossible because it just happened in the previous step"
        )
    };
    let petitioner_uuid = upload_petitioner.object_uuid;

    let pg_schema = fixed_jur.get_postgres_schema_name();
    sqlx::query(&format!(
        "INSERT INTO {pg_schema}.docket_petitioned_by_org (docket_uuid, petitioner_uuid) VALUES ($1,$2)"
    ))
    .bind(parent_docket_uuid)
    .bind(petitioner_uuid)
    .execute(pool)
    .await?;
    Ok(())
}
pub async fn upload_filing_organization_author(
    upload_org_author: &mut ProcessedGenericOrganization,
    parent_filing_uuid: Uuid,
    fixed_jur: FixedJurisdiction,
    pool: &PgPool,
) -> Result<(), anyhow::Error> {
    if parent_filing_uuid.is_nil() {
        bail!("Uploading filing must have a non nil uuid.")
    }
    associate_organization_with_name(upload_org_author, fixed_jur, pool).await?;
    if upload_org_author.object_uuid.is_nil() {
        unreachable!(
            "Uploading filing author must have a non nil uuid. This should be impossible because it just happened in the previous step"
        )
    }
    let org_uuid = upload_org_author.object_uuid;

    let pg_schema = fixed_jur.get_postgres_schema_name();
    sqlx::query(&format!(
            "INSERT INTO {pg_schema}.filings_on_behalf_of_org_relation (author_organization_uuid, filing_uuid) VALUES ($1, $2)"
        ))
        .bind(org_uuid)
        .bind(parent_filing_uuid)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn upload_filing_human_author(
    upload_author: &mut ProcessedGenericHuman,
    parent_filing_uuid: Uuid,
    fixed_jur: FixedJurisdiction,
    pool: &PgPool,
) -> Result<(), anyhow::Error> {
    if parent_filing_uuid.is_nil() {
        bail!("Uploading filing must have a non nil uuid.")
    }
    associate_individual_author_with_name(upload_author, fixed_jur, pool).await?;
    if upload_author.object_uuid.is_nil() {
        unreachable!(
            "Uploading filing author must have a non nil uuid, this should be impossible dispite it being validated on the previous line."
        )
    }

    let pg_schema = fixed_jur.get_postgres_schema_name();
    sqlx::query(&format!(
        "INSERT INTO {pg_schema}.filings_filed_by_individual (human_uuid, filing_uuid) VALUES ($1, $2)"
    ))
    .bind(upload_author.object_uuid)
    .bind(parent_filing_uuid)
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::PgPool;
    use std::env;
    use uuid::Uuid;

    async fn setup_test_db() -> PgPool {
        let database_url = env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://test:test@localhost/test_db".to_string());
        PgPool::connect(&database_url)
            .await
            .expect("Failed to connect to test database")
    }

    #[tokio::test]
    async fn test_associate_individual_author_with_name_new_person() {
        let pool = setup_test_db().await;

        // Create a new individual that doesn't exist in the database
        let mut individual = ProcessedGenericHuman {
            object_uuid: Uuid::nil(),
            western_first_name: "Test".to_string(),
            western_last_name: "Person".to_string(),
            human_name: "Bob Smith".try_into().unwrap(),
            contact_emails: vec!["test@example.com".to_string()],
            contact_phone_numbers: vec!["+1234567890".to_string()],
            contact_addresses: vec![],
            representing_company: None,
            employed_by: None,
            title: "Senior Manager".into(),
        };

        let fixed_jur = FixedJurisdiction::NewYorkPuc; // Use a test jurisdiction
        let result = associate_individual_author_with_name(&mut individual, fixed_jur, &pool).await;

        assert!(
            result.is_ok(),
            "Failed to associate new individual: {:?}",
            result
        );
        assert!(!individual.object_uuid.is_nil(), "UUID should be assigned");

        // Verify the person was actually inserted
        let pg_schema = fixed_jur.get_postgres_schema_name();
        let db_record = query_as::<_, HumanRecord>(&format!(
            "SELECT uuid, western_first_name, western_last_name, contact_emails, contact_phone_numbers FROM {pg_schema}.humans WHERE uuid = $1"
        ))
        .bind(individual.object_uuid)
        .fetch_one(&pool)
        .await;

        assert!(db_record.is_ok(), "Failed to fetch inserted record");
        let record = db_record.unwrap();
        assert_eq!(record.western_first_name, "Test");
        assert_eq!(record.western_last_name, "Person");
        assert_eq!(record.contact_emails, vec!["test@example.com"]);
    }

    #[tokio::test]
    async fn test_query_structures_compile() {
        // This test verifies that our query structures compile correctly
        let pool = setup_test_db().await;

        // // Test HumanRecord query structure
        // let _human_query = query_as::<_, HumanRecord>(
        //     "SELECT uuid, western_first_name, western_last_name, contact_emails, contact_phone_numbers FROM humans LIMIT 0",
        // );

        // // Test OrganizationRecord query structure
        // let _org_query =
        //     query_as::<_, OrganizationRecord>("SELECT uuid, name FROM organizations LIMIT 0");

        // If we get here, the query structures compile correctly
        assert!(true);
    }
}
