#!/usr/bin/env python3
import json
import sys
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values

# Database connection string
DB_URL = "postgresql://postgres.oticlgskebuoswlbpucb:6kDStpZh0E6v07Uy@aws-0-us-east-2.pooler.supabase.com:5432/postgres"

def parse_date(date_str):
    """Parse ISO date string to date object"""
    if not date_str:
        return None
    return datetime.fromisoformat(date_str.replace('Z', '+00:00')).date()

def get_or_create_organization(cursor, org_name):
    """Get or create an organization by name, return UUID"""
    if not org_name or org_name.strip() == '':
        return None

    org_name = org_name.strip()

    # Check if organization exists
    cursor.execute(
        "SELECT uuid FROM ny_puc_data.organizations WHERE name = %s",
        (org_name,)
    )
    result = cursor.fetchone()

    if result:
        return result[0]

    # Create new organization
    cursor.execute(
        """INSERT INTO ny_puc_data.organizations
           (name, aliases, description, artifical_person_type, org_suffix)
           VALUES (%s, %s, %s, %s, %s)
           RETURNING uuid""",
        (org_name, [], '', 'organization', '')
    )
    return cursor.fetchone()[0]

def get_or_create_human(cursor, name, first_name, last_name, contact_email='', contact_phone=''):
    """Get or create a human by name, return UUID"""
    if not name or name.strip() == '':
        return None

    name = name.strip()
    first_name = first_name.strip() if first_name else ''
    last_name = last_name.strip() if last_name else ''

    # Check if human exists by name
    cursor.execute(
        "SELECT uuid FROM ny_puc_data.individuals WHERE name = %s",
        (name,)
    )
    result = cursor.fetchone()

    if result:
        return result[0]

    # Create new human
    emails = [contact_email] if contact_email else []
    phones = [contact_phone] if contact_phone else []

    cursor.execute(
        """INSERT INTO ny_puc_data.individuals
           (name, western_first_name, western_last_name, contact_emails, contact_phone_numbers)
           VALUES (%s, %s, %s, %s, %s)
           RETURNING uuid""",
        (name, first_name, last_name, emails, phones)
    )
    return cursor.fetchone()[0]

def insert_docket(cursor, case_data):
    """Insert a docket and return its UUID"""
    docket_govid = case_data['case_govid']

    # Check if docket already exists
    cursor.execute(
        "SELECT uuid FROM ny_puc_data.dockets WHERE docket_govid = %s",
        (docket_govid,)
    )
    result = cursor.fetchone()

    if result:
        print(f"Docket {docket_govid} already exists, skipping...")
        return result[0]

    petitioner_strings = [case_data.get('petitioner', '')] if case_data.get('petitioner') else []

    cursor.execute(
        """INSERT INTO ny_puc_data.dockets
           (docket_govid, docket_title, docket_type, docket_subtype,
            industry, opened_date, petitioner_strings, docket_description,
            hearing_officer, closed_date, current_status, assigned_judge)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           RETURNING uuid""",
        (
            docket_govid,
            case_data.get('case_name', ''),
            case_data.get('case_type', ''),
            case_data.get('case_subtype', ''),
            case_data.get('industry', ''),
            parse_date(case_data.get('opened_date')),
            petitioner_strings,
            '',  # docket_description
            '',  # hearing_officer
            None,  # closed_date
            '',  # current_status
            ''   # assigned_judge
        )
    )
    return cursor.fetchone()[0]

def insert_filing(cursor, docket_uuid, docket_govid, filing_data):
    """Insert a filing and return its UUID"""
    openscrapers_id = f"{docket_govid}_{filing_data.get('filing_govid', '')}"

    # Check if filing already exists
    cursor.execute(
        "SELECT uuid FROM ny_puc_data.filings WHERE openscrapers_id = %s",
        (openscrapers_id,)
    )
    result = cursor.fetchone()

    if result:
        print(f"Filing {openscrapers_id} already exists, skipping...")
        return result[0]

    individual_authors = filing_data.get('individual_authors', [])
    org_authors_blob = filing_data.get('organization_authors_blob', '')
    org_authors = [org_authors_blob] if org_authors_blob else []

    cursor.execute(
        """INSERT INTO ny_puc_data.filings
           (docket_uuid, docket_govid, filing_name, filed_date, filing_type,
            filing_description, filing_govid, filing_number, openscrapers_id,
            individual_author_strings, organization_author_strings)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           RETURNING uuid""",
        (
            docket_uuid,
            docket_govid,
            filing_data.get('name', ''),
            parse_date(filing_data.get('filed_date')),
            filing_data.get('filing_type', ''),
            filing_data.get('description', ''),
            filing_data.get('filing_govid', ''),
            filing_data.get('filing_number', ''),
            openscrapers_id,
            individual_authors,
            org_authors
        )
    )
    return cursor.fetchone()[0]

def insert_attachment(cursor, filing_uuid, docket_govid, filing_govid, attachment_data, index):
    """Insert an attachment"""
    openscrapers_id = f"{docket_govid}_{filing_govid}_att_{index}"

    # Check if attachment already exists
    cursor.execute(
        "SELECT uuid FROM ny_puc_data.attachments WHERE openscrapers_id = %s",
        (openscrapers_id,)
    )
    result = cursor.fetchone()

    if result:
        print(f"Attachment {openscrapers_id} already exists, skipping...")
        return result[0]

    file_name = attachment_data.get('extra_metadata', {}).get('fileName', '')

    cursor.execute(
        """INSERT INTO ny_puc_data.attachments
           (parent_filing_uuid, attachment_title, attachment_file_name,
            attachment_file_extension, attachment_url, attachment_type,
            attachment_subtype, openscrapers_id, blake2b_hash)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
           RETURNING uuid""",
        (
            filing_uuid,
            attachment_data.get('name', ''),
            file_name,
            attachment_data.get('document_extension', ''),
            attachment_data.get('url', ''),
            attachment_data.get('attachment_type', ''),
            attachment_data.get('attachment_subtype', ''),
            openscrapers_id,
            ''  # blake2b_hash
        )
    )
    return cursor.fetchone()[0]

def main():
    # Read JSON file
    if len(sys.argv) < 2:
        print("Usage: python upload_script.py <json_file>")
        sys.exit(1)

    json_file = sys.argv[1]
    with open(json_file, 'r') as f:
        cases = json.load(f)

    # Connect to database
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()

    try:
        for case in cases:
            print(f"\nProcessing case {case['case_govid']}...")

            # 1. Insert/get organizations (from petitioner)
            petitioner_org_uuid = None
            if case.get('petitioner'):
                petitioner_org_uuid = get_or_create_organization(cursor, case['petitioner'])
                print(f"  Petitioner org UUID: {petitioner_org_uuid}")

            # 2. Insert docket
            docket_uuid = insert_docket(cursor, case)
            print(f"  Docket UUID: {docket_uuid}")

            # 3. Link docket to petitioner organization
            if petitioner_org_uuid:
                # Check if relation already exists
                cursor.execute(
                    """SELECT uuid FROM ny_puc_data.docket_petitioned_by_org
                       WHERE docket_uuid = %s AND petitioner_uuid = %s""",
                    (docket_uuid, petitioner_org_uuid)
                )
                if not cursor.fetchone():
                    cursor.execute(
                        """INSERT INTO ny_puc_data.docket_petitioned_by_org
                           (docket_uuid, petitioner_uuid)
                           VALUES (%s, %s)""",
                        (docket_uuid, petitioner_org_uuid)
                    )

            # 4. Process case_parties (humans)
            case_party_humans = {}
            for party in case.get('case_parties', []):
                if party.get('artifical_person_type') == 'human':
                    name = party.get('name', '')
                    human_uuid = get_or_create_human(
                        cursor,
                        name,
                        party.get('western_human_first_name', ''),
                        party.get('western_human_last_name', ''),
                        party.get('contact_email', ''),
                        party.get('contact_phone', '')
                    )
                    case_party_humans[name] = human_uuid
                    print(f"  Case party human '{name}' UUID: {human_uuid}")

                    # Link human to docket
                    associated_org_uuid = None
                    if party.get('human_associated_company'):
                        associated_org_uuid = get_or_create_organization(
                            cursor, party['human_associated_company']
                        )

                    # Check if relation already exists
                    cursor.execute(
                        """SELECT uuid FROM ny_puc_data.individual_offical_party_to_docket
                           WHERE docket_uuid = %s AND individual_uuid = %s""",
                        (docket_uuid, human_uuid)
                    )
                    if not cursor.fetchone():
                        cursor.execute(
                            """INSERT INTO ny_puc_data.individual_offical_party_to_docket
                               (docket_uuid, individual_uuid, party_email_contact,
                                party_phone_contact, employed_by_org)
                               VALUES (%s, %s, %s, %s, %s)""",
                            (
                                docket_uuid,
                                human_uuid,
                                party.get('contact_email', ''),
                                party.get('contact_phone', ''),
                                associated_org_uuid
                            )
                        )

            # 5. Process filings
            for filing in case.get('filings', []):
                filing_uuid = insert_filing(
                    cursor, docket_uuid, case['case_govid'], filing
                )
                print(f"  Filing UUID: {filing_uuid}")

                # Link filing to individual authors
                for individual_author in filing.get('individual_authors', []):
                    human_uuid = get_or_create_human(
                        cursor, individual_author, '', ''
                    )

                    # Check if relation already exists
                    cursor.execute(
                        """SELECT uuid FROM ny_puc_data.filings_filed_by_individual
                           WHERE filing_uuid = %s AND individual_uuid = %s""",
                        (filing_uuid, human_uuid)
                    )
                    if not cursor.fetchone():
                        cursor.execute(
                            """INSERT INTO ny_puc_data.filings_filed_by_individual
                               (filing_uuid, individual_uuid)
                               VALUES (%s, %s)""",
                            (filing_uuid, human_uuid)
                        )

                # Link filing to organization authors
                org_blob = filing.get('organization_authors_blob', '')
                if org_blob:
                    org_uuid = get_or_create_organization(cursor, org_blob)

                    # Check if relation already exists
                    cursor.execute(
                        """SELECT relation_uuid FROM ny_puc_data.filings_on_behalf_of_org_relation
                           WHERE filing_uuid = %s AND author_organization_uuid = %s""",
                        (filing_uuid, org_uuid)
                    )
                    if not cursor.fetchone():
                        cursor.execute(
                            """INSERT INTO ny_puc_data.filings_on_behalf_of_org_relation
                               (filing_uuid, author_organization_uuid)
                               VALUES (%s, %s)""",
                            (filing_uuid, org_uuid)
                        )

                # Insert attachments
                for idx, attachment in enumerate(filing.get('attachments', [])):
                    attachment_uuid = insert_attachment(
                        cursor, filing_uuid, case['case_govid'],
                        filing.get('filing_govid', ''), attachment, idx
                    )
                    print(f"    Attachment {idx} UUID: {attachment_uuid}")

        # Commit all changes
        conn.commit()
        print("\n✅ All data inserted successfully!")

    except Exception as e:
        conn.rollback()
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    main()
