#!/usr/bin/env python3
import json
import sys
import os
from datetime import datetime
from collections import defaultdict
import psycopg2
from psycopg2.extras import execute_values

# Database connection string - read from environment or use default
DB_URL = os.getenv('SUPABASE_DATABASE_URL', os.getenv('DATABASE_URL',
    "postgresql://postgres.oticlgskebuoswlbpucb:8i5oRvtQN8ciglnR@aws-0-us-east-2.pooler.supabase.com:6543/postgres"))

def parse_date(date_str):
    """Parse ISO date string to date object"""
    if not date_str:
        return None
    return datetime.fromisoformat(date_str.replace('Z', '+00:00')).date()

def parse_name(full_name):
    """
    Parse full name into first and last names.

    Rules:
    - If name contains " and ", keep entire name as first_name, last_name = ''
    - Otherwise: first word -> first_name, remaining -> last_name

    Returns: (first_name, last_name)
    """
    full_name = full_name.strip()

    # Handle names with " and " (e.g., "Ruth and William Scott")
    if ' and ' in full_name.lower():
        return (full_name, '')

    # Handle single-word names
    if ' ' not in full_name:
        return (full_name, '')

    # Regular parsing: first word = first name, rest = last name
    parts = full_name.split(' ', 1)
    first_name = parts[0]
    last_name = parts[1] if len(parts) > 1 else ''

    return (first_name, last_name)

def bulk_insert_organizations(cursor, org_names):
    """
    Bulk insert organizations with deduplication.
    Returns mapping of name -> UUID.
    Matches dokito-backend's associate_organization_with_name logic.
    """
    if not org_names:
        return {}

    # Get existing organizations by name
    cursor.execute(
        "SELECT name, uuid FROM ny_puc_data.organizations WHERE name = ANY(%s)",
        (list(org_names),)
    )
    existing = {row[0]: row[1] for row in cursor.fetchall()}

    # Insert only truly new organizations
    new_orgs = org_names - existing.keys()
    if new_orgs:
        org_values = [(name, [name], '', 'organization', '') for name in new_orgs]
        result = execute_values(
            cursor,
            """INSERT INTO ny_puc_data.organizations
               (name, aliases, description, artifical_person_type, org_suffix)
               VALUES %s
               ON CONFLICT (uuid) DO NOTHING
               RETURNING name, uuid""",
            org_values,
            page_size=1000,
            fetch=True
        )
        for name, uuid in result:
            existing[name] = uuid

    return existing

def bulk_insert_individuals(cursor, individuals_data):
    """
    Bulk insert individuals with deduplication and contact info merging.
    Returns mapping of (first_name, last_name) -> UUID.
    Matches dokito-backend's associate_individual_author_with_name logic.
    """
    if not individuals_data:
        return {}

    # Build lookup by (first_name, last_name)
    name_key_to_data = {}
    for ind in individuals_data:
        first = ind.get('first_name', '')
        last = ind.get('last_name', '')
        key = (first, last)
        if key not in name_key_to_data:
            name_key_to_data[key] = ind

    # Get existing individuals by first+last name
    name_pairs = list(name_key_to_data.keys())
    if not name_pairs:
        return {}

    # Query existing individuals
    cursor.execute(
        """SELECT western_first_name, western_last_name, uuid, contact_emails, contact_phone_numbers
           FROM ny_puc_data.individuals
           WHERE (western_first_name, western_last_name) IN %s""",
        (tuple(name_pairs),)
    )

    existing = {}
    updates_needed = []

    for row in cursor.fetchall():
        first, last, uuid, db_emails, db_phones = row
        key = (first, last)
        existing[key] = uuid

        # Merge contact info if new data available
        ind_data = name_key_to_data[key]
        new_email = ind_data.get('contact_email', '')
        new_phone = ind_data.get('contact_phone', '')

        # Use sets for deduplication
        merged_emails = set(db_emails if db_emails else [])
        merged_phones = set(db_phones if db_phones else [])

        if new_email:
            merged_emails.add(new_email)
        if new_phone:
            merged_phones.add(new_phone)

        # Check if update needed
        if (len(merged_emails) != len(db_emails or []) or
            len(merged_phones) != len(db_phones or [])):
            updates_needed.append((
                sorted(list(merged_emails)),
                sorted(list(merged_phones)),
                uuid
            ))

    # Update merged contact info
    if updates_needed:
        cursor.executemany(
            """UPDATE ny_puc_data.individuals
               SET contact_emails = %s, contact_phone_numbers = %s, updated_at = now()
               WHERE uuid = %s""",
            updates_needed
        )

    # Insert new individuals
    new_individuals = [
        (key, ind) for key, ind in name_key_to_data.items()
        if key not in existing
    ]

    if new_individuals:
        ind_values = [
            (
                f"{ind.get('first_name', '')} {ind.get('last_name', '')}".strip(),
                ind.get('first_name', ''),
                ind.get('last_name', ''),
                [ind.get('contact_email')] if ind.get('contact_email') else [],
                [ind.get('contact_phone')] if ind.get('contact_phone') else []
            )
            for key, ind in new_individuals
        ]
        result = execute_values(
            cursor,
            """INSERT INTO ny_puc_data.individuals
               (name, western_first_name, western_last_name, contact_emails, contact_phone_numbers)
               VALUES %s RETURNING western_first_name, western_last_name, uuid""",
            ind_values,
            page_size=1000,
            fetch=True
        )
        for first, last, uuid in result:
            existing[(first, last)] = uuid

    return existing

def bulk_insert_dockets(cursor, dockets_data):
    """Bulk insert dockets and return mapping of docket_govid -> UUID"""
    if not dockets_data:
        return {}

    docket_govids = [d['case_govid'] for d in dockets_data]

    # Get existing dockets
    cursor.execute(
        "SELECT docket_govid, uuid FROM ny_puc_data.dockets WHERE docket_govid = ANY(%s)",
        (docket_govids,)
    )
    existing = {row[0]: row[1] for row in cursor.fetchall()}

    # Insert new dockets
    new_dockets = [d for d in dockets_data if d['case_govid'] not in existing]
    if new_dockets:
        docket_values = [
            (
                d['case_govid'],
                d.get('case_name', ''),
                d.get('case_type', ''),
                d.get('case_subtype', ''),
                d.get('industry', ''),
                parse_date(d.get('opened_date')),
                [d.get('petitioner', '')] if d.get('petitioner') else [],
                '',  # docket_description
                '',  # hearing_officer
                None,  # closed_date
                '',  # current_status
                ''   # assigned_judge
            )
            for d in new_dockets
        ]
        result = execute_values(
            cursor,
            """INSERT INTO ny_puc_data.dockets
               (docket_govid, docket_title, docket_type, docket_subtype,
                industry, opened_date, petitioner_strings, docket_description,
                hearing_officer, closed_date, current_status, assigned_judge)
               VALUES %s RETURNING docket_govid, uuid""",
            docket_values,
            page_size=1000,
            fetch=True
        )
        for govid, uuid in result:
            existing[govid] = uuid

    return existing

def bulk_insert_filings(cursor, filings_data, docket_uuid_map):
    """Bulk insert filings and return list of UUIDs in same order as input"""
    if not filings_data:
        return []

    filing_values = [
        (
            docket_uuid_map[f['docket_govid']],
            f['docket_govid'],
            f.get('name', ''),
            parse_date(f.get('filed_date')),
            f.get('filing_type', ''),
            f.get('description', ''),
            f.get('filing_govid', ''),
            f.get('filing_number', ''),
            f.get('individual_authors', []),
            [f.get('organization_authors_blob', '')] if f.get('organization_authors_blob') else []
        )
        for f in filings_data
    ]

    result = execute_values(
        cursor,
        """INSERT INTO ny_puc_data.filings
           (docket_uuid, docket_govid, filing_name, filed_date, filing_type,
            filing_description, filing_govid, filing_number,
            individual_author_strings, organization_author_strings)
           VALUES %s RETURNING uuid""",
        filing_values,
        page_size=1000,
        fetch=True
    )

    return [row[0] for row in result]

def bulk_insert_attachments(cursor, attachments_data):
    """Bulk insert attachments"""
    if not attachments_data:
        return

    attachment_values = [
        (
            a['filing_uuid'],
            a.get('name', ''),
            a.get('extra_metadata', {}).get('fileName', ''),
            a.get('document_extension', ''),
            a.get('url', ''),
            a.get('attachment_type', ''),
            a.get('attachment_subtype', ''),
            ''  # blake2b_hash
        )
        for a in attachments_data
    ]

    execute_values(
        cursor,
        """INSERT INTO ny_puc_data.attachments
           (parent_filing_uuid, attachment_title, attachment_file_name,
            attachment_file_extension, attachment_url, attachment_type,
            attachment_subtype, donloaded_file_blake2b_hash)
           VALUES %s""",
        attachment_values,
        page_size=1000
    )

def bulk_insert_relations(cursor, relations_data, relation_type):
    """Bulk insert junction table relations"""
    if not relations_data:
        return

    if relation_type == 'docket_petitioned_by_org':
        execute_values(
            cursor,
            """INSERT INTO ny_puc_data.docket_petitioned_by_org
               (docket_uuid, petitioner_uuid)
               VALUES %s ON CONFLICT DO NOTHING""",
            relations_data,
            page_size=1000
        )
    elif relation_type == 'individual_offical_party_to_docket':
        execute_values(
            cursor,
            """INSERT INTO ny_puc_data.individual_offical_party_to_docket
               (docket_uuid, individual_uuid, party_email_contact,
                party_phone_contact, employed_by_org, representing_org_uuid)
               VALUES %s ON CONFLICT DO NOTHING""",
            relations_data,
            page_size=1000
        )
    elif relation_type == 'filings_filed_by_individual':
        execute_values(
            cursor,
            """INSERT INTO ny_puc_data.filings_filed_by_individual
               (filing_uuid, individual_uuid)
               VALUES %s ON CONFLICT DO NOTHING""",
            relations_data,
            page_size=1000
        )
    elif relation_type == 'filings_on_behalf_of_org':
        execute_values(
            cursor,
            """INSERT INTO ny_puc_data.filings_on_behalf_of_org_relation
               (filing_uuid, author_organization_uuid)
               VALUES %s ON CONFLICT DO NOTHING""",
            relations_data,
            page_size=1000
        )

def main():
    if len(sys.argv) < 2:
        print("Usage: python upload_script.py <json_file> [batch_size]")
        sys.exit(1)

    json_file = sys.argv[1]
    batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 100

    with open(json_file, 'r') as f:
        cases = json.load(f)

    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()

    try:
        total_cases = len(cases)
        print(f"Total cases to process: {total_cases}")
        print(f"Batch size: {batch_size}")

        for batch_start in range(0, total_cases, batch_size):
            batch_end = min(batch_start + batch_size, total_cases)
            batch_cases = cases[batch_start:batch_end]

            print(f"\n{'='*60}")
            print(f"Processing batch {batch_start//batch_size + 1}: Cases {batch_start + 1}-{batch_end}")
            print(f"{'='*60}")

            # 1. Collect all unique organizations
            org_names = set()
            for case in batch_cases:
                if case.get('petitioner'):
                    org_names.add(case['petitioner'].strip())
                for party in case.get('case_parties', []):
                    # Collect both employed_by and representing orgs
                    if party.get('individual_associated_company'):
                        org_names.add(party['individual_associated_company'].strip())
                for filing in case.get('filings', []):
                    org_blob = filing.get('organization_authors_blob', '')
                    if org_blob:
                        org_names.add(org_blob.strip())

            print(f"Inserting {len(org_names)} unique organizations...")
            org_uuid_map = bulk_insert_organizations(cursor, org_names)

            # 2. Collect all unique individuals
            individuals_data = []
            individuals_seen = set()
            for case in batch_cases:
                for party in case.get('case_parties', []):
                    if party.get('artifical_person_type') == 'individual':
                        # Use correct field names from scraped data
                        first = party.get('western_individual_first_name', '')
                        last = party.get('western_individual_last_name', '')
                        key = (first, last)
                        if key not in individuals_seen and (first or last):
                            individuals_seen.add(key)
                            individuals_data.append({
                                'first_name': first,
                                'last_name': last,
                                'contact_email': party.get('contact_email', ''),
                                'contact_phone': party.get('contact_phone', '')
                            })
                for filing in case.get('filings', []):
                    for author in filing.get('individual_authors', []):
                        if author:
                            # Author is just a name string, parse it into first and last
                            first, last = parse_name(author)
                            key = (first, last)
                            if key not in individuals_seen:
                                individuals_seen.add(key)
                                individuals_data.append({
                                    'first_name': first,
                                    'last_name': last,
                                    'contact_email': '',
                                    'contact_phone': ''
                                })

            print(f"Inserting {len(individuals_data)} unique individuals...")
            individual_uuid_map = bulk_insert_individuals(cursor, individuals_data)

            # 3. Bulk insert dockets
            print(f"Inserting {len(batch_cases)} dockets...")
            docket_uuid_map = bulk_insert_dockets(cursor, batch_cases)

            # 4. Prepare and insert all filings
            filings_data = []
            filing_metadata = []  # Keep track of which case/filing for attachments
            for case in batch_cases:
                docket_govid = case['case_govid']
                for filing in case.get('filings', []):
                    filing['docket_govid'] = docket_govid
                    filings_data.append(filing)
                    filing_metadata.append({
                        'case': case,
                        'filing': filing
                    })

            print(f"Inserting {len(filings_data)} filings...")
            filing_uuids = bulk_insert_filings(cursor, filings_data, docket_uuid_map)

            # 5. Prepare and insert all attachments
            attachments_data = []
            for i, metadata in enumerate(filing_metadata):
                filing_uuid = filing_uuids[i]
                for attachment in metadata['filing'].get('attachments', []):
                    attachment['filing_uuid'] = filing_uuid
                    attachments_data.append(attachment)

            print(f"Inserting {len(attachments_data)} attachments...")
            bulk_insert_attachments(cursor, attachments_data)

            # 6. Prepare and insert all relations
            docket_org_relations = []
            individual_party_relations = []
            filing_individual_relations = []
            filing_org_relations = []

            for case in batch_cases:
                docket_uuid = docket_uuid_map[case['case_govid']]

                # Docket petitioner relations
                if case.get('petitioner'):
                    org_uuid = org_uuid_map.get(case['petitioner'].strip())
                    if org_uuid:
                        docket_org_relations.append((docket_uuid, org_uuid))

                # Individual party relations
                for party in case.get('case_parties', []):
                    if party.get('artifical_person_type') == 'individual':
                        first = party.get('western_individual_first_name', '')
                        last = party.get('western_individual_last_name', '')
                        key = (first, last)
                        human_uuid = individual_uuid_map.get(key)
                        if human_uuid:
                            # Handle both employed_by and representing_org
                            employed_by_org_uuid = None
                            representing_org_uuid = None

                            if party.get('individual_associated_company'):
                                org_name = party['individual_associated_company'].strip()
                                # For now, use same org for both employed_by and representing
                                # In future, might want to distinguish these
                                org_uuid = org_uuid_map.get(org_name)
                                employed_by_org_uuid = org_uuid
                                representing_org_uuid = org_uuid

                            individual_party_relations.append((
                                docket_uuid,
                                human_uuid,
                                party.get('contact_email', ''),
                                party.get('contact_phone', ''),
                                employed_by_org_uuid,
                                representing_org_uuid
                            ))

            # Filing author relations
            for i, metadata in enumerate(filing_metadata):
                filing_uuid = filing_uuids[i]
                filing = metadata['filing']

                # Individual authors (parse name and lookup by (first, last))
                for author in filing.get('individual_authors', []):
                    first, last = parse_name(author)
                    key = (first, last)
                    human_uuid = individual_uuid_map.get(key)
                    if human_uuid:
                        filing_individual_relations.append((filing_uuid, human_uuid))

                # Organization authors
                org_blob = filing.get('organization_authors_blob', '')
                if org_blob:
                    org_uuid = org_uuid_map.get(org_blob.strip())
                    if org_uuid:
                        filing_org_relations.append((filing_uuid, org_uuid))

            print(f"Inserting {len(docket_org_relations)} docket-org relations...")
            bulk_insert_relations(cursor, docket_org_relations, 'docket_petitioned_by_org')

            print(f"Inserting {len(individual_party_relations)} individual-party relations...")
            bulk_insert_relations(cursor, individual_party_relations, 'individual_offical_party_to_docket')

            print(f"Inserting {len(filing_individual_relations)} filing-individual relations...")
            bulk_insert_relations(cursor, filing_individual_relations, 'filings_filed_by_individual')

            print(f"Inserting {len(filing_org_relations)} filing-org relations...")
            bulk_insert_relations(cursor, filing_org_relations, 'filings_on_behalf_of_org')

            # Commit batch
            conn.commit()
            print(f"\n✅ Batch {batch_start//batch_size + 1} committed successfully!")

        print(f"\n{'='*60}")
        print("✅ All data inserted successfully!")
        print(f"{'='*60}")

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
