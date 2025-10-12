-- =====================================================
-- NY PUC Data Schema Creation Script
-- =====================================================
-- This script creates all tables in the ny_puc_data schema
-- including all constraints, indexes, and relationships
-- =====================================================

-- Create the schema
CREATE SCHEMA IF NOT EXISTS ny_puc_data;

-- =====================================================
-- Base Tables (no foreign key dependencies)
-- =====================================================

-- Organizations table
CREATE TABLE ny_puc_data.organizations (
    uuid uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    name text NOT NULL DEFAULT ''::text,
    aliases text[] NOT NULL DEFAULT '{}'::text[],
    description text NOT NULL DEFAULT ''::text,
    artifical_person_type text NOT NULL DEFAULT ''::text,
    org_suffix text NOT NULL DEFAULT ''::text
);

-- Humans table
CREATE TABLE ny_puc_data.humans (
    uuid uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    name text NOT NULL,
    western_first_name text NOT NULL,
    western_last_name text NOT NULL,
    contact_emails text[] NOT NULL,
    contact_phone_numbers text[] NOT NULL
);

-- Dockets table
CREATE TABLE ny_puc_data.dockets (
    uuid uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    docket_govid text NOT NULL DEFAULT ''::text,
    docket_subtype text NOT NULL DEFAULT ''::text,
    docket_description text NOT NULL DEFAULT ''::text,
    docket_title text NOT NULL DEFAULT ''::text,
    industry text NOT NULL DEFAULT ''::text,
    hearing_officer text NOT NULL DEFAULT ''::text,
    opened_date date NOT NULL,
    closed_date date,
    current_status text NOT NULL DEFAULT ''::text,
    assigned_judge text NOT NULL DEFAULT ''::text,
    docket_type text NOT NULL DEFAULT ''::text,
    petitioner_strings text[] NOT NULL DEFAULT '{}'::text[],
    CONSTRAINT dockets_docket_govid_key UNIQUE (docket_govid)
);

-- =====================================================
-- Tables with one level of foreign key dependencies
-- =====================================================

-- Filings table (depends on dockets)
CREATE TABLE ny_puc_data.filings (
    uuid uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    docket_uuid uuid NOT NULL,
    docket_govid text NOT NULL DEFAULT ''::text,
    individual_author_strings text[] NOT NULL DEFAULT '{}'::text[],
    organization_author_strings text[] NOT NULL DEFAULT '{}'::text[],
    filed_date date NOT NULL,
    filing_type text NOT NULL DEFAULT ''::text,
    filing_name text NOT NULL DEFAULT ''::text,
    filing_description text NOT NULL DEFAULT ''::text,
    filing_govid text NOT NULL DEFAULT ''::text,
    filing_number text NOT NULL DEFAULT ''::text,
    openscrapers_id text NOT NULL,
    CONSTRAINT filings_openscrapers_id_key UNIQUE (openscrapers_id),
    CONSTRAINT filings_docket_uuid_fkey FOREIGN KEY (docket_uuid)
        REFERENCES ny_puc_data.dockets(uuid)
);

-- =====================================================
-- Junction/Relation Tables
-- =====================================================

-- Attachments table (depends on filings)
CREATE TABLE ny_puc_data.attachments (
    uuid uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    blake2b_hash text NOT NULL DEFAULT ''::text,
    parent_filing_uuid uuid NOT NULL,
    attachment_file_extension text NOT NULL DEFAULT ''::text,
    attachment_file_name text NOT NULL DEFAULT ''::text,
    attachment_title text NOT NULL DEFAULT ''::text,
    attachment_type text NOT NULL DEFAULT ''::text,
    attachment_subtype text NOT NULL DEFAULT ''::text,
    attachment_url text NOT NULL DEFAULT ''::text,
    openscrapers_id text NOT NULL,
    CONSTRAINT attachments_openscrapers_id_key UNIQUE (openscrapers_id),
    CONSTRAINT attachments_parent_filing_uuid_fkey FOREIGN KEY (parent_filing_uuid)
        REFERENCES ny_puc_data.filings(uuid)
);

-- Docket petitioned by organization (junction table)
CREATE TABLE ny_puc_data.docket_petitioned_by_org (
    uuid uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    docket_uuid uuid NOT NULL DEFAULT gen_random_uuid(),
    petitioner_uuid uuid NOT NULL DEFAULT gen_random_uuid(),
    CONSTRAINT fk_docket_petitioned_by_org_docket_uuid FOREIGN KEY (docket_uuid)
        REFERENCES ny_puc_data.dockets(uuid),
    CONSTRAINT fk_docket_petitioned_by_org_petitioner_uuid FOREIGN KEY (petitioner_uuid)
        REFERENCES ny_puc_data.organizations(uuid)
);

-- Filings filed by individual (junction table)
CREATE TABLE ny_puc_data.filings_filed_by_individual (
    uuid uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    human_uuid uuid NOT NULL,
    filing_uuid uuid NOT NULL,
    CONSTRAINT filings_filed_by_individual_human_uuid_fkey FOREIGN KEY (human_uuid)
        REFERENCES ny_puc_data.humans(uuid),
    CONSTRAINT filings_filed_by_individual_filing_uuid_fkey FOREIGN KEY (filing_uuid)
        REFERENCES ny_puc_data.filings(uuid)
);

-- Filings on behalf of organization relation (junction table)
CREATE TABLE ny_puc_data.filings_on_behalf_of_org_relation (
    relation_uuid uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    filing_uuid uuid NOT NULL DEFAULT gen_random_uuid(),
    author_organization_uuid uuid NOT NULL DEFAULT gen_random_uuid(),
    CONSTRAINT filings_organization_authors_relation_filing_uuid_fkey FOREIGN KEY (filing_uuid)
        REFERENCES ny_puc_data.filings(uuid),
    CONSTRAINT filings_organization_authors_rel_author_organization_uuid_fkey FOREIGN KEY (author_organization_uuid)
        REFERENCES ny_puc_data.organizations(uuid)
);

-- Individual official party to docket (complex junction table)
CREATE TABLE ny_puc_data.individual_offical_party_to_docket (
    uuid uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    docket_uuid uuid NOT NULL,
    individual_uuid uuid NOT NULL,
    representing_org_uuid uuid,
    party_email_contact text NOT NULL,
    party_phone_contact text NOT NULL,
    employed_by_org uuid,
    CONSTRAINT individual_offical_party_to_docket_docket_uuid_fkey FOREIGN KEY (docket_uuid)
        REFERENCES ny_puc_data.dockets(uuid),
    CONSTRAINT individual_offical_party_to_docket_individual_uuid_fkey FOREIGN KEY (individual_uuid)
        REFERENCES ny_puc_data.humans(uuid),
    CONSTRAINT individual_offical_party_to_docket_representing_org_uuid_fkey FOREIGN KEY (representing_org_uuid)
        REFERENCES ny_puc_data.organizations(uuid),
    CONSTRAINT individual_offical_party_to_docket_employed_by_org_fkey FOREIGN KEY (employed_by_org)
        REFERENCES ny_puc_data.organizations(uuid)
);

-- =====================================================
-- Indexes
-- =====================================================
-- Note: Primary key and unique constraint indexes are automatically created
-- The following would be additional indexes if needed for performance

-- CREATE INDEX idx_filings_docket_uuid ON ny_puc_data.filings(docket_uuid);
-- CREATE INDEX idx_attachments_parent_filing_uuid ON ny_puc_data.attachments(parent_filing_uuid);
-- CREATE INDEX idx_dockets_opened_date ON ny_puc_data.dockets(opened_date);
-- CREATE INDEX idx_filings_filed_date ON ny_puc_data.filings(filed_date);

-- =====================================================
-- Comments (Optional - for documentation)
-- =====================================================

COMMENT ON SCHEMA ny_puc_data IS 'New York Public Utilities Commission data storage schema';

COMMENT ON TABLE ny_puc_data.dockets IS 'Main docket records from NY PUC';
COMMENT ON TABLE ny_puc_data.filings IS 'Documents filed in relation to dockets';
COMMENT ON TABLE ny_puc_data.attachments IS 'File attachments associated with filings';
COMMENT ON TABLE ny_puc_data.humans IS 'Individual persons involved in proceedings';
COMMENT ON TABLE ny_puc_data.organizations IS 'Organizations/entities involved in proceedings';
COMMENT ON TABLE ny_puc_data.docket_petitioned_by_org IS 'Many-to-many relationship between dockets and petitioning organizations';
COMMENT ON TABLE ny_puc_data.filings_filed_by_individual IS 'Many-to-many relationship between filings and individual authors';
COMMENT ON TABLE ny_puc_data.filings_on_behalf_of_org_relation IS 'Many-to-many relationship between filings and organizational authors';
COMMENT ON TABLE ny_puc_data.individual_offical_party_to_docket IS 'Represents individuals who are official parties to dockets, including their organizational affiliations';
