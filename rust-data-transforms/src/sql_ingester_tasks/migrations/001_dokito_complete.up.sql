-- Enable pgcrypto so we can use gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Organizations
CREATE TABLE public.organizations (
  uuid uuid NOT NULL DEFAULT gen_random_uuid(),
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  name text NOT NULL DEFAULT ''::text,
  aliases text[] NOT NULL DEFAULT '{}'::text[],
  description text NOT NULL DEFAULT ''::text,
  org_suffix text NOT NULL DEFAULT ''::text,
  CONSTRAINT organizations_pkey PRIMARY KEY (uuid)
);
ALTER TABLE public.organizations ENABLE ROW LEVEL SECURITY;

-- Humans
CREATE TABLE public.humans (
  uuid uuid NOT NULL DEFAULT gen_random_uuid(),
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  name text NOT NULL,
  western_first_name text NOT NULL,
  western_last_name text NOT NULL,
  contact_emails text[] NOT NULL,
  contact_phone_numbers text[] NOT NULL,
  CONSTRAINT humans_pkey PRIMARY KEY (uuid)
);
ALTER TABLE public.humans ENABLE ROW LEVEL SECURITY;

-- Dockets
CREATE TABLE public.dockets (
  uuid uuid NOT NULL DEFAULT gen_random_uuid(),
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  docket_govid text NOT NULL DEFAULT ''::text UNIQUE,
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
  CONSTRAINT dockets_pkey PRIMARY KEY (uuid)
);
ALTER TABLE public.dockets ENABLE ROW LEVEL SECURITY;

-- Fillings
CREATE TABLE public.filings (
  uuid uuid NOT NULL DEFAULT gen_random_uuid(),
  docket_uuid uuid NOT NULL,
  docket_govid text NOT NULL DEFAULT ''::text,
  individual_author_strings text[] NOT NULL DEFAULT '{}'::text[],
  organization_author_strings text[] NOT NULL DEFAULT '{}'::text[],
  filed_date date NOT NULL,
  filing_type text NOT NULL DEFAULT ''::text,
  filing_name text NOT NULL DEFAULT ''::text,
  filing_description text NOT NULL DEFAULT ''::text,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  filing_govid text NOT NULL DEFAULT ''::text,
  openscrapers_id text NOT NULL UNIQUE,
  CONSTRAINT filings_pkey PRIMARY KEY (uuid),
  CONSTRAINT filings_docket_uuid_fkey FOREIGN KEY (docket_uuid) REFERENCES public.dockets(uuid) ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE public.filings ENABLE ROW LEVEL SECURITY;

-- Attachments
CREATE TABLE public.attachments (
  uuid uuid NOT NULL DEFAULT gen_random_uuid(),
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  file_hash_if_downloaded text NOT NULL DEFAULT ''::text,
  parent_filing_uuid uuid NOT NULL,
  attachment_file_extension text NOT NULL DEFAULT ''::text,
  attachment_file_name text NOT NULL DEFAULT ''::text,
  attachment_title text NOT NULL DEFAULT ''::text,
  attachment_type text NOT NULL DEFAULT ''::text,
  attachment_subtype text NOT NULL DEFAULT ''::text,
  attachment_url text NOT NULL DEFAULT ''::text,
  openscrapers_id text NOT NULL UNIQUE,
  CONSTRAINT attachments_pkey PRIMARY KEY (uuid),
  CONSTRAINT attachments_parent_filing_uuid_fkey FOREIGN KEY (parent_filing_uuid) REFERENCES public.filings(uuid) ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE public.attachments ENABLE ROW LEVEL SECURITY;

-- Docket petitioned by org
CREATE TABLE public.docket_petitioned_by_org (
  uuid uuid NOT NULL DEFAULT gen_random_uuid(),
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  docket_uuid uuid NOT NULL DEFAULT gen_random_uuid(),
  petitioner_uuid uuid NOT NULL DEFAULT gen_random_uuid(),
  CONSTRAINT docket_petitioned_by_org_pkey PRIMARY KEY (uuid),
  CONSTRAINT fk_docket_petitioned_by_org_docket_uuid FOREIGN KEY (docket_uuid) REFERENCES public.dockets(uuid) ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT fk_docket_petitioned_by_org_petitioner_uuid FOREIGN KEY (petitioner_uuid) REFERENCES public.organizations(uuid) ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE public.docket_petitioned_by_org ENABLE ROW LEVEL SECURITY;

-- Fillings filed by individual
CREATE TABLE public.filings_filed_by_individual (
  uuid uuid NOT NULL DEFAULT gen_random_uuid(),
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  human_uuid uuid NOT NULL,
  filing_uuid uuid NOT NULL,
  CONSTRAINT filings_filed_by_individual_pkey PRIMARY KEY (uuid),
  CONSTRAINT filings_filed_by_individual_human_uuid_fkey FOREIGN KEY (human_uuid) REFERENCES public.humans(uuid) ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT filings_filed_by_individual_filing_uuid_fkey FOREIGN KEY (filing_uuid) REFERENCES public.filings(uuid) ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE public.filings_filed_by_individual ENABLE ROW LEVEL SECURITY;


-- Fillings on behalf of org relation
CREATE TABLE public.filings_on_behalf_of_org_relation (
  relation_uuid uuid NOT NULL DEFAULT gen_random_uuid(),
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  filing_uuid uuid NOT NULL DEFAULT gen_random_uuid(),
  author_organization_uuid uuid NOT NULL DEFAULT gen_random_uuid(),
  CONSTRAINT filings_on_behalf_of_org_relation_pkey PRIMARY KEY (relation_uuid),
  CONSTRAINT filings_organization_authors_rel_author_organization_uuid_fkey FOREIGN KEY (author_organization_uuid) REFERENCES public.organizations(uuid) ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT filings_organization_authors_relation_filing_uuid_fkey FOREIGN KEY (filing_uuid) REFERENCES public.filings(uuid) ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE public.filings_on_behalf_of_org_relation ENABLE ROW LEVEL SECURITY;

-- Individual offical party to docket
CREATE TABLE public.individual_offical_party_to_docket (
  uuid uuid NOT NULL DEFAULT gen_random_uuid(),
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  docket_uuid uuid NOT NULL,
  individual_uuid uuid NOT NULL,
  representing_org_uuid uuid,
  party_email_contact text NOT NULL,
  party_phone_contact text NOT NULL,
  employed_by_org uuid,
  CONSTRAINT individual_offical_party_to_docket_pkey PRIMARY KEY (uuid),
  CONSTRAINT individual_offical_party_to_docket_employed_by_org_fkey FOREIGN KEY (employed_by_org) REFERENCES public.organizations(uuid) ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT individual_offical_party_to_docket_docket_uuid_fkey FOREIGN KEY (docket_uuid) REFERENCES public.dockets(uuid) ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT individual_offical_party_to_docket_individual_uuid_fkey FOREIGN KEY (individual_uuid) REFERENCES public.humans(uuid) ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT individual_offical_party_to_docket_representing_org_uuid_fkey FOREIGN KEY (representing_org_uuid) REFERENCES public.organizations(uuid) ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE public.individual_offical_party_to_docket ENABLE ROW LEVEL SECURITY;
