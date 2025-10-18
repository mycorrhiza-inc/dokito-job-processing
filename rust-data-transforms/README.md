# Rust Data Transforms

A Rust library and binary for processing government docket data, converting raw scraped data into structured formats and managing storage across S3 and PostgreSQL.

## Core Components

### 1. Data Type System & Processing Pipeline

#### Raw Data Types (from `openscraper_types`)
- **`RawGenericDocket`** - Raw docket data from scrapers with unprocessed fields
- **`RawGenericFiling`** - Individual court filings within a docket
- **`RawGenericAttachment`** - Document attachments (PDFs, images, etc.)
- **`RawGenericParty`** - Case participants (humans and organizations)

#### Processed Data Types (`src/types/processed.rs`)
- **`ProcessedGenericDocket`** - Cleaned docket with normalized fields, UUID assignment, and metadata
- **`ProcessedGenericFiling`** - Structured filing with author associations and attachment links
- **`ProcessedGenericAttachment`** - Validated attachments with hash verification and file metadata
- **`ProcessedGenericHuman`** - Extracted person entities with name parsing and contact info
- **`ProcessedGenericOrganization`** - Normalized organization entities with type classification

#### Data Transformation (`src/openscraper_data_traits.rs`)
Key transformation methods:
- **`ProcessFrom<RawGenericDocket>`** - Converts raw dockets to processed format
- **`Revalidate`** - Updates existing processed data with missing fields (UUIDs, subtypes)
- **LLM Integration** - Uses AI to clean organization names and extract entities
- **Database Association** - Links authors to existing database records
- **Attachment Validation** - Verifies file hashes and downloads from S3

#### Database Integration (`src/sql_ingester_tasks/`)
- **`nypuc_ingest.rs`** - Bulk ingestion tasks for specific jurisdictions
- **`database_author_association.rs`** - Author entity resolution and linking
- **`recreate_dokito_table_schema.rs`** - Schema management and data purging
- **PostgreSQL Operations** - CRUD operations with conflict resolution

### 2. S3 Storage Management (`src/types/s3_stuff.rs`)

#### Canonical S3 Locations
Each data type maps to specific S3 paths:
- **Raw Attachments**: `raw/metadata/{hash}.json` + `raw/file/{hash}`
- **Processed Dockets**: `objects/{country}/{state}/{jurisdiction}/{docket_id}`
- **Raw Dockets**: `objects_raw/{country}/{state}/{jurisdiction}/{docket_id}`

#### Key S3 Operations
- **`fetch_attachment_file_from_s3(hash)`** - Download file by hash with integrity checking
- **`does_openscrapers_attachment_exist(hash)`** - Verify both metadata and file exist
- **`list_processed_cases_for_jurisdiction()`** - Directory listing for bulk operations
- **`push_raw_attach_file_to_s3()`** - Upload with automatic key generation

#### Data Integrity Features
- **Hash-based Addressing** - Blake2b hashes ensure content integrity
- **Metadata Separation** - File content and metadata stored separately
- **Resumable Operations** - Failed uploads can be retried
- **Hierarchical Organization** - Geographic and jurisdictional structure

## Architecture

**Library Structure**:
- `rust_data_transforms` - Library exposing all processing functionality
- `rust-data-transforms` - Binary providing HTTP API and background workers

**Key Processing Traits** (`src/data_processing_traits.rs`):
- **`ProcessFrom<T>`** - Convert raw data to processed with error handling
- **`Revalidate`** - Update existing data with improvements
- **`DownloadIncomplete`** - Resume partial processing operations

**Background Tasks**:
- Automatic processing queue for new raw dockets
- Batch operations for jurisdiction-wide ingestion
- LLM-powered entity extraction and normalization
- Database synchronization with conflict resolution

### Prerequisites

Install Nix and direnv:

```bash
# Install Nix
curl --proto '=https' --tlsv1.2 -sSf -L https://install.determinate.systems/nix | sh -s -- install

# Install direnv (Ubuntu/Debian)
curl -sfL https://direnv.net/install.sh | bash

# Add direnv hook to your shell (.bashrc/.zshrc)
eval "$(direnv hook bash)"  # or zsh
```

## Environment Variables

Copy `example.env` to `.env` and configure:

- `DATABASE_URL` - PostgreSQL connection string
- `DIGITALOCEAN_S3_ACCESS_KEY` - DigitalOcean Spaces access key
- `DIGITALOCEAN_S3_SECRET_KEY` - DigitalOcean Spaces secret key
- `DEEPINFRA_API_KEY` - DeepInfra API key for AI services

See `example.env` for all variables and default values.

# Run

**Production Container:**

```bash
nix run .#build-container
docker run --env-file .env dokito-backend:latest
```

## Development

If you're developing on this project, the direnv setup above automatically provides:

- Rust toolchain (cargo, rustc, rust-analyzer)
- Required system dependencies (OpenSSL, pkg-config)
- Proper environment variables for compilation

Just run `cargo run` or use your IDE after the initial setup.

or alternatively if that isnt working just run:

```bash
devenv shell
```
