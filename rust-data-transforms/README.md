# Rust Data Transforms

A Rust library and CLI suite for processing government docket data, converting raw scraped data into structured formats and managing storage across S3 and PostgreSQL. This project provides three modular CLI binaries for different stages of the data processing pipeline.

## CLI Binaries

### 🔄 **process-dockets** - Raw Data Processing
Converts `RawGenericDocket` to `ProcessedGenericDocket` with full data normalization and entity extraction.

**Usage:**
```bash
echo '{"case_govid": "12345", ...}' | ./target/debug/process-dockets --fixed-jur new_york_puc
cat raw_dockets.json | ./target/debug/process-dockets --fixed-jur colorado_puc > processed_dockets.json
```

**Features:**
- JSON input/output via stdin/stdout
- Handles both single objects and arrays
- LLM-powered organization name cleaning
- Author entity resolution and database linking
- UUID assignment and metadata enrichment
- Date validation and normalization

### 📤 **upload-dockets** - Database Storage
Uploads `ProcessedGenericDocket` data to PostgreSQL with proper schema mapping and conflict resolution.

**Usage:**
```bash
echo '{"case_govid": "12345", ...}' | ./target/debug/upload-dockets --fixed-jur new_york_puc
cat processed_dockets.json | ./target/debug/upload-dockets --fixed-jur california_puc
```

**Features:**
- PostgreSQL upsert operations with conflict handling
- Jurisdiction-specific schema mapping
- Author association with existing database records
- Concurrent processing for performance
- Returns uploaded data for pipeline chaining

### 📥 **download-attachments** - Attachment Processing
Downloads missing attachments and populates file hashes for complete data records.

**Usage:**
```bash
echo '{"filings": [...]}' | ./target/debug/download-attachments
cat dockets_with_attachments.json | ./target/debug/download-attachments > complete_dockets.json
```

**Features:**
- Downloads attachments from URLs
- Generates Blake2b hashes for file integrity
- Stores files in S3 with canonical structure
- Validates file extensions and metadata
- Concurrent downloads with retry logic

## Supported Jurisdictions

All binaries accept the `--fixed-jur` parameter with these values:

- `new_york_puc` - New York Public Utilities Commission
- `colorado_puc` - Colorado Public Utilities Commission
- `california_puc` - California Public Utilities Commission
- `utah_dogm_coal` - Utah Division of Oil, Gas & Mining (Coal)

## Pipeline Usage

The binaries are designed to work together in a processing pipeline:

```bash
# Complete pipeline example
cat raw_input.json \
  | ./target/debug/process-dockets --fixed-jur new_york_puc \
  | ./target/debug/download-attachments \
  | ./target/debug/upload-dockets --fixed-jur new_york_puc \
  > final_result.json
```

## Data Types

### Raw Data Types (Input to process-dockets)
- **`RawGenericDocket`** - Raw docket data from scrapers
- **`RawGenericFiling`** - Individual court filings within a docket
- **`RawGenericAttachment`** - Document attachments with URLs
- **`RawGenericParty`** - Case participants (humans and organizations)

### Processed Data Types (Output from process-dockets)
- **`ProcessedGenericDocket`** - Cleaned docket with normalized fields and UUIDs
- **`ProcessedGenericFiling`** - Structured filing with author associations
- **`ProcessedGenericAttachment`** - Validated attachments with hash verification
- **`ProcessedGenericHuman`** - Extracted person entities with parsed names
- **`ProcessedGenericOrganization`** - Normalized organization entities

## Environment Variables

Copy `example.env` to `.env` and configure:

- `DATABASE_URL` - PostgreSQL connection string (required for upload-dockets)
- `DIGITALOCEAN_S3_ACCESS_KEY` - DigitalOcean Spaces access key
- `DIGITALOCEAN_S3_SECRET_KEY` - DigitalOcean Spaces secret key
- `DEEPINFRA_API_KEY` - DeepInfra API key for LLM services

## Installation & Development

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

### Building

```bash
# Build all binaries
cargo build --release

# Build individual binaries
cargo build --bin process-dockets
cargo build --bin upload-dockets
cargo build --bin download-attachments

# Or use development environment
devenv shell
```

### Testing

```bash
# Check all binaries compile
cargo check --bins

# View help for any binary
./target/debug/process-dockets --help
./target/debug/upload-dockets --help
./target/debug/download-attachments --help
```

## Library Components

The underlying `rust_data_transforms` library provides:

### Core Processing Traits (`src/data_processing_traits.rs`)
- **`ProcessFrom<T>`** - Convert raw data to processed with error handling
- **`Revalidate`** - Update existing data with improvements
- **`DownloadIncomplete`** - Resume partial processing operations

### S3 Storage Management (`src/types/s3_stuff.rs`)
- Hash-based file addressing with Blake2b
- Canonical S3 path structure by jurisdiction
- Metadata separation from file content
- Resumable upload/download operations

### Database Integration (`src/sql_ingester_tasks/`)
- PostgreSQL schema management per jurisdiction
- Author entity resolution and linking
- Bulk ingestion with conflict resolution
- Connection pooling and transaction management

## Architecture

This is a **library-first** design where three focused CLI binaries expose specific functionality:

1. **Data Processing** - Raw → Processed transformation
2. **Database Storage** - PostgreSQL ingestion and management
3. **Attachment Handling** - File downloads and integrity verification

Each binary operates independently and can be used in isolation or as part of a larger processing pipeline. The modular design enables easy integration into automated workflows, batch processing systems, or manual data operations.