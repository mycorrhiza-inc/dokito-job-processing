# Utility Commission Scrapers

A collection of Node.js/TypeScript web scrapers for various utility commission websites that extract case information, documents, and party details with parallel processing, intelligent caching, and S3 storage integration.

## Available Scrapers

- **NY PUC**: New York Public Utilities Commission scraper with S3 integration
- **CO PUC**: Colorado Public Utilities Commission scraper
- **Utah Coal**: Utah coal mine data scraper

## Features

- **Multiple Scraping Modes**: Extract full case details, metadata only, documents only, or parties only
- **S3 Storage Integration**: Save and retrieve HTML snapshots from S3 for faster reprocessing
- **Intelligent Caching**: Blake2 hash-based caching with parsed data storage
- **Parallel Processing**: Process multiple cases simultaneously with configurable concurrency (default: 4 browser windows)
- **Task Queue Management**: Intelligent queuing system prevents resource overload
- **Flexible Input**: Support for comma-separated IDs, file input, date-based scraping, or today's filings
- **JSON Output**: Save results to structured JSON files or display in console
- **Browser Automation**: Uses Playwright with Chromium for reliable data extraction
- **Metadata Enhancement**: Concurrent fetching of additional filing metadata

## Installation

You can run these scrapers using Nix flakes for a reproducible environment with all dependencies:

```bash
# List available scrapers
nix flake show

# Run NY PUC scraper (default)
nix run . -- --mode all --gov-ids "14-M-0094,18-M-0084" -o results.json

# Run specific scrapers
nix run .#ny-puc -- --mode all --gov-ids "14-M-0094,18-M-0084" -o results.json
nix run .#co-puc -- --mode all --gov-ids "22AL-0530E" -o co_results.json
nix run .#utah-coal -- --index-only -o utah_results.json
```

### Alternative: Local Development

```bash
npm install
# or
pnpm install
```

## Usage

### Using Nix (Recommended)

```bash
# NY PUC scraper
nix run .#ny-puc -- [OPTIONS]

# Colorado PUC scraper
nix run .#co-puc -- [OPTIONS]

# Utah coal scraper
nix run .#utah-coal -- [OPTIONS]
```

### Direct TypeScript Execution

```bash
npx ts-node playwright/ny_puc_scraper.spec.ts [OPTIONS]
npx ts-node playwright/co_puc_scraper.copied-spec.ts [OPTIONS]
npx ts-node playwright/utah_coal_grand_scraper.spec.ts [OPTIONS]
```

### Command Line Options

| Flag | Long Form | Description | Example |
|------|-----------|-------------|---------|
| `--mode` | | Scraping mode (required) | `--mode all` |
| `--gov-ids` | | Comma-separated government IDs | `--gov-ids "14-M-0094,18-M-0084"` |
| `--from-file` | | Read government IDs from file | `--from-file gov_ids.txt` |
| `--date` | | Scrape cases for specific date | `--date "09/10/2025"` |
| `--begin-date` | | Start date for date range scraping | `--begin-date "01/01/2024"` |
| `--end-date` | | End date for date range scraping | `--end-date "12/31/2024"` |
| `--today-filings` | | Scrape today's filings automatically | `--today-filings` |
| `--fetch-all-docket-metadata` | | Fetch all available docket metadata | `--fetch-all-docket-metadata` |
| `--intermediate-dir` | | Directory for intermediate file storage | `--intermediate-dir ./cache` |
| `--source-s3` | | Use S3 as source instead of live scraping | `--source-s3` |
| `--headed` | | Run browser in headed mode (visible) | `--headed` |
| `-o` | `--outfile` | Save results to JSON file | `-o results.json` |

### Scraping Modes

| Mode | Description | Output |
|------|-------------|--------|
| `all` | Complete case details with documents and parties | Full RawGenericDocket objects |
| `meta` | Case metadata only | Basic case information |
| `filing` | Documents and filings only | Array of RawGenericFiling objects |
| `parties` | Case parties only | Array of RawGenericParty objects |
| `ids_only` | Return case IDs without additional scraping | Basic case information |

## Examples

### 1. Full Case Details with File Input

Extract complete information for all cases listed in `gov_ids.txt`:

```bash
npx ts-node playwright/ny_puc_scraper.spec.ts --mode all --from-file gov_ids.txt -o full_cases.json
```

### 2. Documents Only for Specific Cases

Extract only documents for specific government IDs:

```bash
npx ts-node playwright/ny_puc_scraper.spec.ts --mode filing --gov-ids "14-M-0094,18-M-0084,24-E-0165" -o documents.json
```

### 3. Parties Information

Get party information for cases in file:

```bash
npx ts-node playwright/ny_puc_scraper.spec.ts --mode parties --from-file gov_ids.txt -o parties.json
```

### 4. Metadata Only (Quick Overview)

Get basic case information without heavy document/party extraction:

```bash
npx ts-node playwright/ny_puc_scraper.spec.ts --mode meta --gov-ids "20-E-0197,18-E-0071" -o metadata.json
```

### 5. Today's Filings Workflow

Automatically find and scrape all cases with filings from today:

```bash
npx ts-node playwright/ny_puc_scraper.spec.ts --today-filings --mode all -o today_filings.json
```

### 6. Date Range Scraping

Get all cases with filings between two dates:

```bash
npx ts-node playwright/ny_puc_scraper.spec.ts --mode all --begin-date "01/01/2024" --end-date "01/31/2024" -o january_filings.json
```

### 7. S3 Source Mode (Reprocessing)

Reprocess previously scraped data from S3 storage:

```bash
npx ts-node playwright/ny_puc_scraper.spec.ts --mode all --source-s3 --gov-ids "14-M-0094,18-M-0084" -o reprocessed.json
```

### 8. With Intermediate Storage

Save HTML snapshots and cache parsed data:

```bash
npx ts-node playwright/ny_puc_scraper.spec.ts --mode all --gov-ids "14-M-0094" --intermediate-dir ./cache -o results.json
```

### 9. All Available Dockets

Fetch metadata for all available dockets (slow but comprehensive):

```bash
npx ts-node playwright/ny_puc_scraper.spec.ts --mode ids_only --fetch-all-docket-metadata -o all_dockets.json
```

### 10. Console Output (No File)

Display results in console without saving to file:

```bash
npx ts-node playwright/ny_puc_scraper.spec.ts --mode meta --gov-ids "15-E-0751"
```

## Input File Format

Create a `gov_ids.txt` file with one government ID per line:

```text
# NY PUC government IDs for scraping
# Lines starting with # are ignored
14-M-0094
18-M-0084
18-E-0130
24-E-0165
20-E-0197
18-E-0071
15-E-0751
21-E-0629
```

## Performance & Caching

### Intelligent Caching System

The scraper implements a sophisticated caching system to avoid redundant work:

- **Blake2 Hash-based**: Each HTML snapshot is hashed to detect changes
- **Metadata Storage**: Parsed data is cached alongside raw HTML
- **Cache Hit/Miss Logging**: Clear feedback on cache performance
- **S3 Integration**: Seamless storage and retrieval from S3

### Concurrency & Task Management

The scraper uses intelligent task queuing with a default maximum of 4 concurrent browser windows:

- **Resource Management**: Prevents system overload by limiting concurrent browsers
- **Dynamic Queue**: New tasks start automatically as others complete
- **Progress Tracking**: Real-time logging of task progress
- **Error Handling**: Failed tasks don't stop the entire process

### Example Progress Output:
```
Cache HIT for search-results at https://documents.dps.ny.gov/... (hash: a1b2c3d4e5f6...)
Cache MISS for case-filings at https://documents.dps.ny.gov/... (hash: f6e5d4c3b2a1...)
Scraping documents for 8 cases with max 4 concurrent browsers
Starting task 1/8 (4 running, max: 4)
Starting task 2/8 (4 running, max: 4)
Starting task 3/8 (4 running, max: 4)
Starting task 4/8 (4 running, max: 4)
Completed task 1/8 (3 still running)
Starting task 5/8 (4 running, max: 4)
```

## Output Examples

### Console Success Output:
```
✅ Successfully saved 8 results to results.json
📊 Mode: all, File size: 245.67 KB
```

### JSON Structure (all mode):
```json
[
  {
    "case_govid": "14-M-0094",
    "case_name": "In the Matter of Staff's Proposal...",
    "case_url": "https://documents.dps.ny.gov/public/MatterManagement/CaseMaster.aspx?MatterCaseNo=14-M-0094",
    "opened_date": "2014-03-15T00:00:00.000Z",
    "case_type": "Matter Type",
    "case_subtype": "Investigation",
    "petitioner": "Petitioner Name",
    "industry": "Electric",
    "filings": [
      {
        "name": "Document Title",
        "filed_date": "2014-03-20T00:00:00.000Z",
        "filing_type": "Filing Type",
        "filing_govid": "123456",
        "filing_url": "https://documents.dps.ny.gov/public/MatterManagement/MatterFilingItem.aspx?FilingSeq=123456",
        "description": "Enhanced description from metadata",
        "individual_authors": ["John Doe"],
        "organization_authors_blob": "Organization Name",
        "attachments": [
          {
            "name": "attachment.pdf",
            "url": "https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=12345",
            "document_extension": "pdf",
            "attachment_type": "primary"
          }
        ]
      }
    ],
    "case_parties": [
      {
        "name": "John Doe",
        "artifical_person_type": "Human",
        "western_human_first_name": "John",
        "western_human_last_name": "Doe",
        "human_title": "Director",
        "human_associated_company": "Example Corp",
        "contact_email": "john.doe@example.com",
        "contact_phone": "(555) 123-4567",
        "contact_address": "123 Main St, Albany, NY"
      }
    ]
  }
]
```

## S3 Integration

The scraper supports S3 storage for HTML snapshots and metadata:

### Storage Structure
```
s3://bucket/raw/ny/puc/{hostname}/{path}/
├── page.html\?query=value--2024-01-15T10:30:00Z--search-results.html
├── page.html\?query=value--2024-01-15T10:35:00Z--case-filings.html
└── metadata.json
```

### Metadata Format
```json
{
  "filename.html": {
    "filename": "filename.html",
    "blake2_hash": "a1b2c3d4e5f6...",
    "url": "https://documents.dps.ny.gov/...",
    "stage": "search-results",
    "saved_at": "2024-01-15T10:30:00Z",
    "file_size": 12345,
    "parsed_data": { /* cached parsing results */ }
  }
}
```

## Error Handling

- **Network Issues**: Automatic retry with exponential backoff (up to 3 attempts)
- **File Operations**: Clear error messages for file read/write issues
- **Invalid Arguments**: Helpful validation messages
- **Browser Crashes**: Individual task failures don't stop the entire process
- **Cache Corruption**: Graceful fallback to live scraping if cache is invalid
- **S3 Connectivity**: Automatic fallback to local storage if S3 is unavailable

## Advanced Features

### Concurrent Metadata Enhancement
The scraper fetches additional filing metadata concurrently to enrich the base data:
- Description enhancement from filing detail pages
- Author name formatting and parsing
- Date standardization and validation

### Intelligent Window Management
- Reuses browser windows when possible to reduce overhead
- Automatic cleanup of failed contexts
- Memory-efficient tab management

### Blake2 Hash Verification
- Content integrity checking using Blake2 cryptographic hashes
- Automatic cache invalidation when content changes
- Duplicate detection and deduplication

## Troubleshooting

### Common Issues:

1. **Permission Denied**: Ensure write permissions for output directory and intermediate storage
2. **Network Timeouts**: NY PUC website may be slow; scraper has 180-second timeouts for navigation
3. **Too Many Browsers**: Reduce concurrency if system resources are limited (default: 4)
4. **Invalid Government IDs**: Check ID format (e.g., "14-M-0094", "18-E-0130")
5. **S3 Access**: Ensure proper AWS credentials are configured for S3 operations
6. **Cache Issues**: Delete intermediate directory if experiencing cache corruption

### Debug Mode:

Use `--headed` flag to run browsers in visible mode for debugging navigation issues:

```bash
npx ts-node playwright/ny_puc_scraper.spec.ts --mode all --gov-ids "14-M-0094" --headed
```

### Performance Optimization:

- Use `--intermediate-dir` for faster subsequent runs with caching
- Use `--source-s3` for reprocessing without hitting the website
- Use `--mode ids_only` for quick metadata-only runs

## Contributing

1. Follow existing code patterns
2. Add error handling for new features  
3. Update this README for any new command-line options
4. Test with various government ID formats

## License

[Add your license information here]