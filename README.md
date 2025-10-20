# Dokito Job Processing - Full Pipeline API

A complete pipeline system that runs scrapers, processes data, and uploads results via a simple REST API.

## Quick Start

### 1. Set Environment Variables

```bash
# Scraper binary paths
export OPENSCRAPER_PATH_NYPUC=/path/to/nypuc-scraper
export OPENSCRAPER_PATH_COPUC=/path/to/copuc-scraper
export OPENSCRAPER_PATH_UTAHCOAL=/path/to/utahcoal-scraper

# Dokito processing binaries
export DOKITO_PROCESS_DOCKETS_BINARY_PATH=/path/to/processor
export DOKITO_UPLOAD_DOCKETS_BINARY_PATH=/path/to/uploader
export DOKITO_DOWNLOAD_ATTACHMENTS_BINARY_PATH=/path/to/downloader
```

### 2. Build and Run

```bash
cd runner
go build -o dokito-runner
./dokito-runner -port 8080
```

### 3. Run Full Pipeline

```bash
curl -X POST http://localhost:8080/api/pipeline/full \
  -H 'Content-Type: application/json' \
  -d '{"gov_id": "14-M-0094"}'
```

## How It Works

The `/api/pipeline/full` endpoint orchestrates the complete pipeline:

1. **🎭 Scraper Selection**: Determines which scraper to use for the GovID
2. **📄 Scraping**: Executes `scraper_binary govid ALL` and captures JSON output
3. **✅ Validation**: Ensures output deserializes to `[]map[string]interface{}`
4. **🔧 Processing**: Pipes data through dokito processing binary
5. **📤 Upload**: Sends final results to upload binary

## API Response

```json
{
  "success": true,
  "gov_id": "14-M-0094",
  "scraper_type": "nypuc",
  "scrape_count": 25,
  "process_count": 25,
  "message": "Full pipeline completed successfully for 14-M-0094. Scraped 25 items, processed 25 items."
}
```

## Error Handling

The API provides detailed error messages for each stage:
- Scraper execution failures
- JSON validation errors
- Processing binary errors
- Upload failures

## GovID Mapping

The system uses a configurable mapping system (no regex patterns) with:
- Direct GovID mappings for exceptions
- Fallback to default scraper (NYPUC)
- Easy to extend for new scrapers

## Health Check

```bash
curl http://localhost:8080/api/health
```

## Architecture

- **Single binary**: Everything in one executable
- **Environment-driven**: All paths configured via env vars
- **Stateless**: No queue management, just immediate processing
- **Error-resilient**: Comprehensive error handling at each stage