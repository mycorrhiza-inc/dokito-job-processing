# Dokito Job Processing - Go Coordinator

A powerful Go-based coordinator that orchestrates web scraping, data processing, and upload workflows for public utility commission data. The system supports both CLI and REST API interfaces with flexible pipeline execution modes.

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Web Scrapers  │    │  Data Processor │    │   Database      │
│  (Playwright)   │───▶│    (Rust)       │───▶│   Upload        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │  Go Coordinator │
                    │  • CLI Interface│
                    │  • REST API     │
                    │  • S3 Storage   │
                    └─────────────────┘
```

## 🚀 Quick Start

### Using Nix (Recommended)

```bash
# Run CLI commands
nix run .#cli -- --help

# Start API server
nix run .#server

# Start Redis (for caching)
nix run .#redis
```

## 📋 CLI Interface

The CLI provides powerful debugging and automation capabilities:

### Pipeline Commands

```bash
# Run complete pipeline for a government ID
dokito-cli pipeline 00-F-0229

# Use intermediate data sources for faster processing
dokito-cli pipeline 00-F-0229 --intermediate-source=html
dokito-cli pipeline 00-F-0229 --intermediate-source=raw_json
dokito-cli pipeline 00-F-0229 --intermediate-source=processed_json
```

### Individual Stage Commands

```bash
# Run scraper only
dokito-cli scrape 00-F-0229

# Process existing JSON data
dokito-cli process scraped_data.json

# Upload processed data
dokito-cli upload processed_data.json
```

### Utility Commands

```bash
# Find NY PUC govids not in database
dokito-cli missing-govids

# Show environment configuration
dokito-cli env
```

### Intermediate Source Options

- **`none`** - Full scraping from live websites (default)
- **`html`** - Process from HTML snapshots stored in S3
- **`raw_json`** - Use raw JSON objects from S3 storage
- **`processed_json`** - Use already processed JSON from S3

## 🌐 REST API

### Start the Server

```bash
nix run .#server
# or
dokito-server --port 8080
```

### Health Check

```bash
curl http://localhost:8080/api/health
```

Response:

```json
{
  "status": "ok",
  "timestamp": "2024-10-25T14:30:00Z",
  "services": {
    "nypuc_scraper": "configured",
    "copuc_scraper": "configured",
    "utahcoal_scraper": "configured",
    "process_dockets": "configured",
    "upload_dockets": "configured",
    "database_utils": "configured"
  }
}
```

### Execute Full Pipeline

```bash
curl -X POST http://localhost:8080/api/pipeline/full \
  -H 'Content-Type: application/json' \
  -d '{"gov_id": "00-F-0229"}'
```

Response:

```json
{
  "success": true,
  "gov_id": "00-F-0229",
  "scraper_type": "nypuc",
  "scrape_count": 25,
  "process_count": 25,
  "message": "Full pipeline completed successfully for 00-F-0229. Scraped 25 items, processed 25 items."
}
```

## 🔧 Configuration

### Environment Variables

| Variable                                  | Description                      | Required |
| ----------------------------------------- | -------------------------------- | -------- |
| `OPENSCRAPER_PATH_NYPUC`                  | NY PUC scraper binary path       | ✅       |
| `OPENSCRAPER_PATH_COPUC`                  | Colorado PUC scraper binary path | ✅       |
| `OPENSCRAPER_PATH_UTAHCOAL`               | Utah Coal scraper binary path    | ✅       |
| `DOKITO_PROCESS_DOCKETS_BINARY_PATH`      | Data processing binary path      | ✅       |
| `DOKITO_UPLOAD_DOCKETS_BINARY_PATH`       | Database upload binary path      | ✅       |
| `DOKITO_DOWNLOAD_ATTACHMENTS_BINARY_PATH` | Attachment download binary path  | ✅       |
| `DOKITO_DATABASE_UTILS_BINARY_PATH`       | Database utilities binary path   | ✅       |
| `DATABASE_URL`                            | PostgreSQL connection string     | ✅       |
| `REDIS_URL`                               | Redis connection for caching     | ⚠️       |

### GovID Mapping

The system uses intelligent scraper selection:

- Direct mappings for specific GovIDs (Colorado cases)
- Fallback to NY PUC scraper for most cases
- Easily extensible for new jurisdictions

## 📦 Storage Integration

### S3 Support

The system supports multiple storage backends:

- **Local storage** for development
- **S3-compatible storage** for production
- **Intermediate data caching** for performance

### Data Flow

1. **Raw Data**: Scraped HTML/JSON stored in S3
2. **Processed Data**: Normalized JSON objects
3. **Database Upload**: Final structured data in PostgreSQL

## 🔍 Pipeline Stages

### 1. Scraping Stage

- **Input**: Government ID (e.g., "00-F-0229")
- **Process**: Execute appropriate scraper binary
- **Output**: JSON array of case data
- **Storage**: Raw data saved to S3 and local cache

### 2. Processing Stage

- **Input**: Raw scraped JSON data
- **Process**: Data normalization and enrichment via Rust binary
- **Output**: Structured JSON with consistent schema
- **Storage**: Processed data saved to S3

### 3. Upload Stage

- **Input**: Processed JSON data
- **Process**: Database insertion with conflict resolution
- **Output**: Success/failure status
- **Features**: Author caching via Redis for performance

## 🛠️ Development

### Project Structure

```
runner/
├── cmd/
│   ├── dokito-cli/          # CLI application entry point
│   └── dokito-server/       # REST API server entry point
├── internal/
│   ├── api/                 # REST API handlers
│   ├── core/                # Core business logic
│   ├── pipelines/           # Pipeline implementations
│   └── storage/             # S3 and local storage
├── main.nix                 # Nix build configuration
└── README.md                # This file
```

### Building

```bash
# Using Nix (handles all dependencies)
nix build .#dokito-cli
nix build .#dokito-server

# Using Go directly
go build ./cmd/dokito-cli
go build ./cmd/dokito-server
```

### Testing

```bash
# Run all tests
go test ./...

# Test with coverage
go test -cover ./...
```

## 🚨 Error Handling

The system provides comprehensive error handling:

- **Scraper failures**: Detailed error messages with retry suggestions
- **Processing errors**: Validation failures with data context
- **Upload failures**: Database constraint violations with resolution hints
- **Network issues**: Automatic retries with exponential backoff

## 📊 Monitoring

### Health Checks

- Service availability monitoring
- Binary path validation
- Database connectivity testing
- Redis cache status

### Logging

- Structured JSON logging
- Request/response tracing
- Performance metrics
- Error aggregation

## 🔐 Security Considerations

- Environment variable validation
- Input sanitization for GovIDs
- S3 credential management
- Database connection security

## 🤝 Integration

### External Services

- **Playwright Scrapers**: Web scraping automation
- **Rust Data Processor**: High-performance data transformation
- **PostgreSQL**: Structured data storage
- **Redis**: Caching and session management
- **S3**: Object storage for intermediate data

### API Compatibility

- RESTful JSON APIs
- OpenAPI/Swagger documentation
- Standard HTTP status codes
- Consistent error response format

## 📚 Examples

### Complete Workflow

```bash
# 1. Check system health
dokito-cli env

# 2. Find missing govids
dokito-cli missing-govids

# 3. Process a specific case
dokito-cli pipeline 00-F-0229

# 4. Process with cached data for faster execution
dokito-cli pipeline 00-F-0229 --intermediate-source=html
```

### API Integration

```python
import requests

# Health check
response = requests.get('http://localhost:8080/api/health')
print(response.json())

# Process pipeline
response = requests.post(
    'http://localhost:8080/api/pipeline/full',
    json={'gov_id': '00-F-0229'}
)
print(response.json())
```

