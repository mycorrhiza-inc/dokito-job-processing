# JobRunner - Dokito Job Processing System

This is a **containerized job processing system** for legal case data scraping and processing, specifically designed for the New York Public Utilities Commission (NY PUC). The project consists of two main components:

## 1. **Go-based Job Runner** (`main.go`, `api.go`, `load_balancer.go`)
- **Job orchestrator** that manages and distributes scraping tasks
- Supports multiple job types: `scrape`, `dokito`, `pipeline`, `caselist`
- **Load balancer** with round-robin and least-loaded strategies for distributing work across workers
- **REST API** for queue management (add, process, retry, save jobs)
- **Docker integration** to spawn containerized scraper workers
- **Retry logic** with exponential backoff for failed jobs
- **Integration** with a "dokito" backend service for data processing

## 2. **Node.js/Playwright Scraper Container** (`scraper-container.js`, `scraper-wrapper.js`)
- **Containerized web scraper** using Playwright for browser automation
- Scrapes NY PUC case data with multiple modes:
  - `all` - Complete scraping
  - `meta` - Metadata only
  - `docs` - Documents
  - `parties` - Party information
  - `dates` - Date information
- **Cheerio** for HTML parsing
- Outputs scraped data as JSON files

## Key Features:
- **Scalable**: Uses Docker containers for parallel processing
- **Fault-tolerant**: Retry mechanisms and timeout configurations
- **Flexible**: Multiple scraping modes and job types
- **Load-balanced**: Distributes work efficiently across workers
- **Integration-ready**: Built to work with external "dokito" backend service

The system appears designed for large-scale legal document processing, taking government case IDs and extracting structured data from public utility commission websites.

## Usage

Build and run the system:
```bash
make build    # Build both scraper image and runner binary
make run      # Build and run the runner
make test     # Run all tests including integration tests
```

See `make help` for all available commands.