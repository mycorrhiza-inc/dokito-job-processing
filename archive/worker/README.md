# Worker - Redis-Based Job Processing Engine

This is a **Node.js worker service** that processes scraping jobs from a Redis queue. It's designed to work as a distributed worker in the job processing pipeline, complementing the Go-based runner.

## **Core Components:**

### 1. **Redis Job Worker** (`redis-client.js`, `index.js`)
- **Queue processor** that pulls jobs from Redis queues
- **Worker identification** with unique worker IDs
- **Job status tracking** via Redis streams
- **Auto-reconnection** and fault tolerance for Redis connections
- **Concurrent job processing** with configurable limits

### 2. **NY PUC Scraper Engine** (`scraper-wrapper.js`)
- **Playwright-based web scraper** for NY Public Utilities Commission
- **Multiple scraping modes**: full, metadata, documents, parties, dates, case-list
- **Browser management** with tab pooling and caching
- **Page state management** and URL handling
- **Structured data extraction** to JSON format

### 3. **Storage Backends**
- **S3 Client** (`s3-client.js`) - DigitalOcean Spaces integration for artifact storage
- **Supabase Storage** (`supabase-storage-client.js`) - Alternative cloud storage backend
- **Data compression** with gzip for efficient storage
- **Hash-based file naming** to prevent conflicts

### 4. **Integration Services**
- **Dokito Client** (`dokito-client.js`) - HTTP client for backend API communication
- **Log Collectors** (`log-collector.js`, `file-log-collector.js`) - Structured logging with buffering
- **Status reporting** via Redis streams

## **Key Features:**

- **Distributed Architecture**: Workers can run on multiple machines, pulling from shared Redis queues
- **Cloud Storage Integration**: Supports both AWS S3-compatible storage and Supabase
- **Robust Logging**: Structured logging with batching and API integration
- **Browser Automation**: Headless Chrome via Playwright for complex web scraping
- **Data Pipeline**: Seamless integration with the Dokito backend for data processing
- **Fault Tolerance**: Redis reconnection, job retry mechanisms, and error handling

## **Workflow:**
1. Worker connects to Redis and waits for jobs
2. Receives scraping jobs with Gov IDs and parameters
3. Launches Playwright browser to scrape NY PUC data
4. Extracts structured data (cases, documents, parties, etc.)
5. Stores artifacts in cloud storage (S3/Supabase)
6. Reports status and logs back to Redis streams
7. Optionally sends data to Dokito backend for further processing

This worker is designed for **high-throughput legal document processing**, capable of running multiple instances to handle large-scale scraping operations across distributed infrastructure.

## Usage

Start the worker:
```bash
npm install
npm start
```

For development with auto-reload:
```bash
npm run dev
```

## Environment Variables

- `WORKER_ID` - Unique worker identifier
- `REDIS_URL` - Redis connection URL
- `REDIS_JOB_QUEUE` - Job queue name
- `REDIS_STATUS_STREAM` - Status stream name
- `REDIS_LOG_STREAM` - Log stream name
- `SUPABASE_URL` - Supabase project URL
- `SUPABASE_SERVICE_ROLE_KEY` - Supabase service key
- `DIGITALOCEAN_S3_*` - DigitalOcean Spaces configuration