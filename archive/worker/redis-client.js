const { createClient } = require('redis');
const { SupabaseArtifactStorageClient } = require('./supabase-storage-client');
const { FileLogCollector } = require('./file-log-collector');

class RedisJobWorker {
  constructor() {
    this.workerId = process.env.WORKER_ID || `worker-${Math.random().toString(36).substr(2, 9)}`;
    this.redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
    this.jobQueue = process.env.REDIS_JOB_QUEUE || 'jobrunner:jobs';
    this.statusStream = process.env.REDIS_STATUS_STREAM || 'jobrunner:status';
    this.logStream = process.env.REDIS_LOG_STREAM || 'jobrunner:logs';
    this.running = false;
    this.activeJobs = 0;
    this.browser = null;
    this.redis = null;

    // Initialize artifact storage (using Supabase for testing)
    this.artifactStorage = new SupabaseArtifactStorageClient();

    console.log(`Initializing Redis worker ${this.workerId}`);
    console.log(`Redis URL: ${this.redisUrl}`);
    console.log(`Job queue: ${this.jobQueue}`);
  }

  async init() {
    // Wait for Redis to be available before initializing
    await this.waitForRedis();

    // Initialize Redis client
    this.redis = createClient({
      url: this.redisUrl,
      socket: {
        connectTimeout: 10000,
        lazyConnect: true,
        reconnectStrategy: (retries) => {
          console.log(`Redis reconnect attempt ${retries}`);
          if (retries > 10) {
            console.error('Redis reconnection failed after 10 attempts');
            return false;
          }
          return Math.min(retries * 50, 1000);
        }
      }
    });

    // Set up error handling
    this.redis.on('error', (err) => {
      console.error('Redis client error:', err);
    });

    this.redis.on('connect', () => {
      console.log(`Redis worker ${this.workerId} connected`);
    });

    this.redis.on('ready', () => {
      console.log(`Redis worker ${this.workerId} ready`);
    });

    this.redis.on('end', () => {
      console.log(`Redis worker ${this.workerId} disconnected`);
    });

    // Connect to Redis
    await this.redis.connect();

    console.log(`Redis worker ${this.workerId} initialized successfully`);
  }

  async waitForRedis() {
    const maxAttempts = parseInt(process.env.REDIS_WAIT_TIMEOUT) || 60;
    const delayMs = 5000; // 5 seconds between attempts
    
    console.log(`Waiting for Redis to be available: ${this.redisUrl}`);
    
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        // Try to connect to Redis
        const testClient = createClient({
          url: this.redisUrl,
          socket: {
            connectTimeout: 5000
          }
        });
        
        await testClient.connect();
        await testClient.ping();
        await testClient.disconnect();
        
        console.log(`✅ Redis is available (attempt ${attempt}/${maxAttempts})`);
        return;
        
      } catch (error) {
        console.log(`⏳ Redis not ready yet (attempt ${attempt}/${maxAttempts}): ${error.message}`);
        
        if (attempt === maxAttempts) {
          console.error(`❌ Failed to connect to Redis after ${maxAttempts} attempts`);
          throw new Error(`Redis connection timeout after ${maxAttempts * delayMs / 1000} seconds`);
        }
        
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    }
  }

  async start() {
    this.running = true;
    console.log(`Redis worker ${this.workerId} starting...`);

    // Track active jobs for cancellation
    this.runningJobs = new Map(); // job_id -> { promise, cancel }

    // Start consuming jobs from the queue
    this.processJobs();

    // Handle graceful shutdown
    process.on('SIGINT', () => this.stop());
    process.on('SIGTERM', () => this.stop());
  }

  async processJobs() {
    while (this.running) {
      try {
        // Block for up to 5 seconds waiting for a job
        const result = await this.redis.brPop(this.jobQueue, 5);
        
        if (result) {
          const jobData = JSON.parse(result.element);
          await this.handleJobMessage(jobData);
        }
      } catch (error) {
        if (this.running) {
          console.error(`Error processing jobs: ${error.message}`);
          await new Promise(resolve => setTimeout(resolve, 1000));
        }
      }
    }
  }

  async handleJobMessage(redisJob) {
    // Extract the job data from the Redis job format
    const job = redisJob.data;
    
    // Check if job is already cancelled
    if (this.runningJobs.has(job.id) && this.runningJobs.get(job.id).cancelled) {
      console.log(`WORKER: Skipping job ${job.id} - already cancelled`);
      return;
    }
    
    if (this.activeJobs >= (parseInt(process.env.WORKER_CONCURRENCY) || 3)) {
      console.log(`Worker ${this.workerId} at capacity, requeuing job ${job.id}`);
      // Put the job back in the queue
      await this.redis.lPush(this.jobQueue, JSON.stringify(redisJob));
      return;
    }

    this.activeJobs++;
    console.log(`Worker ${this.workerId} processing job ${job.id}`);
    
    // Create cancellation mechanism
    let cancelled = false;
    let cancelResolve = null;
    const cancelPromise = new Promise(resolve => {
      cancelResolve = resolve;
    });
    
    // Track the job
    this.runningJobs.set(job.id, {
      cancelled: false,
      cancel: () => {
        console.log(`WORKER: Cancelling job ${job.id}`);
        cancelled = true;
        this.runningJobs.get(job.id).cancelled = true;
        cancelResolve();
      }
    });
    
    // Process job in background with cancellation support
    const jobPromise = this.processJobWithCancellation(job, cancelPromise)
      .finally(() => {
        this.activeJobs--;
        this.runningJobs.delete(job.id);
      });
    
    // Store the promise reference
    this.runningJobs.get(job.id).promise = jobPromise;
  }

  async processJobWithCancellation(job, cancelPromise) {
    console.log(`WORKER: Starting job ${job.id} with cancellation support`);
    
    try {
      // Publish running status
      await this.publishJobUpdate(job, 'running');
      
      // Start the actual job processing
      const jobPromise = this.processJob(job);
      
      // Race between job completion and cancellation
      const result = await Promise.race([
        jobPromise,
        cancelPromise.then(() => Promise.reject(new Error('Job cancelled')))
      ]);
      
      // Job completed successfully
      console.log(`WORKER: Job ${job.id} completed successfully`);
      await this.publishJobUpdate(job, 'completed', result);
      return result;
      
    } catch (error) {
      if (error.message === 'Job cancelled') {
        console.log(`WORKER: Job ${job.id} was cancelled`);
        await this.publishJobUpdate(job, 'cancelled', null, 'Job cancelled by user');
        throw error;
      } else {
        console.error(`WORKER: Job ${job.id} failed:`, error);
        await this.publishJobUpdate(job, 'failed', null, error.message);
        throw error;
      }
    }
  }

  async stop() {
    console.log(`Redis worker ${this.workerId} stopping...`);
    this.running = false;

    // Wait for active jobs to complete (with timeout)
    const timeout = 30000; // 30 seconds
    const start = Date.now();
    
    while (this.activeJobs > 0 && (Date.now() - start) < timeout) {
      console.log(`Waiting for ${this.activeJobs} active jobs to complete...`);
      await new Promise(resolve => setTimeout(resolve, 1000));
    }

    if (this.browser) {
      await this.browser.close();
    }

    // Disconnect from Redis
    try {
      await this.redis.disconnect();
    } catch (error) {
      console.error(`Error disconnecting from Redis: ${error.message}`);
    }

    console.log(`Redis worker ${this.workerId} stopped`);
    process.exit(0);
  }

  async publishJobUpdate(job, status, result = null, error = null) {
    try {
      console.log(`WORKER: Preparing to publish job update for ${job.id} to status: ${status}`);
      
      const update = {
        job_id: job.id,
        status: status,
        timestamp: new Date().toISOString(),
        worker_id: this.workerId,
      };

      // Handle results by storing in Redis
      if (result) {
        console.log(`WORKER: Job ${job.id} has result data of type ${typeof result}`);
        
        try {
          // Store full result in Redis with TTL
          const resultKey = `jobrunner:results:${job.id}`;
          const resultTTL = parseInt(process.env.RESULT_TTL_DAYS || '7') * 24 * 60 * 60; // 7 days default
          
          console.log(`WORKER: Storing result in Redis with key: ${resultKey}`);
          await this.redis.setEx(resultKey, resultTTL, JSON.stringify(result));
          console.log(`WORKER: Successfully stored result in Redis for job ${job.id}`);
          
          // Include result summary in the status update for immediate viewing
          update.result_summary = this.createResultSummary(result);
          update.result_stored_in_redis = true;
          
        } catch (redisError) {
          console.error(`WORKER ERROR: Failed to store result in Redis for job ${job.id}:`, redisError);
          
          // Fallback to artifact storage if Redis fails
          if (this.artifactStorage.shouldStoreInSupabase(result)) {
            try {
              console.log(`WORKER: Redis failed, falling back to Supabase for job ${job.id}`);
              const storageInfo = await this.artifactStorage.storeArtifact(
                job.id, 
                result, 
                'result',
                { job_status: status }
              );
              
              update.artifact_storage_url = storageInfo.storage_url;
              update.artifact_metadata = storageInfo.artifact_metadata;
              update.result = this.artifactStorage.createResultSummary(result);
              
              console.log(`WORKER: Stored large result in Supabase as fallback: ${storageInfo.storage_url}`);
            } catch (storageError) {
              console.error(`WORKER ERROR: Both Redis and Supabase storage failed for job ${job.id}:`, storageError);
              update.result = this.createResultSummary(result, 500);
              update.error = update.error ? `${update.error}; Storage failed: ${redisError.message}` : 
                            `Storage failed: ${redisError.message}`;
            }
          } else {
            // Small result, include directly as fallback
            update.result = result;
            update.error = update.error ? `${update.error}; Redis storage failed: ${redisError.message}` : 
                          `Redis storage failed: ${redisError.message}`;
          }
        }
      }

      if (error) {
        update.error = error;
        console.log(`WORKER: Job ${job.id} has error: ${error}`);
      }

      if (status === 'running' && !job.started_at) {
        update.started_at = new Date().toISOString();
        console.log(`WORKER: Setting started_at for job ${job.id}: ${update.started_at}`);
      }
      
      if (status === 'completed' || status === 'failed') {
        update.completed_at = new Date().toISOString();
        console.log(`WORKER: Setting completed_at for job ${job.id}: ${update.completed_at}`);
      }

      // Publish update to Redis stream
      await this.redis.xAdd(this.statusStream, '*', update);

      console.log(`WORKER SUCCESS: Published job update for ${job.id}: ${status} to stream ${this.statusStream}`);
    } catch (error) {
      console.error(`WORKER ERROR: Failed to publish job update for ${job.id}:`, error);
      console.error(`WORKER ERROR DETAILS: stream=${this.statusStream}, status=${status}, worker=${this.workerId}`);
    }
  }

  async publishJobLog(jobId, level, message, metadata = {}) {
    try {
      const logEntry = {
        id: `log-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
        job_id: jobId,
        timestamp: new Date().toISOString(),
        level: level,
        message: message,
        worker_id: this.workerId,
        metadata: JSON.stringify(metadata),
      };

      await this.redis.xAdd(this.logStream, '*', logEntry);

    } catch (error) {
      console.error(`Failed to publish job log:`, error);
    }
  }

  async processJob(job) {
    // Initialize file-based logging for this job
    const logger = new FileLogCollector(job.id, this.workerId);
    await logger.init();

    try {
      logger.info(`Starting job execution`, { mode: job.mode, gov_ids: job.gov_ids });
      await this.publishJobUpdate(job, 'running');
      
      let result;
      
      // Check if this is a dokito backend job
      if (job.mode && job.mode.startsWith('dokito-')) {
        // Handle dokito backend API jobs
        const { DokitoClient } = require('./dokito-client');
        const dokitoClient = new DokitoClient(
          process.env.DOKITO_BACKEND_URL || 'http://localhost:8123',
          {
            info: (msg, meta) => logger.info(msg, meta),
            error: (msg, meta) => logger.error(msg, meta),
            warn: (msg, meta) => logger.warn(msg, meta),
            debug: (msg, meta) => logger.debug(msg, meta),
          }
        );
        
        switch (job.mode) {
          case 'dokito-case-fetch':
            logger.info(`Fetching case data`, { 
              state: job.state, 
              jurisdiction_name: job.jurisdiction_name, 
              case_name: job.case_name 
            });
            result = await dokitoClient.fetchCase(job.state, job.jurisdiction_name, job.case_name);
            break;
            
          case 'dokito-caselist':
            logger.info(`Fetching case list`, { 
              state: job.state, 
              jurisdiction_name: job.jurisdiction_name,
              limit: job.limit || 10,
              offset: job.offset || 0
            });
            result = await dokitoClient.fetchCaseList(job.state, job.jurisdiction_name, job.limit, job.offset);
            break;
            
          case 'dokito-attachment-obj':
            logger.info(`Fetching attachment metadata`, { blake2b_hash: job.blake2b_hash });
            result = await dokitoClient.fetchAttachmentObj(job.blake2b_hash);
            break;
            
          case 'dokito-attachment-raw':
            logger.info(`Fetching raw attachment`, { blake2b_hash: job.blake2b_hash });
            result = await dokitoClient.fetchAttachmentRaw(job.blake2b_hash);
            // Convert buffer to base64 for JSON serialization
            if (Buffer.isBuffer(result)) {
              result = {
                jobtype: 'binary',
                data: result.toString('base64'),
                size: result.length
              };
            }
            break;
            
          case 'dokito-case-submit':
            logger.info(`Submitting case data`, { case_data_keys: Object.keys(job.case_data || {}) });
            result = await dokitoClient.submitCase(job.case_data);
            break;
            
          case 'dokito-reprocess':
            logger.info(`Triggering reprocess operation`, { 
              operation_type: job.operation_type,
              state: job.state,
              jurisdiction_name: job.jurisdiction_name
            });
            result = await dokitoClient.reprocessOperation(job.operation_type, job.state, job.jurisdiction_name);
            break;
            
          default:
            throw new Error(`Unsupported dokito job mode: ${job.mode}`);
        }
        
        logger.info(`Dokito job completed successfully`, { mode: job.mode });
      } else {
        // Handle traditional playwright scraping jobs
        const { chromium } = require('playwright');
        
        // Initialize browser if not already done
        if (!this.browser) {
          this.browser = await chromium.launch({ 
            headless: process.env.PLAYWRIGHT_HEADLESS !== 'false',
            args: [
              '--no-sandbox',
              '--disable-setuid-sandbox',
              '--disable-dev-shm-usage',
              '--disable-web-security',
              '--disable-features=VizDisplayCompositor'
            ]
          });
        }

        // Create new browser context for this job
        const context = await this.browser.newContext();
        const page = await context.newPage();
        
        try {
          // Create scraper instance with logger
          const { NyPucScraper } = require('./scraper-wrapper');
          const scraper = new NyPucScraper(page, context, this.browser, {
            info: (msg, meta) => logger.info(msg, meta),
            error: (msg, meta) => logger.error(msg, meta),
            warn: (msg, meta) => logger.warn(msg, meta),
            debug: (msg, meta) => logger.debug(msg, meta),
          });
          
          switch (job.mode) {
            case 'full':
              logger.info(`Starting full scraping for ${job.gov_ids.length} government IDs`);
              result = await scraper.scrapeByGovIds(job.gov_ids, 'full');
              break;
              
            case 'meta':
              logger.info(`Starting metadata scraping for ${job.gov_ids.length} government IDs`);
              result = await scraper.scrapeByGovIds(job.gov_ids, 'meta');
              break;
              
            case 'docs':
              logger.info(`Starting document scraping for ${job.gov_ids.length} government IDs`);
              result = await scraper.scrapeByGovIds(job.gov_ids, 'docs');
              break;
              
            case 'parties':
              logger.info(`Starting parties scraping for ${job.gov_ids.length} government IDs`);
              result = await scraper.scrapeByGovIds(job.gov_ids, 'parties');
              break;
              
            case 'full-extraction':
              logger.info(`Starting full extraction for ${job.gov_ids.length} government IDs`);
              result = await scraper.scrapeByGovIds(job.gov_ids, 'full-extraction');
              break;
              
            case 'case-list':
              logger.info(`Starting case list scraping`);
              result = await scraper.getCaseList();
              break;
              
            case 'dates':
              logger.info(`Starting date-based scraping`, { date_string: job.date_string });
              result = await scraper.getDateCases(job.date_string);
              break;
              
            case 'filings-between-dates':
              const beginDate = new Date(job.begin_date);
              const endDate = new Date(job.end_date);
              logger.info(`Starting date range scraping`, { begin_date: job.begin_date, end_date: job.end_date });
              result = await scraper.getFilingsBetweenDates(beginDate, endDate);
              break;
              
            default:
              throw new Error(`Unsupported scraping job mode: ${job.mode}`);
          }

          const resultCount = Array.isArray(result) ? result.length : 1;
          logger.info(`Scraping job completed successfully`, { result_count: resultCount });
          
        } finally {
          await context.close();
        }
      }
      
      logger.info(`Job completed successfully`, { 
        result_type: typeof result, 
        result_size: Array.isArray(result) ? result.length : 'N/A' 
      });
      
      await logger.logJobCompletion('completed', result);
      await this.publishJobUpdate(job, 'completed', result);
      
    } catch (error) {
      const errorMessage = error.message;
      logger.error(`Job failed`, { error: errorMessage, stack: error.stack });
      await logger.logJobCompletion('failed', null, errorMessage);
      await this.publishJobUpdate(job, 'failed', null, errorMessage);
    } finally {
      // Always finalize logger
      if (logger) {
        await logger.finalize();
      }
    }
  }

  // Helper method to create result summary for immediate viewing
  createResultSummary(result, maxLength = 200) {
    if (!result) return null;
    
    if (Array.isArray(result)) {
      return {
        jobtype: 'array',
        length: result.length,
        sample: result.slice(0, 3), // First 3 items
        preview: `Array with ${result.length} items`
      };
    }
    
    if (typeof result === 'object') {
      const keys = Object.keys(result);
      return {
        jobtype: 'object',
        keys: keys.slice(0, 10), // First 10 keys
        keyCount: keys.length,
        preview: `Object with ${keys.length} keys: ${keys.slice(0, 5).join(', ')}${keys.length > 5 ? '...' : ''}`
      };
    }
    
    if (typeof result === 'string') {
      return {
        jobtype: 'string',
        length: result.length,
        preview: result.length > maxLength ? result.substring(0, maxLength) + '...' : result
      };
    }
    
    return {
      jobtype: typeof result,
      preview: String(result).substring(0, maxLength)
    };
  }
}

module.exports = { RedisJobWorker };