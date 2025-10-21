const fs = require('fs').promises;
const path = require('path');
const { chromium } = require('playwright');
const { NyPucScraper } = require('./scraper-wrapper');

// Container entry point for running scrapers
async function main() {
  console.log('🚀 Starting scraper container');

  // Parse environment variables
  const govIds = (process.env.GOVIDS || '').split(',').filter(id => id.trim());
  const mode = process.env.MODE || 'full';
  const outputDir = process.env.OUTPUT_DIR || '/app/output';
  const jobId = process.env.JOB_ID || 'unknown';
  const workerId = process.env.WORKER_ID || 'unknown';
  const dateString = process.env.DATE_STRING;
  const beginDate = process.env.BEGIN_DATE;
  const endDate = process.env.END_DATE;

  console.log(`📋 Job Configuration:`);
  console.log(`  Job ID: ${jobId}`);
  console.log(`  Worker ID: ${workerId}`);
  console.log(`  Mode: ${mode}`);
  console.log(`  Gov IDs: ${govIds.join(', ')}`);
  console.log(`  Output Directory: ${outputDir}`);
  if (dateString) console.log(`  Date String: ${dateString}`);
  if (beginDate) console.log(`  Begin Date: ${beginDate}`);
  if (endDate) console.log(`  End Date: ${endDate}`);

  // Validate required parameters
  if (govIds.length === 0 && !['case-list', 'dates', 'filings-between-dates'].includes(mode)) {
    console.error('❌ Error: GOVIDS environment variable is required for this mode');
    process.exit(1);
  }

  if (mode === 'dates' && !dateString) {
    console.error('❌ Error: DATE_STRING environment variable is required for dates mode');
    process.exit(1);
  }

  if (mode === 'filings-between-dates' && (!beginDate || !endDate)) {
    console.error('❌ Error: BEGIN_DATE and END_DATE environment variables are required for filings-between-dates mode');
    process.exit(1);
  }

  let browser = null;
  let context = null;
  let page = null;

  try {
    // Ensure output directory exists
    await fs.mkdir(outputDir, { recursive: true });

    // Initialize browser
    console.log('🌐 Launching browser...');
    browser = await chromium.launch({
      headless: process.env.PLAYWRIGHT_HEADLESS !== 'false',
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-web-security',
        '--disable-features=VizDisplayCompositor'
      ]
    });

    context = await browser.newContext();
    page = await context.newPage();

    // Create scraper instance with container logger
    const logger = {
      info: (msg, meta = {}) => console.log(`ℹ️ [INFO] ${msg}`, meta),
      debug: (msg, meta = {}) => console.log(`🔍 [DEBUG] ${msg}`, meta),
      warn: (msg, meta = {}) => console.warn(`⚠️ [WARN] ${msg}`, meta),
      error: (msg, meta = {}) => console.error(`❌ [ERROR] ${msg}`, meta)
    };

    const scraper = new NyPucScraper(page, context, browser, logger);

    console.log(`🔧 Running scraper in mode: ${mode}`);
    let result;

    switch (mode) {
      case 'all':
        result = await scraper.scrapeByGovIds(govIds, 'all');
        break;

      case 'meta':
        result = await scraper.scrapeByGovIds(govIds, 'meta');
        break;

      case 'docs':
        result = await scraper.scrapeByGovIds(govIds, 'docs');
        break;

      case 'parties':
        result = await scraper.scrapeByGovIds(govIds, 'parties');
        break;

      case 'case-list':
        result = await scraper.getCaseList();
        break;

      case 'dates':
        result = await scraper.getDateCases(dateString);
        break;

      case 'filings-between-dates':
        const beginDateObj = new Date(beginDate);
        const endDateObj = new Date(endDate);
        result = await scraper.getFilingsBetweenDates(beginDateObj, endDateObj);
        break;

      default:
        throw new Error(`Unsupported scraping mode: ${mode}`);
    }

    console.log(`✅ Scraping completed successfully`);
    console.log(`📊 Result summary: ${Array.isArray(result) ? result.length + ' items' : typeof result}`);

    // Write results to output files
    await writeResults(result, govIds, outputDir, mode);

    console.log('💾 Results written to output directory');
    console.log('🎉 Container execution completed successfully');

  } catch (error) {
    console.error('💥 Container execution failed:', error.message);
    console.error('Stack trace:', error.stack);

    // Write error information to output directory
    try {
      const errorInfo = {
        error: error.message,
        stack: error.stack,
        timestamp: new Date().toISOString(),
        job_id: jobId,
        worker_id: workerId,
        mode: mode,
        gov_ids: govIds
      };

      const errorPath = path.join(outputDir, 'error.json');
      await fs.writeFile(errorPath, JSON.stringify(errorInfo, null, 2));
      console.log(`📝 Error details written to ${errorPath}`);
    } catch (writeError) {
      console.error('Failed to write error file:', writeError.message);
    }

    process.exit(1);
  } finally {
    // Clean up browser resources
    if (context) {
      try {
        await context.close();
      } catch (e) {
        console.warn('Error closing context:', e.message);
      }
    }
    if (browser) {
      try {
        await browser.close();
      } catch (e) {
        console.warn('Error closing browser:', e.message);
      }
    }
  }
}

// Write results to output files based on the structure expected by the Go runner
async function writeResults(result, govIds, outputDir, mode) {
  if (!result) {
    console.warn('⚠️ No results to write');
    return;
  }

  // Handle different result structures
  if (Array.isArray(result)) {
    if (mode === 'case-list' || mode === 'dates' || mode === 'filings-between-dates') {
      // For list modes, write a single result file
      const resultPath = path.join(outputDir, 'result.json');
      await fs.writeFile(resultPath, JSON.stringify(result, null, 2));
      console.log(`📁 Wrote ${result.length} items to result.json`);
    } else {
      // For gov ID based modes, organize by gov ID if possible
      const govIdMap = new Map();

      // Try to map results to gov IDs
      result.forEach((item, index) => {
        let govId = null;

        // Try to extract gov ID from various possible fields
        if (item && typeof item === 'object') {
          govId = item.case_govid || item.gov_id || item.govid ||
            item.case_gov_id || item.government_id;
        }

        // If no gov ID found, use index-based mapping
        if (!govId && govIds && govIds[index]) {
          govId = govIds[index];
        }

        // If still no gov ID, use a generic key
        if (!govId) {
          govId = `result_${index}`;
        }

        if (!govIdMap.has(govId)) {
          govIdMap.set(govId, []);
        }
        govIdMap.get(govId).push(item);
      });

      // Write individual files for each gov ID
      for (const [govId, items] of govIdMap) {
        const fileName = `${govId}.json`;
        const filePath = path.join(outputDir, fileName);
        const data = items.length === 1 ? items[0] : items;
        await fs.writeFile(filePath, JSON.stringify(data, null, 2));
        console.log(`📁 Wrote ${items.length} items to ${fileName}`);
      }
    }
  } else {
    // Single result object
    const resultPath = path.join(outputDir, 'result.json');
    await fs.writeFile(resultPath, JSON.stringify(result, null, 2));
    console.log(`📁 Wrote single result to result.json`);
  }

  // Also write a summary file with metadata
  const summary = {
    timestamp: new Date().toISOString(),
    mode: mode,
    gov_ids: govIds,
    result_type: Array.isArray(result) ? 'array' : typeof result,
    result_count: Array.isArray(result) ? result.length : 1,
    job_id: process.env.JOB_ID,
    worker_id: process.env.WORKER_ID
  };

  const summaryPath = path.join(outputDir, 'summary.json');
  await fs.writeFile(summaryPath, JSON.stringify(summary, null, 2));
  console.log(`📋 Wrote execution summary to summary.json`);
}

// Handle graceful shutdown
process.on('SIGTERM', () => {
  console.log('📨 Received SIGTERM, shutting down gracefully...');
  process.exit(0);
});

process.on('SIGINT', () => {
  console.log('📨 Received SIGINT, shutting down gracefully...');
  process.exit(0);
});

// Start the container
if (require.main === module) {
  main().catch(error => {
    console.error('💥 Unhandled error in main:', error);
    process.exit(1);
  });
}
