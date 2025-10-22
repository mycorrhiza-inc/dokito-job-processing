const axios = require('axios');

/**
 * LogCollector handles structured logging and sends logs to the API
 */
class LogCollector {
  constructor(jobId, workerId, apiBaseUrl = 'http://api:8080/api/v1') {
    this.jobId = jobId;
    this.workerId = workerId;
    this.apiBaseUrl = apiBaseUrl;
    this.logBuffer = [];
    this.bufferSize = 10; // Send logs in batches of 10
    this.flushInterval = 5000; // Flush every 5 seconds
    this.flushTimer = null;
    
    // Start periodic flush
    this.startPeriodicFlush();
  }

  /**
   * Log a debug message
   */
  debug(message, metadata = {}) {
    this.addLog('debug', message, metadata);
  }

  /**
   * Log an info message
   */
  info(message, metadata = {}) {
    this.addLog('info', message, metadata);
  }

  /**
   * Log a warning message
   */
  warn(message, metadata = {}) {
    this.addLog('warn', message, metadata);
  }

  /**
   * Log an error message
   */
  error(message, metadata = {}) {
    this.addLog('error', message, metadata);
  }

  /**
   * Add a log entry to the buffer
   */
  addLog(level, message, metadata = {}) {
    const logEntry = {
      level,
      message,
      worker_id: this.workerId,
      metadata: {
        timestamp: new Date().toISOString(),
        ...metadata
      }
    };

    // Also log to console for local debugging
    const formattedMessage = `[${level.toUpperCase()}] [${this.workerId}] [Job: ${this.jobId}] ${message}`;
    switch (level) {
      case 'error':
        console.error(formattedMessage, metadata);
        break;
      case 'warn':
        console.warn(formattedMessage, metadata);
        break;
      case 'debug':
        console.debug(formattedMessage, metadata);
        break;
      default:
        console.log(formattedMessage, metadata);
    }

    this.logBuffer.push(logEntry);

    // Flush if buffer is full
    if (this.logBuffer.length >= this.bufferSize) {
      this.flush();
    }
  }

  /**
   * Start periodic flushing of logs
   */
  startPeriodicFlush() {
    this.flushTimer = setInterval(() => {
      if (this.logBuffer.length > 0) {
        this.flush();
      }
    }, this.flushInterval);
  }

  /**
   * Stop periodic flushing
   */
  stopPeriodicFlush() {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
  }

  /**
   * Flush all buffered logs to the API
   */
  async flush() {
    if (this.logBuffer.length === 0) return;

    const logsToSend = [...this.logBuffer];
    this.logBuffer = [];

    try {
      // Send logs in batch
      for (const logEntry of logsToSend) {
        await this.sendLogToAPI(logEntry);
      }
    } catch (error) {
      console.error(`Failed to send logs to API:`, error.message);
      // Re-add failed logs to buffer for retry (up to buffer size)
      this.logBuffer.unshift(...logsToSend.slice(0, this.bufferSize - this.logBuffer.length));
    }
  }

  /**
   * Send a single log entry to the API
   */
  async sendLogToAPI(logEntry) {
    try {
      await axios.post(
        `${this.apiBaseUrl}/jobs/${this.jobId}/logs`,
        logEntry,
        {
          timeout: 5000,
          headers: {
            'Content-Type': 'application/json'
          }
        }
      );
    } catch (error) {
      // Don't throw, just log the error to avoid infinite loops
      if (error.response) {
        console.error(`API log error (${error.response.status}):`, error.response.data);
      } else {
        console.error(`Network error sending log:`, error.message);
      }
      throw error; // Re-throw for retry logic in flush()
    }
  }

  /**
   * Final flush and cleanup when job completes
   */
  async finalize() {
    this.stopPeriodicFlush();
    await this.flush();
  }

  /**
   * Create a scoped logger with additional metadata
   */
  scope(additionalMetadata = {}) {
    return {
      debug: (message, metadata = {}) => this.debug(message, { ...additionalMetadata, ...metadata }),
      info: (message, metadata = {}) => this.info(message, { ...additionalMetadata, ...metadata }),
      warn: (message, metadata = {}) => this.warn(message, { ...additionalMetadata, ...metadata }),
      error: (message, metadata = {}) => this.error(message, { ...additionalMetadata, ...metadata })
    };
  }
}

module.exports = { LogCollector };