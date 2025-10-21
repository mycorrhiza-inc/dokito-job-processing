const fs = require('fs').promises;
const path = require('path');

/**
 * FileLogCollector handles structured logging directly to files
 * This replaces the HTTP-based LogCollector for better performance and reliability
 */
class FileLogCollector {
  constructor(jobId, workerId, logsDir = '/shared/logs') {
    this.jobId = jobId;
    this.workerId = workerId;
    this.logsDir = logsDir;
    this.logFilePath = path.join(logsDir, `${jobId}.log`);
    this.writeBuffer = [];
    this.bufferSize = 50; // Buffer more entries since file writes are faster
    this.flushInterval = 2000; // Flush every 2 seconds
    this.flushTimer = null;
    this.fileHandle = null;
    
    this.startPeriodicFlush();
  }

  /**
   * Initialize the log file and ensure directory exists
   */
  async init() {
    try {
      await fs.mkdir(this.logsDir, { recursive: true });
      
      // Create initial log entry
      const initEntry = {
        timestamp: new Date().toISOString(),
        level: 'info',
        message: `Job logging started`,
        worker_id: this.workerId,
        job_id: this.jobId,
        metadata: {
          action: 'job_start'
        }
      };
      
      await this.writeLogEntry(initEntry);
      console.log(`Log file initialized: ${this.logFilePath}`);
    } catch (error) {
      console.error(`Failed to initialize log file ${this.logFilePath}:`, error.message);
      throw error;
    }
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
      timestamp: new Date().toISOString(),
      level,
      message,
      worker_id: this.workerId,
      job_id: this.jobId,
      metadata: {
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

    this.writeBuffer.push(logEntry);

    // Flush if buffer is full
    if (this.writeBuffer.length >= this.bufferSize) {
      this.flush();
    }
  }

  /**
   * Start periodic flushing of logs
   */
  startPeriodicFlush() {
    this.flushTimer = setInterval(() => {
      if (this.writeBuffer.length > 0) {
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
   * Flush all buffered logs to file
   */
  async flush() {
    if (this.writeBuffer.length === 0) return;

    const logsToWrite = [...this.writeBuffer];
    this.writeBuffer = [];

    try {
      for (const logEntry of logsToWrite) {
        await this.writeLogEntry(logEntry);
      }
    } catch (error) {
      console.error(`Failed to write logs to file:`, error.message);
      // Re-add failed logs to buffer for retry (up to buffer size)
      this.writeBuffer.unshift(...logsToWrite.slice(0, this.bufferSize - this.writeBuffer.length));
    }
  }

  /**
   * Write a single log entry to file (JSON Lines format)
   */
  async writeLogEntry(logEntry) {
    try {
      const logLine = JSON.stringify(logEntry) + '\n';
      await fs.appendFile(this.logFilePath, logLine, 'utf8');
    } catch (error) {
      console.error(`Failed to write log entry to ${this.logFilePath}:`, error.message);
      throw error;
    }
  }

  /**
   * Log job completion and cleanup
   */
  async logJobCompletion(status, result = null, error = null) {
    const completionEntry = {
      timestamp: new Date().toISOString(),
      level: status === 'completed' ? 'info' : 'error',
      message: `Job ${status}`,
      worker_id: this.workerId,
      job_id: this.jobId,
      metadata: {
        action: 'job_end',
        final_status: status,
        result: result ? 'available' : 'none',
        error: error || null
      }
    };

    await this.writeLogEntry(completionEntry);
  }

  /**
   * Final flush and cleanup when job completes
   */
  async finalize() {
    this.stopPeriodicFlush();
    await this.flush();
    
    // Log finalization
    const finalEntry = {
      timestamp: new Date().toISOString(),
      level: 'info',
      message: 'Log collection finalized',
      worker_id: this.workerId,
      job_id: this.jobId,
      metadata: {
        action: 'log_finalize'
      }
    };
    
    await this.writeLogEntry(finalEntry);
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

  /**
   * Get the current log file path
   */
  getLogFilePath() {
    return this.logFilePath;
  }

  /**
   * Check if log file exists
   */
  async logFileExists() {
    try {
      await fs.access(this.logFilePath);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Get log file size in bytes
   */
  async getLogFileSize() {
    try {
      const stats = await fs.stat(this.logFilePath);
      return stats.size;
    } catch {
      return 0;
    }
  }
}

module.exports = { FileLogCollector };