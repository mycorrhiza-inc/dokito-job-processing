const fs = require('fs').promises;
const path = require('path');

/**
 * Log management utilities for file-based logging
 */
class LogUtils {
  constructor(logsDir = '/shared/logs') {
    this.logsDir = logsDir;
  }

  /**
   * Clean up old log files based on age
   * @param {number} maxAgeHours - Maximum age in hours
   * @returns {Promise<{deleted: number, errors: string[]}>}
   */
  async cleanupOldLogs(maxAgeHours = 24 * 7) { // Default 7 days
    const results = { deleted: 0, errors: [] };
    const cutoffTime = Date.now() - (maxAgeHours * 60 * 60 * 1000);

    try {
      const files = await fs.readdir(this.logsDir);
      const logFiles = files.filter(file => file.endsWith('.log'));

      for (const file of logFiles) {
        const filePath = path.join(this.logsDir, file);
        
        try {
          const stats = await fs.stat(filePath);
          
          if (stats.mtime.getTime() < cutoffTime) {
            await fs.unlink(filePath);
            results.deleted++;
            console.log(`Deleted old log file: ${file}`);
          }
        } catch (error) {
          results.errors.push(`Failed to process ${file}: ${error.message}`);
        }
      }
    } catch (error) {
      results.errors.push(`Failed to read logs directory: ${error.message}`);
    }

    return results;
  }

  /**
   * Get log file information
   * @param {string} jobId 
   * @returns {Promise<{exists: boolean, size: number, lastModified: Date|null}>}
   */
  async getLogFileInfo(jobId) {
    const logFilePath = path.join(this.logsDir, `${jobId}.log`);
    
    try {
      const stats = await fs.stat(logFilePath);
      return {
        exists: true,
        size: stats.size,
        lastModified: stats.mtime,
        path: logFilePath
      };
    } catch (error) {
      return {
        exists: false,
        size: 0,
        lastModified: null,
        path: logFilePath
      };
    }
  }

  /**
   * Read log file lines with pagination
   * @param {string} jobId 
   * @param {number} limit 
   * @param {number} offset 
   * @returns {Promise<{logs: object[], total: number, hasMore: boolean}>}
   */
  async readLogFile(jobId, limit = 100, offset = 0) {
    const logFilePath = path.join(this.logsDir, `${jobId}.log`);
    
    try {
      const fileContent = await fs.readFile(logFilePath, 'utf8');
      const lines = fileContent.trim().split('\n').filter(line => line.trim());
      
      // Parse JSON lines
      const logs = [];
      for (const line of lines) {
        try {
          const logEntry = JSON.parse(line);
          logs.push(logEntry);
        } catch (parseError) {
          // Skip invalid JSON lines
          console.warn(`Skipping invalid log line in ${jobId}.log:`, line);
        }
      }

      // Sort by timestamp (newest first)
      logs.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));

      // Apply pagination
      const paginatedLogs = logs.slice(offset, offset + limit);
      
      return {
        logs: paginatedLogs,
        total: logs.length,
        hasMore: offset + limit < logs.length
      };
    } catch (error) {
      if (error.code === 'ENOENT') {
        return { logs: [], total: 0, hasMore: false };
      }
      throw error;
    }
  }

  /**
   * Stream log file for real-time viewing
   * @param {string} jobId 
   * @param {function} callback - Called for each new log line
   * @returns {Promise<function>} Cleanup function
   */
  async streamLogFile(jobId, callback) {
    const logFilePath = path.join(this.logsDir, `${jobId}.log`);
    let lastSize = 0;
    let isWatching = true;

    // Get initial file size
    try {
      const stats = await fs.stat(logFilePath);
      lastSize = stats.size;
    } catch {
      // File doesn't exist yet, start from 0
      lastSize = 0;
    }

    const watchInterval = setInterval(async () => {
      if (!isWatching) return;

      try {
        const stats = await fs.stat(logFilePath);
        
        if (stats.size > lastSize) {
          // Read new content
          const fileHandle = await fs.open(logFilePath, 'r');
          const buffer = Buffer.alloc(stats.size - lastSize);
          await fileHandle.read(buffer, 0, buffer.length, lastSize);
          await fileHandle.close();

          const newContent = buffer.toString('utf8');
          const newLines = newContent.split('\n').filter(line => line.trim());

          for (const line of newLines) {
            try {
              const logEntry = JSON.parse(line);
              callback(logEntry);
            } catch (parseError) {
              // Skip invalid JSON lines
            }
          }

          lastSize = stats.size;
        }
      } catch (error) {
        if (error.code !== 'ENOENT') {
          console.error(`Error streaming log file ${jobId}:`, error.message);
        }
      }
    }, 1000); // Check every second

    // Return cleanup function
    return () => {
      isWatching = false;
      clearInterval(watchInterval);
    };
  }

  /**
   * Get logs directory statistics
   * @returns {Promise<{totalFiles: number, totalSize: number, oldestFile: Date|null, newestFile: Date|null}>}
   */
  async getLogsDirectoryStats() {
    const stats = {
      totalFiles: 0,
      totalSize: 0,
      oldestFile: null,
      newestFile: null
    };

    try {
      const files = await fs.readdir(this.logsDir);
      const logFiles = files.filter(file => file.endsWith('.log'));

      for (const file of logFiles) {
        const filePath = path.join(this.logsDir, file);
        
        try {
          const fileStats = await fs.stat(filePath);
          stats.totalFiles++;
          stats.totalSize += fileStats.size;

          if (!stats.oldestFile || fileStats.mtime < stats.oldestFile) {
            stats.oldestFile = fileStats.mtime;
          }
          
          if (!stats.newestFile || fileStats.mtime > stats.newestFile) {
            stats.newestFile = fileStats.mtime;
          }
        } catch (error) {
          console.warn(`Failed to stat log file ${file}:`, error.message);
        }
      }
    } catch (error) {
      console.error(`Failed to read logs directory:`, error.message);
    }

    return stats;
  }

  /**
   * Search logs for a specific pattern
   * @param {string} jobId 
   * @param {string} searchTerm 
   * @param {string} level - Optional log level filter
   * @returns {Promise<object[]>}
   */
  async searchLogs(jobId, searchTerm, level = null) {
    const { logs } = await this.readLogFile(jobId, 10000, 0); // Read all logs
    
    return logs.filter(log => {
      const matchesSearch = log.message.toLowerCase().includes(searchTerm.toLowerCase());
      const matchesLevel = !level || log.level === level;
      return matchesSearch && matchesLevel;
    });
  }

  /**
   * Ensure logs directory exists
   */
  async ensureLogsDirectory() {
    try {
      await fs.mkdir(this.logsDir, { recursive: true });
      return true;
    } catch (error) {
      console.error(`Failed to create logs directory ${this.logsDir}:`, error.message);
      return false;
    }
  }
}

module.exports = { LogUtils };