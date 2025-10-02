package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// LogEntry represents a single log entry from the file
type LogFileEntry struct {
	Timestamp string                 `json:"timestamp"`
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	WorkerID  string                 `json:"worker_id"`
	JobID     string                 `json:"job_id"`
	Metadata  map[string]interface{} `json:"metadata"`
}

// LogManager handles file-based log operations with pipeline support
type LogManager struct {
	LogsDirectory    string
	PipelineLogsDir  string
	StageLogsDir     string
}

// NewLogManager creates a new log manager instance
func NewLogManager(logsDir string) *LogManager {
	pipelineLogsDir := filepath.Join(logsDir, "pipeline")
	stageLogsDir := filepath.Join(logsDir, "stages")
	
	// Create directories if they don't exist
	os.MkdirAll(pipelineLogsDir, 0755)
	os.MkdirAll(stageLogsDir, 0755)
	
	return &LogManager{
		LogsDirectory:   logsDir,
		PipelineLogsDir: pipelineLogsDir,
		StageLogsDir:    stageLogsDir,
	}
}

// GetJobLogFile returns the path to a job's log file
func (lm *LogManager) GetJobLogFile(jobID string) string {
	return filepath.Join(lm.LogsDirectory, fmt.Sprintf("%s.log", jobID))
}

// LogFileExists checks if a log file exists for the given job
func (lm *LogManager) LogFileExists(jobID string) bool {
	logPath := lm.GetJobLogFile(jobID)
	_, err := os.Stat(logPath)
	return err == nil
}

// GetLogFileInfo returns information about a log file
func (lm *LogManager) GetLogFileInfo(jobID string) (map[string]interface{}, error) {
	logPath := lm.GetJobLogFile(jobID)
	
	stat, err := os.Stat(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]interface{}{
				"exists":        false,
				"size":          0,
				"last_modified": nil,
				"path":          logPath,
			}, nil
		}
		return nil, fmt.Errorf("failed to stat log file: %w", err)
	}

	return map[string]interface{}{
		"exists":        true,
		"size":          stat.Size(),
		"last_modified": stat.ModTime(),
		"path":          logPath,
	}, nil
}

// ReadLogFile reads and parses a job's log file with pagination
func (lm *LogManager) ReadLogFile(jobID string, limit, offset int) (*LogsResponse, error) {
	logPath := lm.GetJobLogFile(jobID)
	
	file, err := os.Open(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &LogsResponse{
				Logs:  make([]LogEntry, 0),
				Total: 0,
			}, nil
		}
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}
	defer file.Close()

	var compatibleLogs []LogEntry
	scanner := bufio.NewScanner(file)
	
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var logEntry LogFileEntry
		if err := json.Unmarshal([]byte(line), &logEntry); err != nil {
			continue // Skip invalid lines
		}

		timestamp, _ := time.Parse(time.RFC3339, logEntry.Timestamp)
		
		compatibleLogs = append(compatibleLogs, LogEntry{
			ID:        "",
			Timestamp: timestamp,
			Level:     LogLevel(logEntry.Level),
			Message:   logEntry.Message,
			WorkerID:  logEntry.WorkerID,
			Metadata:  logEntry.Metadata,
		})
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading log file: %w", err)
	}


	// Apply pagination
	total := len(compatibleLogs)
	start := offset
	end := offset + limit

	if start > total {
		start = total
	}
	if end > total {
		end = total
	}

	return &LogsResponse{
		Logs:  compatibleLogs[start:end],
		Total: total,
	}, nil
}

// StreamLogFile creates a file watcher for real-time log streaming
// This is a simplified version - in production you'd want to use fsnotify
func (lm *LogManager) StreamLogFile(jobID string, lastOffset int64, callback func(LogFileEntry)) (int64, error) {
	logPath := lm.GetJobLogFile(jobID)
	
	file, err := os.Open(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return lastOffset, nil // No new content
		}
		return lastOffset, fmt.Errorf("failed to open log file: %w", err)
	}
	defer file.Close()

	// Seek to the last position
	_, err = file.Seek(lastOffset, 0)
	if err != nil {
		return lastOffset, fmt.Errorf("failed to seek log file: %w", err)
	}

	scanner := bufio.NewScanner(file)
	newOffset := lastOffset

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var logEntry LogFileEntry
		if err := json.Unmarshal([]byte(line), &logEntry); err != nil {
			// Skip invalid JSON lines
			continue
		}

		callback(logEntry)
		newOffset += int64(len(scanner.Bytes()) + 1) // +1 for newline
	}

	if err := scanner.Err(); err != nil {
		return newOffset, fmt.Errorf("error reading log file: %w", err)
	}

	return newOffset, nil
}

// CleanupOldLogs removes log files older than the specified duration
func (lm *LogManager) CleanupOldLogs(maxAge time.Duration) (int, error) {
	files, err := os.ReadDir(lm.LogsDirectory)
	if err != nil {
		return 0, fmt.Errorf("failed to read logs directory: %w", err)
	}

	cutoff := time.Now().Add(-maxAge)
	deletedCount := 0

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".log") {
			continue
		}

		filePath := filepath.Join(lm.LogsDirectory, file.Name())
		info, err := file.Info()
		if err != nil {
			fmt.Printf("Warning: Failed to get info for %s: %v\n", file.Name(), err)
			continue
		}

		if info.ModTime().Before(cutoff) {
			if err := os.Remove(filePath); err != nil {
				fmt.Printf("Warning: Failed to delete %s: %v\n", file.Name(), err)
				continue
			}
			deletedCount++
			fmt.Printf("Deleted old log file: %s\n", file.Name())
		}
	}

	return deletedCount, nil
}

// LogPipelineStage logs a detailed pipeline stage event
func (lm *LogManager) LogPipelineStage(stageLog *PipelineStageLog) error {
	// Create stage-specific log file
	stageLogFile := filepath.Join(lm.StageLogsDir, fmt.Sprintf("%s-%s.log", stageLog.ParentJobID, stageLog.Stage))
	
	// Calculate duration if end time is set
	if stageLog.EndTime != nil {
		duration := stageLog.EndTime.Sub(stageLog.StartTime)
		stageLog.Duration = duration.String()
	}
	
	// Marshal to JSON
	logData, err := json.Marshal(stageLog)
	if err != nil {
		return fmt.Errorf("failed to marshal pipeline stage log: %w", err)
	}
	
	// Append to stage log file
	file, err := os.OpenFile(stageLogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open stage log file: %w", err)
	}
	defer file.Close()
	
	_, err = file.WriteString(string(logData) + "\n")
	if err != nil {
		return fmt.Errorf("failed to write stage log: %w", err)
	}
	
	return nil
}

// LogPipelineEvent logs a general pipeline event with enhanced context
func (lm *LogManager) LogPipelineEvent(jobID, parentJobID, stage, govID, message string, level LogLevel, debugContext map[string]interface{}) error {
	logEntry := &LogEntry{
		ID:            fmt.Sprintf("%s-%d", jobID, time.Now().UnixNano()),
		Timestamp:     time.Now(),
		Level:         level,
		Message:       message,
		JobID:         jobID,
		ParentJobID:   parentJobID,
		PipelineStage: stage,
		GovID:         govID,
		DebugContext:  debugContext,
	}
	
	// Write to job-specific log file
	jobLogFile := lm.GetJobLogFile(jobID)
	logData, err := json.Marshal(logEntry)
	if err != nil {
		return fmt.Errorf("failed to marshal pipeline log entry: %w", err)
	}
	
	file, err := os.OpenFile(jobLogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open job log file: %w", err)
	}
	defer file.Close()
	
	_, err = file.WriteString(string(logData) + "\n")
	if err != nil {
		return fmt.Errorf("failed to write pipeline log: %w", err)
	}
	
	// Also write to pipeline-specific log for aggregated view
	if parentJobID != "" {
		pipelineLogFile := filepath.Join(lm.PipelineLogsDir, fmt.Sprintf("%s.log", parentJobID))
		pipelineFile, err := os.OpenFile(pipelineLogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err == nil {
			pipelineFile.WriteString(string(logData) + "\n")
			pipelineFile.Close()
		}
	}
	
	return nil
}

// GetPipelineStageLog retrieves the stage log for a specific pipeline and stage
func (lm *LogManager) GetPipelineStageLog(parentJobID, stage string) ([]*PipelineStageLog, error) {
	stageLogFile := filepath.Join(lm.StageLogsDir, fmt.Sprintf("%s-%s.log", parentJobID, stage))
	
	file, err := os.Open(stageLogFile)
	if err != nil {
		if os.IsNotExist(err) {
			return []*PipelineStageLog{}, nil
		}
		return nil, fmt.Errorf("failed to open stage log file: %w", err)
	}
	defer file.Close()
	
	var stageLogs []*PipelineStageLog
	scanner := bufio.NewScanner(file)
	
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		
		var stageLog PipelineStageLog
		if err := json.Unmarshal([]byte(line), &stageLog); err != nil {
			continue // Skip malformed lines
		}
		
		stageLogs = append(stageLogs, &stageLog)
	}
	
	return stageLogs, scanner.Err()
}

// GetPipelineOverview gets an overview of all stages for a pipeline job
func (lm *LogManager) GetPipelineOverview(parentJobID string) (map[string][]*PipelineStageLog, error) {
	overview := make(map[string][]*PipelineStageLog)
	
	// Read pipeline logs directory for this job
	pattern := filepath.Join(lm.StageLogsDir, fmt.Sprintf("%s-*.log", parentJobID))
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to glob stage log files: %w", err)
	}
	
	for _, file := range files {
		// Extract stage name from filename
		filename := filepath.Base(file)
		parts := strings.Split(filename, "-")
		if len(parts) < 2 {
			continue
		}
		stage := strings.TrimSuffix(strings.Join(parts[1:], "-"), ".log")
		
		stageLogs, err := lm.GetPipelineStageLog(parentJobID, stage)
		if err != nil {
			continue // Skip files with errors
		}
		
		overview[stage] = stageLogs
	}
	
	return overview, nil
}

// GetPipelineProgress calculates progress metrics for a pipeline job
func (lm *LogManager) GetPipelineProgress(parentJobID string) (*PipelineProgress, error) {
	overview, err := lm.GetPipelineOverview(parentJobID)
	if err != nil {
		return nil, err
	}
	
	progress := &PipelineProgress{
		StageProgress:   make(map[string]StageProgress),
		SubTasks:        make(map[string]SubTaskStatus),
		FailedGovIDs:    make([]string, 0),
		CompletedGovIDs: make([]string, 0),
		LastUpdated:     time.Now(),
	}
	
	allGovIDs := make(map[string]bool)
	
	// Process each stage
	for stage, stageLogs := range overview {
		stageProgress := StageProgress{
			Stage: stage,
		}
		
		for _, stageLog := range stageLogs {
			allGovIDs[stageLog.GovID] = true
			
			switch stageLog.Status {
			case "started", "running":
				stageProgress.Running++
			case "completed":
				stageProgress.Completed++
			case "failed":
				stageProgress.Failed++
			default:
				stageProgress.Pending++
			}
			
			// Track first start time for stage
			if stageProgress.StartedAt == nil || stageLog.StartTime.Before(*stageProgress.StartedAt) {
				stageProgress.StartedAt = &stageLog.StartTime
			}
			
			// Track latest completion time for stage
			if stageLog.EndTime != nil && (stageProgress.CompletedAt == nil || stageLog.EndTime.After(*stageProgress.CompletedAt)) {
				stageProgress.CompletedAt = stageLog.EndTime
			}
		}
		
		progress.StageProgress[stage] = stageProgress
	}
	
	progress.TotalGovIDs = len(allGovIDs)
	
	return progress, nil
}

// GetLogDirectoryStats returns statistics about the logs directory
func (lm *LogManager) GetLogDirectoryStats() (map[string]interface{}, error) {
	files, err := os.ReadDir(lm.LogsDirectory)
	if err != nil {
		return nil, fmt.Errorf("failed to read logs directory: %w", err)
	}

	stats := map[string]interface{}{
		"total_files": 0,
		"total_size":  int64(0),
		"oldest_file": (*time.Time)(nil),
		"newest_file": (*time.Time)(nil),
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".log") {
			continue
		}

		info, err := file.Info()
		if err != nil {
			continue
		}

		stats["total_files"] = stats["total_files"].(int) + 1
		stats["total_size"] = stats["total_size"].(int64) + info.Size()

		modTime := info.ModTime()
		if stats["oldest_file"] == nil || modTime.Before(*stats["oldest_file"].(*time.Time)) {
			stats["oldest_file"] = &modTime
		}
		if stats["newest_file"] == nil || modTime.After(*stats["newest_file"].(*time.Time)) {
			stats["newest_file"] = &modTime
		}
	}

	return stats, nil
}

// SearchLogs searches for log entries containing the specified term
func (lm *LogManager) SearchLogs(jobID, searchTerm, level string) ([]LogFileEntry, error) {
	logs, err := lm.ReadLogFile(jobID, 10000, 0) // Read all logs
	if err != nil {
		return nil, err
	}

	var results []LogFileEntry
	searchTermLower := strings.ToLower(searchTerm)

	for _, log := range logs.Logs {
		// Convert back to LogFileEntry for consistent return type
		logFileEntry := LogFileEntry{
			Timestamp: log.Timestamp.Format(time.RFC3339),
			Level:     string(log.Level),
			Message:   log.Message,
			WorkerID:  log.WorkerID,
			JobID:     jobID,
			Metadata:  log.Metadata,
		}

		// Check if message contains search term
		matchesSearch := strings.Contains(strings.ToLower(log.Message), searchTermLower)
		
		// Check if level matches (if specified)
		matchesLevel := level == "" || string(log.Level) == level

		if matchesSearch && matchesLevel {
			results = append(results, logFileEntry)
		}
	}

	return results, nil
}

// EnsureLogDirectory creates the logs directory if it doesn't exist
func (lm *LogManager) EnsureLogDirectory() error {
	return os.MkdirAll(lm.LogsDirectory, 0755)
}