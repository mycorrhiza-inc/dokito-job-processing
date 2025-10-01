package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// Server implements the job management API using Redis and PostgreSQL
type Server struct {
	redis         *RedisClient
	db            *DatabaseClient
	logManager    *LogManager
	dokitoClient  *DokitoClient
}

// NewServer creates a new server instance
func NewServer(redisClient *RedisClient, dbClient *DatabaseClient, logManager *LogManager, dokitoClient *DokitoClient) *Server {
	return &Server{
		redis:         redisClient,
		db:            dbClient,
		logManager:    logManager,
		dokitoClient:  dokitoClient,
	}
}


// CreateJob handles job creation
func (s *Server) CreateJob(c *gin.Context) {
	var req CreateJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	// Validate job parameters based on mode
	if err := s.validateJobRequest(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	// Create job
	job := &Job{
		ID:         uuid.New().String(),
		Mode:       req.Mode,
		GovIDs:     req.GovIDs,
		DateString: req.DateString,
		BeginDate:  req.BeginDate,
		EndDate:    req.EndDate,
		Status:     JobStatusPending,
		CreatedAt:  time.Now(),
		
		// Dokito-specific fields
		State:            req.State,
		JurisdictionName: req.JurisdictionName,
		CaseName:         req.CaseName,
		Blake2bHash:      req.Blake2bHash,
		CaseData:         req.CaseData,
		OperationType:    req.OperationType,
		Limit:            req.Limit,
		Offset:           req.Offset,
	}

	log.Printf("Creating new job: ID=%s, Mode=%s, Client=%s", job.ID, job.Mode, c.ClientIP())

	// Store job in database
	if err := s.db.StoreJob(job); err != nil {
		log.Printf("ERROR: Failed to store job in database: ID=%s, Mode=%s, Error=%v", job.ID, job.Mode, err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "Failed to store job"})
		return
	}

	// Publish job to Redis queue
	if err := s.redis.PublishJob(job); err != nil {
		log.Printf("ERROR: Failed to publish job to Redis: ID=%s, Mode=%s, Error=%v (job stored in database)", job.ID, job.Mode, err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "Failed to publish job"})
		return
	}

	log.Printf("Successfully created and published job: ID=%s, Mode=%s", job.ID, job.Mode)
	c.JSON(http.StatusCreated, job)
}

// GetJob retrieves a specific job
func (s *Server) GetJob(c *gin.Context) {
	jobID := c.Param("id")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "Job ID is required"})
		return
	}

	job, err := s.db.GetJob(jobID)
	if err != nil {
		if err.Error() == "job not found" {
			c.JSON(http.StatusNotFound, ErrorResponse{Error: "Job not found"})
			return
		}
		fmt.Printf("Error retrieving job %s: %v\n", jobID, err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "Failed to retrieve job"})
		return
	}

	// Fetch result from Redis if job is completed
	if job.Status == JobStatusCompleted {
		result, err := s.redis.GetJobResult(jobID)
		if err != nil {
			log.Printf("Warning: Failed to get job result from Redis for job %s: %v", jobID, err)
			// Continue without result rather than failing the whole request
		} else if result != nil {
			job.Result = result
			log.Printf("Successfully retrieved result from Redis for job %s", jobID)
		} else {
			log.Printf("No result found in Redis for completed job %s", jobID)
		}
	}

	c.JSON(http.StatusOK, job)
}

// ListJobs retrieves all jobs with pagination
func (s *Server) ListJobs(c *gin.Context) {
	// Parse pagination parameters
	limitStr := c.DefaultQuery("limit", "10")
	offsetStr := c.DefaultQuery("offset", "0")

	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100 // Cap at 100
	}

	offset, err := strconv.Atoi(offsetStr)
	if err != nil || offset < 0 {
		offset = 0
	}

	jobs, err := s.db.GetAllJobs(limit, offset)
	if err != nil {
		fmt.Printf("Error retrieving jobs: %v\n", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "Failed to retrieve jobs"})
		return
	}

	response := JobListResponse{
		Jobs:  jobs,
		Total: len(jobs),
	}

	c.JSON(http.StatusOK, response)
}

// DeleteJob cancels and removes a job
func (s *Server) DeleteJob(c *gin.Context) {
	jobID := c.Param("id")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "Job ID is required"})
		return
	}

	// Get job first to check if it exists and its status
	job, err := s.db.GetJob(jobID)
	if err != nil {
		if err.Error() == "job not found" {
			c.JSON(http.StatusNotFound, ErrorResponse{Error: "Job not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "Failed to retrieve job"})
		return
	}

	// Update job status to cancelled if it's still pending or running
	if job.Status == JobStatusPending || job.Status == JobStatusRunning {
		if err := s.db.UpdateJobStatus(jobID, JobStatusCancelled, nil, ""); err != nil {
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "Failed to cancel job"})
			return
		}
		
		// Publish job update to Redis
		if err := s.redis.PublishJobUpdate(jobID, JobStatusCancelled, nil, "Job cancelled by user"); err != nil {
			log.Printf("Failed to publish cancellation message for job %s: %v", jobID, err)
			// Continue anyway since database was updated
		} else {
			log.Printf("Published cancellation message for job %s to Redis", jobID)
		}
	}

	c.JSON(http.StatusOK, gin.H{"message": "Job cancelled successfully"})
}

// GetQueueStatus returns queue statistics
func (s *Server) GetQueueStatus(c *gin.Context) {
	queueLength, err := s.redis.GetQueueLength()
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "Failed to get queue status"})
		return
	}

	response := gin.H{
		"queue_length": queueLength,
		"timestamp":    time.Now(),
		"queue_name":   "Redis-based job queue",
	}

	c.JSON(http.StatusOK, response)
}

// HealthCheck endpoint
func (s *Server) HealthCheck(c *gin.Context) {
	services := make(map[string]string)

	// Check Redis
	if err := s.redis.HealthCheck(); err != nil {
		services["redis"] = "unhealthy"
	} else {
		services["redis"] = "healthy"
	}

	// Check Database
	if err := s.db.HealthCheck(); err != nil {
		services["database"] = "unhealthy"
	} else {
		services["database"] = "healthy"
	}

	// Check Dokito Backend
	if err := s.dokitoClient.HealthCheck(); err != nil {
		services["dokito_backend"] = "unhealthy"
	} else {
		services["dokito_backend"] = "healthy"
	}

	status := "healthy"
	for _, serviceStatus := range services {
		if serviceStatus != "healthy" {
			status = "unhealthy"
			break
		}
	}

	response := HealthResponse{
		Status:    status,
		Timestamp: time.Now(),
		Services:  services,
	}

	if status == "healthy" {
		c.JSON(http.StatusOK, response)
	} else {
		c.JSON(http.StatusServiceUnavailable, response)
	}
}

// validateJobRequest validates job request parameters
func (s *Server) validateJobRequest(req *CreateJobRequest) error {
	switch req.Mode {
	case ScrapingModeCaseList:
		// No additional parameters needed
		return nil

	case ScrapingModeDates:
		if req.DateString == "" {
			return fmt.Errorf("date_string is required for dates mode")
		}
		return nil

	case ScrapingModeFilingsBetweenDates:
		if req.BeginDate == "" || req.EndDate == "" {
			return fmt.Errorf("begin_date and end_date are required for filings-between-dates mode")
		}
		return nil

	case ScrapingModeFull, ScrapingModeMetadata, ScrapingModeDocuments, ScrapingModeParties, ScrapingModeFullExtraction:
		if len(req.GovIDs) == 0 {
			return fmt.Errorf("gov_ids are required for %s mode", req.Mode)
		}
		return nil

	// Dokito backend API validations
	case ScrapingModeDokitoCaseFetch:
		if req.State == "" || req.JurisdictionName == "" || req.CaseName == "" {
			return fmt.Errorf("state, jurisdiction_name, and case_name are required for dokito-case-fetch mode")
		}
		return nil

	case ScrapingModeDokitoCaseList:
		if req.State == "" || req.JurisdictionName == "" {
			return fmt.Errorf("state and jurisdiction_name are required for dokito-caselist mode")
		}
		return nil

	case ScrapingModeDokitoAttachmentObj, ScrapingModeDokitoAttachmentRaw:
		if req.Blake2bHash == "" {
			return fmt.Errorf("blake2b_hash is required for %s mode", req.Mode)
		}
		return nil

	case ScrapingModeDokitoCaseSubmit:
		if req.CaseData == nil {
			return fmt.Errorf("case_data is required for dokito-case-submit mode")
		}
		return nil

	case ScrapingModeDokitoReprocess:
		if req.OperationType == "" {
			return fmt.Errorf("operation_type is required for dokito-reprocess mode")
		}
		// Some operations require additional parameters
		switch req.OperationType {
		case "purge":
			if req.State == "" || req.JurisdictionName == "" {
				return fmt.Errorf("state and jurisdiction_name are required for purge operation")
			}
		}
		return nil

	default:
		return fmt.Errorf("unsupported scraping mode: %s", req.Mode)
	}
}


// ScanJobs manually triggers job status scanning
func (s *Server) ScanJobs(c *gin.Context) {
	log.Printf("Manual job scan triggered")
	
	scannedJobs, updatedJobs, err := s.scanStuckJobs()
	if err != nil {
		log.Printf("Error during manual job scan: %v", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "Failed to scan jobs"})
		return
	}
	
	response := gin.H{
		"message": "Job scan completed",
		"scanned_jobs": scannedJobs,
		"updated_jobs": updatedJobs,
		"timestamp": time.Now(),
	}
	
	c.JSON(http.StatusOK, response)
}


// GetJobLogsFromFile retrieves logs for a specific job from log files
func (s *Server) GetJobLogsFromFile(c *gin.Context) {
	fmt.Printf("=== DEBUG HANDLER: GetJobLogsFromFile called ===\n")
	jobID := c.Param("id")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "Job ID is required"})
		return
	}
	fmt.Printf("=== DEBUG HANDLER: Job ID = %s ===\n", jobID)

	// Check if job exists
	_, err := s.db.GetJob(jobID)
	if err != nil {
		if err.Error() == "job not found" {
			c.JSON(http.StatusNotFound, ErrorResponse{Error: "Job not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "Failed to retrieve job"})
		return
	}

	// Parse pagination parameters
	limitStr := c.DefaultQuery("limit", "100")
	offsetStr := c.DefaultQuery("offset", "0")

	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000 // Cap at 1000
	}

	offset, err := strconv.Atoi(offsetStr)
	if err != nil || offset < 0 {
		offset = 0
	}

	// Get logs from file
	response, err := s.logManager.ReadLogFile(jobID, limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "Failed to retrieve logs from file"})
		return
	}

	c.JSON(http.StatusOK, response)
}

// StreamJobLogs streams logs for a specific job using Server-Sent Events
func (s *Server) StreamJobLogs(c *gin.Context) {
	jobID := c.Param("id")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "Job ID is required"})
		return
	}

	// Check if job exists
	_, err := s.db.GetJob(jobID)
	if err != nil {
		if err.Error() == "job not found" {
			c.JSON(http.StatusNotFound, ErrorResponse{Error: "Job not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "Failed to retrieve job"})
		return
	}

	// Set headers for Server-Sent Events
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("Access-Control-Allow-Origin", "*")
	c.Header("Access-Control-Allow-Headers", "Cache-Control")

	// Get initial offset from query parameter
	offsetStr := c.DefaultQuery("offset", "0")
	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		offset = 0
	}

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "Streaming not supported"})
		return
	}

	// Send existing logs first
	if offset == 0 {
		response, err := s.logManager.ReadLogFile(jobID, 1000, 0)
		if err == nil {
			for _, log := range response.Logs {
				logData, _ := json.Marshal(log)
				fmt.Fprintf(c.Writer, "data: %s\n\n", logData)
			}
			flusher.Flush()
		}
	}

	// Stream new logs (simplified polling implementation)
	// In production, you'd want to use file system notifications
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.Request.Context().Done():
			return
		case <-ticker.C:
			// Check for new log entries
			newOffset, err := s.logManager.StreamLogFile(jobID, offset, func(entry LogFileEntry) {
				logData, _ := json.Marshal(entry)
				fmt.Fprintf(c.Writer, "data: %s\n\n", logData)
				flusher.Flush()
			})
			
			if err != nil {
				fmt.Fprintf(c.Writer, "event: error\ndata: %s\n\n", err.Error())
				flusher.Flush()
				return
			}
			
			offset = newOffset
		}
	}
}

// SearchJobLogs searches logs for a specific job
func (s *Server) SearchJobLogs(c *gin.Context) {
	jobID := c.Param("id")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "Job ID is required"})
		return
	}

	// Check if job exists
	_, err := s.db.GetJob(jobID)
	if err != nil {
		if err.Error() == "job not found" {
			c.JSON(http.StatusNotFound, ErrorResponse{Error: "Job not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "Failed to retrieve job"})
		return
	}

	// Get search parameters
	searchTerm := c.Query("q")
	if searchTerm == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "Search term 'q' is required"})
		return
	}

	level := c.Query("level") // Optional level filter

	// Search logs
	results, err := s.logManager.SearchLogs(jobID, searchTerm, level)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "Failed to search logs"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"results": results,
		"total":   len(results),
		"query":   searchTerm,
		"level":   level,
	})
}

// GetLogStats returns statistics about the logs directory
func (s *Server) GetLogStats(c *gin.Context) {
	stats, err := s.logManager.GetLogDirectoryStats()
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "Failed to get log statistics"})
		return
	}

	c.JSON(http.StatusOK, stats)
}

// CleanupLogs manually triggers log cleanup
func (s *Server) CleanupLogs(c *gin.Context) {
	// Get max age from query parameter (in hours)
	maxAgeStr := c.DefaultQuery("max_age_hours", "168") // Default 7 days
	maxAgeHours, err := strconv.Atoi(maxAgeStr)
	if err != nil || maxAgeHours <= 0 {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "Invalid max_age_hours parameter"})
		return
	}

	maxAge := time.Duration(maxAgeHours) * time.Hour
	deletedCount, err := s.logManager.CleanupOldLogs(maxAge)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "Failed to cleanup logs"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":       "Log cleanup completed",
		"deleted_files": deletedCount,
		"max_age_hours": maxAgeHours,
		"timestamp":     time.Now(),
	})
}

// startLogCleanup starts the background log cleanup service
func (s *Server) startLogCleanup(ctx context.Context) {
	cleanupInterval := getEnvAsInt("LOG_CLEANUP_INTERVAL_HOURS", 24) // Default 24 hours
	maxAge := getEnvAsInt("LOG_MAX_AGE_HOURS", 168)                  // Default 7 days
	disabled := os.Getenv("DISABLE_LOG_CLEANUP") == "true"

	if disabled {
		log.Printf("Log cleanup is disabled via DISABLE_LOG_CLEANUP environment variable")
		return
	}

	log.Printf("Starting background log cleanup (interval: %d hours, max age: %d hours)", cleanupInterval, maxAge)

	ticker := time.NewTicker(time.Duration(cleanupInterval) * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("Log cleanup service stopping due to context cancellation")
			return
		case <-ticker.C:
			// Run cleanup
			maxAgeDuration := time.Duration(maxAge) * time.Hour
			deletedCount, err := s.logManager.CleanupOldLogs(maxAgeDuration)
			if err != nil {
				log.Printf("Background log cleanup error: %v", err)
			} else if deletedCount > 0 {
				log.Printf("Background log cleanup: deleted %d old log files", deletedCount)
			}
		}
	}
}

// scanStuckJobs checks for jobs that are stuck in running status and updates them
func (s *Server) scanStuckJobs() (int, int, error) {
	// Get configuration values
	jobTimeoutMinutes := getEnvAsInt("JOB_TIMEOUT_MINUTES", 60) // Default 1 hour
	
	log.Printf("Scanning all running jobs for status mismatches (timeout: %d min)", jobTimeoutMinutes)
	
	// Query for all jobs that are in running status
	runningJobs, err := s.db.GetRunningJobs()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get running jobs: %w", err)
	}
	
	scannedJobs := len(runningJobs)
	updatedJobs := 0
	
	if scannedJobs == 0 {
		log.Printf("No running jobs found")
		return scannedJobs, updatedJobs, nil
	}
	
	log.Printf("Found %d running jobs to check", scannedJobs)
	
	for _, job := range runningJobs {
		// Check if this job is actually still in the Redis queue or being processed
		isInQueue, err := s.redis.IsJobInQueue(job.ID)
		if err != nil {
			log.Printf("Warning: Failed to check if job %s is in Redis queue: %v", job.ID, err)
			// Continue processing even if Redis check fails
		}
		
		// Check if worker has recent activity for this job
		hasRecentActivity, err := s.redis.CheckJobActivity(job.ID, 2) // Check last 2 minutes
		if err != nil {
			log.Printf("Warning: Failed to check Redis activity for job %s: %v", job.ID, err)
		}
		
		if isInQueue || hasRecentActivity {
			log.Printf("Job %s is in queue or has recent activity, skipping", job.ID)
			continue
		}
		
		// Calculate how long the job has been running
		var runningDuration time.Duration
		if job.StartedAt != nil {
			runningDuration = time.Since(*job.StartedAt)
		} else {
			// If no start time, use creation time
			runningDuration = time.Since(job.CreatedAt)
		}
		jobTimeoutDuration := time.Duration(jobTimeoutMinutes) * time.Minute
		
		var newStatus JobStatus
		var errorMsg string
		
		if runningDuration > jobTimeoutDuration {
			// Job has exceeded timeout - mark as failed
			newStatus = JobStatusFailed
			errorMsg = fmt.Sprintf("Job timed out after running for %v (timeout: %v)", runningDuration.Round(time.Minute), jobTimeoutDuration)
			log.Printf("Job %s timed out after %v, marking as failed", job.ID, runningDuration.Round(time.Minute))
		} else {
			// Job is stuck but not timed out - check if worker is still alive
			workerAlive, err := s.redis.CheckWorkerAlive(job.WorkerID)
			if err != nil {
				log.Printf("Warning: Failed to check worker %s status: %v", job.WorkerID, err)
			}
			
			if !workerAlive && job.WorkerID != "" {
				newStatus = JobStatusFailed
				errorMsg = fmt.Sprintf("Job stuck - worker %s appears to be dead (running for %v)", job.WorkerID, runningDuration.Round(time.Minute))
				log.Printf("Job %s worker %s appears dead, marking job as failed", job.ID, job.WorkerID)
			} else {
				log.Printf("Job %s is running longer than threshold but worker appears alive, leaving as-is", job.ID)
				continue
			}
		}
		
		// Update job status in database
		err = s.db.UpdateJobStatus(job.ID, newStatus, nil, errorMsg)
		if err != nil {
			log.Printf("Failed to update stuck job %s status: %v", job.ID, err)
			continue
		}
		
		// Try to publish update to Redis (best effort)
		err = s.redis.PublishJobUpdate(job.ID, newStatus, nil, errorMsg)
		if err != nil {
			log.Printf("Warning: Failed to publish status update to Redis for job %s: %v", job.ID, err)
			// Don't fail the whole operation if Redis publish fails
		}
		
		updatedJobs++
		log.Printf("Updated stuck job %s to status %s", job.ID, newStatus)
	}
	
	log.Printf("Job scan completed: scanned %d jobs, updated %d jobs", scannedJobs, updatedJobs)
	return scannedJobs, updatedJobs, nil
}

// startJobScanner starts the background job scanner that runs every 5 seconds
func (s *Server) startJobScanner(ctx context.Context) {
	scanInterval := getEnvAsInt("JOB_SCAN_INTERVAL_SECONDS", 5) // Default 5 seconds
	disabled := os.Getenv("DISABLE_JOB_SCANNER") == "true"
	
	if disabled {
		log.Printf("Job scanner is disabled via DISABLE_JOB_SCANNER environment variable")
		return
	}
	
	log.Printf("Starting background job scanner (interval: %d seconds)", scanInterval)
	
	ticker := time.NewTicker(time.Duration(scanInterval) * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			log.Printf("Job scanner stopping due to context cancellation")
			return
		case <-ticker.C:
			// Run the scan
			scannedJobs, updatedJobs, err := s.scanStuckJobs()
			if err != nil {
				log.Printf("Background job scan error: %v", err)
			} else if updatedJobs > 0 {
				log.Printf("Background scan: %d jobs scanned, %d jobs updated", scannedJobs, updatedJobs)
			}
			// If no jobs updated, we log at debug level (not implemented here)
		}
	}
}

// getEnvAsInt gets an environment variable as an integer with a default value
func getEnvAsInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

// ===== DOKITO REQUEST VALIDATION FUNCTIONS =====

// validateDateFormat validates that a date string is in YYYY-MM-DD format
func validateDateFormat(dateStr string) error {
	_, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return fmt.Errorf("date must be in YYYY-MM-DD format")
	}
	return nil
}

// validateRawDataAction validates action types for raw dockets endpoint
func validateRawDataAction(action string) error {
	switch ProcessingActionRawData(action) {
	case ProcessingActionRawDataProcessOnly, ProcessingActionRawDataProcessAndIngest, ProcessingActionRawDataUploadRaw:
		return nil
	default:
		return fmt.Errorf("invalid action for raw dockets: %s (must be process_only, process_and_ingest, or upload_raw)", action)
	}
}

// validateIdOnlyAction validates action types for ID-based endpoints
func validateIdOnlyAction(action string) error {
	switch ProcessingActionIdOnly(action) {
	case ProcessingActionIdOnlyProcessOnly, ProcessingActionIdOnlyIngestOnly, ProcessingActionIdOnlyProcessAndIngest:
		return nil
	default:
		return fmt.Errorf("invalid action for ID-based processing: %s (must be process_only, ingest_only, or process_and_ingest)", action)
	}
}

// ===== DOKITO BACKEND API ENDPOINTS =====

// SubmitRawDockets handles raw docket submission to dokito backend
func (s *Server) SubmitRawDockets(c *gin.Context) {
	state := c.Param("state")
	jurisdiction := c.Param("jurisdiction")
	
	if state == "" || jurisdiction == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "State and jurisdiction are required"})
		return
	}

	var req RawDocketsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	// Validate action type for raw data endpoint
	if err := validateRawDataAction(string(req.Action)); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	log.Printf("Submitting %d raw dockets for %s/%s with action: %s", 
		len(req.Dockets), state, jurisdiction, req.Action)

	response, err := s.dokitoClient.SubmitRawDockets(c.Request.Context(), state, jurisdiction, &req)
	if err != nil {
		log.Printf("Error submitting raw dockets: %v", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: fmt.Sprintf("Failed to submit dockets: %v", err)})
		return
	}

	c.JSON(http.StatusOK, response)
}

// ProcessDocketsByIds handles processing dockets by their IDs
func (s *Server) ProcessDocketsByIds(c *gin.Context) {
	state := c.Param("state")
	jurisdiction := c.Param("jurisdiction")
	
	if state == "" || jurisdiction == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "State and jurisdiction are required"})
		return
	}

	var req ByIdsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	// Validate action type for ID-only endpoints
	if err := validateIdOnlyAction(string(req.Action)); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	log.Printf("Processing %d dockets by IDs for %s/%s with action: %s", 
		len(req.DocketIds), state, jurisdiction, req.Action)

	response, err := s.dokitoClient.ProcessByIds(c.Request.Context(), state, jurisdiction, &req)
	if err != nil {
		log.Printf("Error processing dockets by IDs: %v", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: fmt.Sprintf("Failed to process dockets: %v", err)})
		return
	}

	c.JSON(http.StatusOK, response)
}

// ProcessDocketsByJurisdiction handles processing all dockets for a jurisdiction
func (s *Server) ProcessDocketsByJurisdiction(c *gin.Context) {
	state := c.Param("state")
	jurisdiction := c.Param("jurisdiction")
	
	if state == "" || jurisdiction == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "State and jurisdiction are required"})
		return
	}

	var req ByJurisdictionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	// Validate action type for ID-only endpoints
	if err := validateIdOnlyAction(string(req.Action)); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	log.Printf("Processing all dockets for jurisdiction %s/%s with action: %s", 
		state, jurisdiction, req.Action)

	response, err := s.dokitoClient.ProcessByJurisdiction(c.Request.Context(), state, jurisdiction, &req)
	if err != nil {
		log.Printf("Error processing jurisdiction dockets: %v", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: fmt.Sprintf("Failed to process jurisdiction: %v", err)})
		return
	}

	c.JSON(http.StatusOK, response)
}

// ProcessDocketsByDateRange handles processing dockets within a date range
func (s *Server) ProcessDocketsByDateRange(c *gin.Context) {
	state := c.Param("state")
	jurisdiction := c.Param("jurisdiction")
	
	if state == "" || jurisdiction == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "State and jurisdiction are required"})
		return
	}

	var req ByDateRangeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	// Validate date format (YYYY-MM-DD)
	if err := validateDateFormat(req.StartDate); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: fmt.Sprintf("Invalid start_date format: %v", err)})
		return
	}
	if err := validateDateFormat(req.EndDate); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: fmt.Sprintf("Invalid end_date format: %v", err)})
		return
	}

	// Validate action type for ID-only endpoints
	if err := validateIdOnlyAction(string(req.Action)); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	log.Printf("Processing dockets for %s/%s from %s to %s with action: %s", 
		state, jurisdiction, req.StartDate, req.EndDate, req.Action)

	response, err := s.dokitoClient.ProcessByDateRange(c.Request.Context(), state, jurisdiction, &req)
	if err != nil {
		log.Printf("Error processing dockets by date range: %v", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: fmt.Sprintf("Failed to process date range: %v", err)})
		return
	}

	c.JSON(http.StatusOK, response)
}

