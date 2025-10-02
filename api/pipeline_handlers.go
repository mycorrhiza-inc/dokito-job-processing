package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// ===== PIPELINE API ENDPOINTS =====

// CreatePipelineJob creates a new pipeline job for end-to-end processing
func (s *Server) CreatePipelineJob(c *gin.Context) {
	var req CreatePipelineJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	// Validate pipeline job request
	if err := s.validatePipelineJobRequest(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	// Determine stages based on pipeline type
	stages := s.getPipelineStages(req.PipelineType)
	if len(req.Stages) > 0 {
		stages = req.Stages // Use custom stages if provided
	}

	// Create the main pipeline job
	pipelineJob := &Job{
		ID:               uuid.New().String(),
		Mode:             ScrapingModePipelineIngest,
		GovIDs:           req.GovIDs,
		State:            req.State,
		JurisdictionName: req.JurisdictionName,
		Status:           JobStatusPending,
		CreatedAt:        time.Now(),
		IsPipeline:       true,
		MaxRetries:       req.MaxRetries,
		DebugInfo:        make(map[string]interface{}),
		Progress: &PipelineProgress{
			TotalGovIDs:     len(req.GovIDs),
			CurrentStage:    "init",
			StageProgress:   make(map[string]StageProgress),
			SubTasks:        make(map[string]SubTaskStatus),
			FailedGovIDs:    make([]string, 0),
			CompletedGovIDs: make([]string, 0),
			LastUpdated:     time.Now(),
		},
	}

	// Add pipeline configuration to debug info
	pipelineJob.DebugInfo["pipeline_type"] = req.PipelineType
	pipelineJob.DebugInfo["stages"] = stages
	pipelineJob.DebugInfo["auto_advance"] = req.AutoAdvance
	pipelineJob.DebugInfo["failure_policy"] = req.FailurePolicy
	pipelineJob.DebugInfo["scraper_config"] = req.ScraperConfig
	pipelineJob.DebugInfo["upload_config"] = req.UploadConfig
	pipelineJob.DebugInfo["process_config"] = req.ProcessConfig

	log.Printf("Creating pipeline job: ID=%s, Type=%s, GovIDs=%d, Stages=%v", 
		pipelineJob.ID, req.PipelineType, len(req.GovIDs), stages)

	// Store pipeline job in database
	if err := s.db.StoreJob(pipelineJob); err != nil {
		log.Printf("ERROR: Failed to store pipeline job: ID=%s, Error=%v", pipelineJob.ID, err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "Failed to store pipeline job"})
		return
	}

	// Publish to pipeline queue
	if err := s.redis.PublishPipelineJob(pipelineJob); err != nil {
		log.Printf("ERROR: Failed to publish pipeline job: ID=%s, Error=%v", pipelineJob.ID, err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "Failed to publish pipeline job"})
		return
	}

	log.Printf("Successfully created and published pipeline job: ID=%s", pipelineJob.ID)
	c.JSON(http.StatusCreated, pipelineJob)
}

// GetPipelineJobProgress retrieves detailed progress for a pipeline job
func (s *Server) GetPipelineJobProgress(c *gin.Context) {
	jobID := c.Param("id")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "Job ID is required"})
		return
	}

	// Get the job from database
	job, err := s.db.GetJob(jobID)
	if err != nil {
		if err.Error() == "job not found" {
			c.JSON(http.StatusNotFound, ErrorResponse{Error: "Pipeline job not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "Failed to retrieve pipeline job"})
		return
	}

	if !job.IsPipeline {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "Job is not a pipeline job"})
		return
	}

	// Get progress from Redis
	progress, err := s.redis.GetPipelineProgress(jobID)
	if err != nil {
		log.Printf("Warning: Failed to get pipeline progress from Redis: %v", err)
		progress = job.Progress // Fallback to job's progress
	}

	// Get detailed stage logs
	stageOverview, err := s.logManager.GetPipelineOverview(jobID)
	if err != nil {
		log.Printf("Warning: Failed to get stage overview: %v", err)
		stageOverview = make(map[string][]*PipelineStageLog)
	}

	response := gin.H{
		"job_id":        jobID,
		"status":        job.Status,
		"progress":      progress,
		"stage_logs":    stageOverview,
		"created_at":    job.CreatedAt,
		"started_at":    job.StartedAt,
		"completed_at":  job.CompletedAt,
		"error":         job.Error,
		"debug_info":    job.DebugInfo,
	}

	c.JSON(http.StatusOK, response)
}

// GetPipelineStageDetails retrieves detailed information about a specific stage
func (s *Server) GetPipelineStageDetails(c *gin.Context) {
	jobID := c.Param("id")
	stage := c.Param("stage")
	
	if jobID == "" || stage == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "Job ID and stage are required"})
		return
	}

	// Get stage logs
	stageLogs, err := s.logManager.GetPipelineStageLog(jobID, stage)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "Failed to retrieve stage details"})
		return
	}

	// Calculate stage statistics
	stats := map[string]interface{}{
		"total_tasks":      len(stageLogs),
		"completed":        0,
		"failed":           0,
		"running":          0,
		"pending":          0,
		"average_duration": "0s",
		"total_duration":   "0s",
	}

	var totalDuration time.Duration
	completedCount := 0

	for _, stageLog := range stageLogs {
		switch stageLog.Status {
		case "completed":
			stats["completed"] = stats["completed"].(int) + 1
			if stageLog.EndTime != nil {
				duration := stageLog.EndTime.Sub(stageLog.StartTime)
				totalDuration += duration
				completedCount++
			}
		case "failed":
			stats["failed"] = stats["failed"].(int) + 1
		case "running":
			stats["running"] = stats["running"].(int) + 1
		default:
			stats["pending"] = stats["pending"].(int) + 1
		}
	}

	if completedCount > 0 {
		stats["average_duration"] = (totalDuration / time.Duration(completedCount)).String()
	}
	stats["total_duration"] = totalDuration.String()

	response := gin.H{
		"job_id":     jobID,
		"stage":      stage,
		"statistics": stats,
		"logs":       stageLogs,
	}

	c.JSON(http.StatusOK, response)
}

// RetryPipelineStage retries a failed pipeline stage
func (s *Server) RetryPipelineStage(c *gin.Context) {
	jobID := c.Param("id")
	stage := c.Param("stage")
	
	if jobID == "" || stage == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "Job ID and stage are required"})
		return
	}

	// Get the pipeline job
	job, err := s.db.GetJob(jobID)
	if err != nil {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: "Pipeline job not found"})
		return
	}

	if !job.IsPipeline {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "Job is not a pipeline job"})
		return
	}

	// Parse request body for retry options
	var retryReq struct {
		GovIDs    []string `json:"gov_ids,omitempty"`    // Specific gov IDs to retry
		ForceAll  bool     `json:"force_all,omitempty"`  // Retry all, including successful
		ResetStage bool    `json:"reset_stage,omitempty"` // Reset entire stage
	}
	
	if err := c.ShouldBindJSON(&retryReq); err != nil {
		// Default to retrying failed items only
		retryReq.GovIDs = job.GovIDs
	}

	log.Printf("Retrying pipeline stage: JobID=%s, Stage=%s, GovIDs=%v", jobID, stage, retryReq.GovIDs)

	// Create sub-tasks for the retry
	retriedTasks := make([]string, 0)
	for _, govID := range retryReq.GovIDs {
		subTaskID, err := s.redis.PublishSubTask(jobID, govID, stage, map[string]interface{}{
			"retry": true,
			"reason": "manual_retry",
		})
		if err != nil {
			log.Printf("Failed to create retry task for %s: %v", govID, err)
			continue
		}
		retriedTasks = append(retriedTasks, subTaskID)
	}

	response := gin.H{
		"message":       "Stage retry initiated",
		"job_id":        jobID,
		"stage":         stage,
		"retried_tasks": retriedTasks,
		"count":         len(retriedTasks),
	}

	c.JSON(http.StatusOK, response)
}

// CancelPipelineJob cancels a running pipeline job
func (s *Server) CancelPipelineJob(c *gin.Context) {
	jobID := c.Param("id")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "Job ID is required"})
		return
	}

	// Get the pipeline job
	job, err := s.db.GetJob(jobID)
	if err != nil {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: "Pipeline job not found"})
		return
	}

	if !job.IsPipeline {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "Job is not a pipeline job"})
		return
	}

	if job.Status == JobStatusCompleted || job.Status == JobStatusCancelled {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "Pipeline job is already completed or cancelled"})
		return
	}

	// Update job status
	if err := s.db.UpdateJobStatus(jobID, JobStatusCancelled, nil, "Pipeline cancelled by user"); err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "Failed to cancel pipeline job"})
		return
	}

	// Publish cancellation to Redis
	if err := s.redis.PublishJobUpdate(jobID, JobStatusCancelled, nil, "Pipeline job cancelled by user"); err != nil {
		log.Printf("Warning: Failed to publish cancellation to Redis: %v", err)
	}

	response := gin.H{
		"message": "Pipeline job cancelled successfully",
		"job_id":  jobID,
	}

	c.JSON(http.StatusOK, response)
}

// GetEnhancedQueueStatus returns enhanced queue statistics including pipeline queues
func (s *Server) GetEnhancedQueueStatus(c *gin.Context) {
	queueLengths, err := s.redis.GetQueueLengths()
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "Failed to get queue status"})
		return
	}

	response := gin.H{
		"queue_lengths": queueLengths,
		"timestamp":     time.Now(),
		"total_queued":  queueLengths["main"] + queueLengths["pipeline"] + queueLengths["subtask"],
	}

	c.JSON(http.StatusOK, response)
}

// validatePipelineJobRequest validates pipeline job creation requests
func (s *Server) validatePipelineJobRequest(req *CreatePipelineJobRequest) error {
	if len(req.GovIDs) == 0 {
		return fmt.Errorf("gov_ids cannot be empty")
	}

	if req.State == "" {
		return fmt.Errorf("state is required")
	}

	if req.JurisdictionName == "" {
		return fmt.Errorf("jurisdiction_name is required")
	}

	validPipelineTypes := []string{"ingest", "upload", "process"}
	validType := false
	for _, vt := range validPipelineTypes {
		if req.PipelineType == vt {
			validType = true
			break
		}
	}
	if !validType {
		return fmt.Errorf("invalid pipeline_type: %s (must be one of: %v)", req.PipelineType, validPipelineTypes)
	}

	return nil
}

// getPipelineStages returns the default stages for a pipeline type
func (s *Server) getPipelineStages(pipelineType string) []string {
	switch pipelineType {
	case "ingest":
		return []string{"scrape", "upload", "process", "ingest"}
	case "upload":
		return []string{"scrape", "upload"}
	case "process":
		return []string{"process", "ingest"}
	default:
		return []string{"scrape", "upload", "process", "ingest"}
	}
}