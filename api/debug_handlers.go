package main

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// ===== DEBUGGING AND MONITORING API ENDPOINTS =====

// GetPipelineDebugInfo retrieves comprehensive debugging information for a pipeline
func (s *Server) GetPipelineDebugInfo(c *gin.Context) {
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

	// Gather comprehensive debugging information
	debugInfo := gin.H{
		"job_basic_info": gin.H{
			"id":               job.ID,
			"status":           job.Status,
			"created_at":       job.CreatedAt,
			"started_at":       job.StartedAt,
			"completed_at":     job.CompletedAt,
			"total_gov_ids":    len(job.GovIDs),
			"gov_ids":          job.GovIDs,
			"retry_count":      job.RetryCount,
			"max_retries":      job.MaxRetries,
			"error":            job.Error,
		},
		"pipeline_config": job.DebugInfo,
		"system_context":  gin.H{
			"timestamp": time.Now(),
			"server":    s.getServerInfo(),
		},
	}

	// Get pipeline progress with detailed breakdown
	progress, err := s.redis.GetPipelineProgress(jobID)
	if err == nil && progress != nil {
		debugInfo["progress"] = progress
		debugInfo["stage_breakdown"] = s.analyzePipelineProgress(progress)
	}

	// Get detailed stage logs and analysis
	stageOverview, err := s.logManager.GetPipelineOverview(jobID)
	if err == nil {
		debugInfo["stage_logs"] = stageOverview
		debugInfo["failure_analysis"] = s.analyzeFailures(stageOverview)
		debugInfo["performance_metrics"] = s.calculatePerformanceMetrics(stageOverview)
	}

	// Get Redis queue status
	queueLengths, err := s.redis.GetQueueLengths()
	if err == nil {
		debugInfo["queue_status"] = queueLengths
	}

	// Check for stuck tasks
	stuckTasks, err := s.findStuckTasks(jobID)
	if err == nil {
		debugInfo["stuck_tasks"] = stuckTasks
	}

	// Get recent system events
	recentEvents, err := s.getRecentSystemEvents(jobID, 50)
	if err == nil {
		debugInfo["recent_events"] = recentEvents
	}

	c.JSON(http.StatusOK, debugInfo)
}

// GetFailureReport generates a detailed failure report for debugging
func (s *Server) GetFailureReport(c *gin.Context) {
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

	// Get stage overview for failure analysis
	stageOverview, err := s.logManager.GetPipelineOverview(jobID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "Failed to get stage logs"})
		return
	}

	failureReport := gin.H{
		"job_id":        jobID,
		"report_time":   time.Now(),
		"job_status":    job.Status,
		"total_gov_ids": len(job.GovIDs),
	}

	// Analyze failures by stage
	stageFailures := make(map[string]interface{})
	govIdFailures := make(map[string][]string) // gov_id -> list of failed stages
	
	for stage, stageLogs := range stageOverview {
		stageStats := gin.H{
			"total_attempts": len(stageLogs),
			"failures":       0,
			"success_rate":   0.0,
			"common_errors":  make(map[string]int),
			"failed_gov_ids": []string{},
		}

		failedGovIds := []string{}
		errorCounts := make(map[string]int)

		for _, stageLog := range stageLogs {
			if stageLog.Status == "failed" {
				stageStats["failures"] = stageStats["failures"].(int) + 1
				failedGovIds = append(failedGovIds, stageLog.GovID)
				
				// Track error patterns
				if stageLog.Error != "" {
					errorType := s.categorizeError(stageLog.Error)
					errorCounts[errorType]++
				}

				// Track gov ID failures
				if govIdFailures[stageLog.GovID] == nil {
					govIdFailures[stageLog.GovID] = []string{}
				}
				govIdFailures[stageLog.GovID] = append(govIdFailures[stageLog.GovID], stage)
			}
		}

		if len(stageLogs) > 0 {
			successCount := len(stageLogs) - stageStats["failures"].(int)
			stageStats["success_rate"] = float64(successCount) / float64(len(stageLogs))
		}

		stageStats["failed_gov_ids"] = failedGovIds
		stageStats["common_errors"] = errorCounts
		stageFailures[stage] = stageStats
	}

	failureReport["stage_failures"] = stageFailures
	failureReport["gov_id_failures"] = govIdFailures

	// Generate recommendations
	recommendations := s.generateFailureRecommendations(stageFailures, govIdFailures)
	failureReport["recommendations"] = recommendations

	// Identify patterns
	patterns := s.identifyFailurePatterns(stageOverview)
	failureReport["failure_patterns"] = patterns

	c.JSON(http.StatusOK, failureReport)
}

// GetTaskDependencyGraph returns a visualization of task dependencies
func (s *Server) GetTaskDependencyGraph(c *gin.Context) {
	jobID := c.Param("id")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "Job ID is required"})
		return
	}

	// Get pipeline progress to understand task flow
	progress, err := s.redis.GetPipelineProgress(jobID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "Failed to get pipeline progress"})
		return
	}

	if progress == nil {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: "No progress data found"})
		return
	}

	// Build dependency graph
	graph := gin.H{
		"job_id": jobID,
		"stages": []string{},
		"nodes":  []gin.H{},
		"edges":  []gin.H{},
	}

	// Get stages from progress
	stages := make([]string, 0, len(progress.StageProgress))
	for stage := range progress.StageProgress {
		stages = append(stages, stage)
	}
	graph["stages"] = stages

	// Create nodes for each gov_id + stage combination
	nodeId := 0
	nodeMap := make(map[string]int)

	for _, stage := range stages {
		for govId := range progress.SubTasks {
			nodeKey := fmt.Sprintf("%s-%s", govId, stage)
			graph["nodes"] = append(graph["nodes"].([]gin.H), gin.H{
				"id":       nodeId,
				"stage":    stage,
				"gov_id":   govId,
				"label":    fmt.Sprintf("%s\n%s", govId, stage),
				"status":   "unknown", // Will be filled from sub-tasks
			})
			nodeMap[nodeKey] = nodeId
			nodeId++
		}
	}

	// Create edges between sequential stages
	for i := 0; i < len(stages)-1; i++ {
		currentStage := stages[i]
		nextStage := stages[i+1]

		for govId := range progress.SubTasks {
			currentNodeKey := fmt.Sprintf("%s-%s", govId, currentStage)
			nextNodeKey := fmt.Sprintf("%s-%s", govId, nextStage)

			if currentNodeId, exists1 := nodeMap[currentNodeKey]; exists1 {
				if nextNodeId, exists2 := nodeMap[nextNodeKey]; exists2 {
					graph["edges"] = append(graph["edges"].([]gin.H), gin.H{
						"from": currentNodeId,
						"to":   nextNodeId,
						"type": "sequence",
					})
				}
			}
		}
	}

	c.JSON(http.StatusOK, graph)
}

// GetPerformanceMetrics returns detailed performance analytics
func (s *Server) GetPerformanceMetrics(c *gin.Context) {
	// Get query parameters
	jobID := c.Query("job_id")
	hours := c.DefaultQuery("hours", "24")
	
	hoursInt, err := strconv.Atoi(hours)
	if err != nil {
		hoursInt = 24
	}

	// If job_id specified, get metrics for that job
	if jobID != "" {
		metrics, err := s.getJobPerformanceMetrics(jobID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
			return
		}
		c.JSON(http.StatusOK, metrics)
		return
	}

	// Otherwise, get system-wide metrics
	systemMetrics, err := s.getSystemPerformanceMetrics(hoursInt)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}

	c.JSON(http.StatusOK, systemMetrics)
}

// GetSystemHealth returns comprehensive system health information
func (s *Server) GetSystemHealth(c *gin.Context) {
	health := gin.H{
		"timestamp": time.Now(),
		"overall_status": "healthy",
	}

	// Redis health
	if err := s.redis.HealthCheck(); err != nil {
		health["redis"] = gin.H{"status": "unhealthy", "error": err.Error()}
		health["overall_status"] = "degraded"
	} else {
		health["redis"] = gin.H{"status": "healthy"}
	}

	// Database health  
	if err := s.db.HealthCheck(); err != nil {
		health["database"] = gin.H{"status": "unhealthy", "error": err.Error()}
		health["overall_status"] = "degraded"
	} else {
		health["database"] = gin.H{"status": "healthy"}
	}

	// Queue health
	queueLengths, err := s.redis.GetQueueLengths()
	if err != nil {
		health["queues"] = gin.H{"status": "unhealthy", "error": err.Error()}
		health["overall_status"] = "degraded"
	} else {
		totalQueued := int64(0)
		for _, length := range queueLengths {
			totalQueued += length
		}
		
		queueStatus := "healthy"
		if totalQueued > 1000 {
			queueStatus = "warning"
			health["overall_status"] = "degraded"
		}
		
		health["queues"] = gin.H{
			"status": queueStatus,
			"lengths": queueLengths,
			"total_queued": totalQueued,
		}
	}

	// Dokito backend health
	if s.dokitoClient != nil {
		if err := s.dokitoClient.HealthCheck(); err != nil {
			health["dokito_backend"] = gin.H{"status": "unhealthy", "error": err.Error()}
		} else {
			health["dokito_backend"] = gin.H{"status": "healthy"}
		}
	}

	c.JSON(http.StatusOK, health)
}

// Helper methods for analysis

func (s *Server) analyzePipelineProgress(progress *PipelineProgress) gin.H {
	analysis := gin.H{
		"current_stage":     progress.CurrentStage,
		"total_gov_ids":     progress.TotalGovIDs,
		"completed_count":   len(progress.CompletedGovIDs),
		"failed_count":      len(progress.FailedGovIDs),
		"completion_rate":   0.0,
		"stages_summary":    gin.H{},
	}

	if progress.TotalGovIDs > 0 {
		analysis["completion_rate"] = float64(len(progress.CompletedGovIDs)) / float64(progress.TotalGovIDs)
	}

	// Analyze each stage
	stagesSummary := gin.H{}
	for stage, stageProgress := range progress.StageProgress {
		total := stageProgress.Pending + stageProgress.Running + stageProgress.Completed + stageProgress.Failed
		successRate := 0.0
		if total > 0 {
			successRate = float64(stageProgress.Completed) / float64(total)
		}

		stagesSummary[stage] = gin.H{
			"total":        total,
			"pending":      stageProgress.Pending,
			"running":      stageProgress.Running,
			"completed":    stageProgress.Completed,
			"failed":       stageProgress.Failed,
			"success_rate": successRate,
		}
	}
	analysis["stages_summary"] = stagesSummary

	return analysis
}

func (s *Server) analyzeFailures(stageOverview map[string][]*PipelineStageLog) gin.H {
	analysis := gin.H{
		"total_failures":     0,
		"failures_by_stage":  gin.H{},
		"common_error_types": gin.H{},
		"retry_analysis":     gin.H{},
	}

	totalFailures := 0
	stageFailures := gin.H{}
	errorTypeCounts := make(map[string]int)
	retryStats := gin.H{
		"total_retries": 0,
		"avg_retries":   0.0,
		"max_retries":   0,
	}

	totalTasks := 0
	totalRetries := 0
	maxRetries := 0

	for stage, stageLogs := range stageOverview {
		stageFailureCount := 0
		
		for _, stageLog := range stageLogs {
			totalTasks++
			
			if stageLog.RetryCount > 0 {
				totalRetries += stageLog.RetryCount
				if stageLog.RetryCount > maxRetries {
					maxRetries = stageLog.RetryCount
				}
			}

			if stageLog.Status == "failed" {
				totalFailures++
				stageFailureCount++
				
				// Categorize error type
				errorType := s.categorizeError(stageLog.Error)
				errorTypeCounts[errorType]++
			}
		}
		
		stageFailures[stage] = stageFailureCount
	}

	analysis["total_failures"] = totalFailures
	analysis["failures_by_stage"] = stageFailures
	analysis["common_error_types"] = errorTypeCounts

	if totalTasks > 0 {
		retryStats["avg_retries"] = float64(totalRetries) / float64(totalTasks)
	}
	retryStats["total_retries"] = totalRetries
	retryStats["max_retries"] = maxRetries
	analysis["retry_analysis"] = retryStats

	return analysis
}

func (s *Server) categorizeError(errorMsg string) string {
	if errorMsg == "" {
		return "unknown"
	}

	// Simple error categorization - could be more sophisticated
	switch {
	case contains(errorMsg, "timeout", "ETIMEDOUT"):
		return "timeout"
	case contains(errorMsg, "network", "connection", "ECONNREFUSED"):
		return "network"
	case contains(errorMsg, "parse", "JSON", "unmarshal"):
		return "data_format"
	case contains(errorMsg, "permission", "access", "401", "403"):
		return "permission"
	case contains(errorMsg, "not found", "404"):
		return "not_found"
	case contains(errorMsg, "rate limit", "429"):
		return "rate_limit"
	case contains(errorMsg, "server error", "500", "502", "503"):
		return "server_error"
	default:
		return "other"
	}
}

func contains(text string, substrings ...string) bool {
	for _, substr := range substrings {
		if len(text) >= len(substr) {
			for i := 0; i <= len(text)-len(substr); i++ {
				if text[i:i+len(substr)] == substr {
					return true
				}
			}
		}
	}
	return false
}

func (s *Server) getServerInfo() gin.H {
	return gin.H{
		"version":   "1.0.0", // This should come from build info
		"build_time": "2024-01-01", // This should come from build info
		"go_version": "1.21", // This should come from runtime
	}
}

// Additional helper methods would be implemented here for:
// - calculatePerformanceMetrics
// - findStuckTasks  
// - getRecentSystemEvents
// - generateFailureRecommendations
// - identifyFailurePatterns
// - getJobPerformanceMetrics
// - getSystemPerformanceMetrics