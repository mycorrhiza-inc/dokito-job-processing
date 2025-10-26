// Package api Handles API related functionality and logic that still might be useful to other packages
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"runner/internal/core"
	"runner/internal/pipelines"
	"runner/internal/worker"
	"strings"
	"time"
)

type HealthResponse struct {
	Status    string            `json:"status"`
	Timestamp string            `json:"timestamp"`
	Services  map[string]string `json:"services"`
}


type AsyncPipelineRequest struct {
	GovIDs             []string                      `json:"gov_ids"`
	IntermediateSource pipelines.IntermediateSource `json:"intermediate_source,omitempty"`
	DebugMode          bool                          `json:"debug_mode,omitempty"`
}

type AsyncPipelineResponse struct {
	Success    bool     `json:"success"`
	RequestIDs []string `json:"request_ids"`
	GovIDs     []string `json:"gov_ids"`
	Queued     int      `json:"queued"`
	Errors     []string `json:"errors,omitempty"`
	Message    string   `json:"message"`
	Error      string   `json:"error,omitempty"`
}

type QueueStatusResponse struct {
	QueueStats map[string]interface{} `json:"queue_stats"`
}

type QueueClearResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Error   string `json:"error,omitempty"`
}

type BulkQueueRequest struct {
	Limit              int                           `json:"limit,omitempty"`
	IntermediateSource pipelines.IntermediateSource `json:"intermediate_source,omitempty"`
	DebugMode          bool                          `json:"debug_mode,omitempty"`
}

type BulkQueueResponse struct {
	Success      bool     `json:"success"`
	TotalMissing int      `json:"total_missing"`
	Queued       int      `json:"queued"`
	QueuedGovIds []string `json:"queued_gov_ids,omitempty"`
	Errors       []string `json:"errors,omitempty"`
	Message      string   `json:"message"`
	Error        string   `json:"error,omitempty"`
}


func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}

// @Summary Health check endpoint
// @Description Get the health status of the API and all configured services
// @Tags health
// @Accept json
// @Produce json
// @Success 200 {object} HealthResponse
// @Failure 405 {object} map[string]string
// @Router /api/health [get]
func HandleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// Check scraper paths
	scraperPaths := core.GetScraperPaths()
	dokitoPaths := core.GetDokitoPaths()

	services := make(map[string]string)

	if scraperPaths.NYPUCPath != "" {
		services["nypuc_scraper"] = "configured"
	} else {
		services["nypuc_scraper"] = "not_configured"
	}

	if scraperPaths.COPUCPath != "" {
		services["copuc_scraper"] = "configured"
	} else {
		services["copuc_scraper"] = "not_configured"
	}

	if scraperPaths.UtahCoalPath != "" {
		services["utahcoal_scraper"] = "configured"
	} else {
		services["utahcoal_scraper"] = "not_configured"
	}

	if dokitoPaths.ProcessDocketsPath != "" {
		services["process_dockets"] = "configured"
	} else {
		services["process_dockets"] = "not_configured"
	}

	if dokitoPaths.UploadDocketsPath != "" {
		services["upload_dockets"] = "configured"
	} else {
		services["upload_dockets"] = "not_configured"
	}

	response := HealthResponse{
		Status:    "ok",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Services:  services,
	}

	writeJSON(w, http.StatusOK, response)
}


// @Summary Execute async pipeline for multiple IDs
// @Description Queue pipeline tasks for multiple government IDs for background processing
// @Tags pipeline
// @Accept json
// @Produce json
// @Param request body AsyncPipelineRequest true "Async pipeline request with list of government IDs"
// @Success 202 {object} AsyncPipelineResponse
// @Failure 400 {object} map[string]string
// @Failure 405 {object} map[string]string
// @Failure 500 {object} AsyncPipelineResponse
// @Router /api/pipeline/async [post]
func HandleAsyncPipeline(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Failed to read request body")
		return
	}
	defer r.Body.Close()

	var req AsyncPipelineRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid JSON: %v", err))
		return
	}

	if len(req.GovIDs) == 0 {
		writeError(w, http.StatusBadRequest, "gov_ids array is required and cannot be empty")
		return
	}

	// Create execution context (disable debug mode for API requests unless explicitly requested)
	var config *core.ExecutionConfig
	if req.DebugMode {
		config = core.NewExecutionConfigWithDebug()
	} else {
		config = core.NewExecutionConfig()
	}
	ctx := core.WithExecutionConfig(context.Background(), config)

	// Queue tasks for all government IDs
	var requestIDs []string
	var successfulGovIDs []string
	var errors []string

	for _, govID := range req.GovIDs {
		govID = strings.TrimSpace(govID)
		if govID == "" {
			errors = append(errors, "empty gov_id found in list")
			continue
		}

		// Enqueue the pipeline task
		requestID, err := worker.EnqueuePipelineTask(ctx, govID, req.IntermediateSource)
		if err != nil {
			errors = append(errors, fmt.Sprintf("Failed to queue %s: %v", govID, err))
			continue
		}

		requestIDs = append(requestIDs, requestID)
		successfulGovIDs = append(successfulGovIDs, govID)
		log.Printf("📋 Pipeline task queued for %s (RequestID: %s)", govID, requestID)
	}

	response := AsyncPipelineResponse{
		RequestIDs: requestIDs,
		GovIDs:     successfulGovIDs,
		Queued:     len(successfulGovIDs),
		Errors:     errors,
	}

	// Determine response status and message
	if len(successfulGovIDs) == 0 {
		response.Success = false
		response.Error = "Failed to queue any pipeline tasks"
		writeJSON(w, http.StatusInternalServerError, response)
		return
	}

	response.Success = true
	if len(errors) > 0 {
		response.Message = fmt.Sprintf("Queued %d of %d pipeline tasks. %d failed.", len(successfulGovIDs), len(req.GovIDs), len(errors))
	} else {
		response.Message = fmt.Sprintf("Successfully queued %d pipeline tasks.", len(successfulGovIDs))
	}

	log.Printf("📋 Queued %d pipeline tasks, %d errors", len(successfulGovIDs), len(errors))
	writeJSON(w, http.StatusAccepted, response)
}

// @Summary Get queue status
// @Description Get the current status of the background task queues
// @Tags queue
// @Produce json
// @Success 200 {object} QueueStatusResponse
// @Router /api/queue/status [get]
func HandleQueueStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	stats := worker.GetQueueStats()
	response := QueueStatusResponse{
		QueueStats: stats,
	}

	writeJSON(w, http.StatusOK, response)
}

// @Summary Clear queue
// @Description Remove all pending, scheduled, and retry tasks from the queue
// @Tags queue
// @Produce json
// @Success 200 {object} QueueClearResponse
// @Failure 405 {object} map[string]string
// @Failure 500 {object} QueueClearResponse
// @Router /api/queue/clear [delete]
func HandleQueueClear(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	err := worker.ClearQueue()

	response := QueueClearResponse{}

	if err != nil {
		response.Success = false
		response.Error = err.Error()
		writeJSON(w, http.StatusInternalServerError, response)
		return
	}

	response.Success = true
	response.Message = "Queue cleared successfully. All pending, scheduled, and retry tasks have been removed."

	log.Printf("🗑️  Queue cleared via API")
	writeJSON(w, http.StatusOK, response)
}

// @Summary Bulk queue missing govids
// @Description Find missing govids, randomize them, and queue a limited number for background processing
// @Tags pipeline
// @Accept json
// @Produce json
// @Param request body BulkQueueRequest true "Bulk queue request with optional limit and intermediate source"
// @Success 200 {object} BulkQueueResponse
// @Failure 400 {object} map[string]string
// @Failure 405 {object} map[string]string
// @Failure 500 {object} BulkQueueResponse
// @Router /api/pipeline/bulk-queue [post]
func HandleBulkQueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Failed to read request body")
		return
	}
	defer r.Body.Close()

	var req BulkQueueRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid JSON: %v", err))
		return
	}

	// Default limit to 1000 if not specified
	limit := req.Limit
	if limit <= 0 {
		limit = 1000
	}

	log.Printf("📋 [Bulk Queue API] Starting bulk queue operation (limit: %d, source: %s, debug: %t)",
		limit, req.IntermediateSource, req.DebugMode)

	// Create execution context with debug mode from request
	var config *core.ExecutionConfig
	if req.DebugMode {
		config = core.NewExecutionConfigWithDebug()
	} else {
		config = core.NewExecutionConfig()
	}
	ctx := core.WithExecutionConfig(context.Background(), config)

	// Execute the bulk queue operation
	result, err := worker.BulkQueueMissingGovIds(ctx, limit, req.IntermediateSource)

	response := BulkQueueResponse{}

	if err != nil {
		response.Success = false
		response.Error = err.Error()
		log.Printf("❌ [Bulk Queue API] Operation failed: %v", err)
		writeJSON(w, http.StatusInternalServerError, response)
		return
	}

	// Map the worker result to API response
	response.Success = true
	response.TotalMissing = result.TotalMissing
	response.Queued = result.Queued
	response.Errors = result.Errors
	response.Message = result.Message

	// Only include govid list if it's a reasonable size (to avoid huge responses)
	if len(result.QueuedGovIds) <= 100 {
		response.QueuedGovIds = result.QueuedGovIds
	}

	log.Printf("✅ [Bulk Queue API] Operation completed: %d queued", result.Queued)
	writeJSON(w, http.StatusOK, response)
}

