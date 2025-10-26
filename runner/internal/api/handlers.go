// Package api Handles API related functionality and logic that still might be useful to other packages
package api

import (
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

type FullPipelineRequest struct {
	GovID              string                        `json:"gov_id"`
	IntermediateSource pipelines.IntermediateSource `json:"intermediate_source,omitempty"`
}

type AsyncPipelineRequest struct {
	GovID              string                        `json:"gov_id"`
	IntermediateSource pipelines.IntermediateSource `json:"intermediate_source,omitempty"`
}

type AsyncPipelineResponse struct {
	Success   bool   `json:"success"`
	RequestID string `json:"request_id"`
	GovID     string `json:"gov_id"`
	Message   string `json:"message"`
	Error     string `json:"error,omitempty"`
}

type QueueStatusResponse struct {
	QueueStats map[string]interface{} `json:"queue_stats"`
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

type FullPipelineResponse struct {
	Success      bool   `json:"success"`
	GovID        string `json:"gov_id"`
	ScraperType  string `json:"scraper_type"`
	ScrapeCount  int    `json:"scrape_count"`
	ProcessCount int    `json:"process_count"`
	Message      string `json:"message"`
	Error        string `json:"error,omitempty"`
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

// @Summary Execute full pipeline
// @Description Execute the complete data pipeline for a given government ID including scraping, processing, and uploading
// @Tags pipeline
// @Accept json
// @Produce json
// @Param request body FullPipelineRequest true "Pipeline request with government ID"
// @Success 200 {object} FullPipelineResponse
// @Failure 400 {object} map[string]string
// @Failure 405 {object} map[string]string
// @Failure 500 {object} FullPipelineResponse
// @Router /api/pipeline/full [post]
func HandleFullPipeline(w http.ResponseWriter, r *http.Request) {
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

	var req FullPipelineRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid JSON: %v", err))
		return
	}

	if req.GovID == "" {
		writeError(w, http.StatusBadRequest, "gov_id is required")
		return
	}

	govID := strings.TrimSpace(req.GovID)

	// Execute the shared NY PUC pipeline
	result, err := pipelines.ExecuteNYPUCBasicPipeline(govID)

	response := FullPipelineResponse{
		GovID: govID,
	}

	if err != nil {
		response.Success = false
		response.Error = err.Error()
		if result != nil {
			response.ScraperType = result.ScraperType
			response.ScrapeCount = result.ScrapeCount
			response.ProcessCount = result.ProcessCount
		}
		writeJSON(w, http.StatusInternalServerError, response)
		return
	}

	// Set scraper type from successful result
	response.ScraperType = result.ScraperType

	// Success - populate response with pipeline results
	response.Success = true
	response.ScrapeCount = result.ScrapeCount
	response.ProcessCount = result.ProcessCount
	response.Message = fmt.Sprintf("Full pipeline completed successfully for %s. Scraped %d items, processed %d items.",
		result.GovID, result.ScrapeCount, result.ProcessCount)

	log.Printf("✅ Full pipeline completed for %s", govID)
	writeJSON(w, http.StatusOK, response)
}

// @Summary Execute async pipeline
// @Description Queue a pipeline task for background processing
// @Tags pipeline
// @Accept json
// @Produce json
// @Param request body AsyncPipelineRequest true "Async pipeline request with government ID"
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

	if req.GovID == "" {
		writeError(w, http.StatusBadRequest, "gov_id is required")
		return
	}

	govID := strings.TrimSpace(req.GovID)

	// Enqueue the pipeline task (disable debug mode for API requests)
	requestID, err := worker.EnqueuePipelineTask(govID, req.IntermediateSource, false)

	response := AsyncPipelineResponse{
		GovID: govID,
	}

	if err != nil {
		response.Success = false
		response.Error = err.Error()
		writeJSON(w, http.StatusInternalServerError, response)
		return
	}

	response.Success = true
	response.RequestID = requestID
	response.Message = fmt.Sprintf("Pipeline task queued for %s. Use request_id %s to check status.", govID, requestID)

	log.Printf("📋 Pipeline task queued for %s (RequestID: %s)", govID, requestID)
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

	log.Printf("📋 [Bulk Queue API] Starting bulk queue operation (limit: %d, source: %s)",
		limit, req.IntermediateSource)

	// Execute the bulk queue operation
	result, err := worker.BulkQueueMissingGovIds(limit, req.IntermediateSource, req.DebugMode)

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

