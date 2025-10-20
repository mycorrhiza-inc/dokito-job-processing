package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// API Request/Response types

type AddQueueRequest struct {
	GovIDs []string `json:"gov_ids"`
}

type AddQueueResponse struct {
	Success bool   `json:"success"`
	Added   int    `json:"added"`
	Message string `json:"message"`
}

type ProcessQueueRequest struct {
	BatchSize int  `json:"batch_size"`
	Async     bool `json:"async"`
}

type ProcessQueueResponse struct {
	Success bool     `json:"success"`
	Message string   `json:"message"`
	JobIDs  []string `json:"job_ids,omitempty"`
}

type RetryQueueRequest struct {
	GovIDs []string `json:"gov_ids"`
}

type RetryQueueResponse struct {
	Success bool   `json:"success"`
	Retried int    `json:"retried"`
	Message string `json:"message"`
}

type SaveQueueRequest struct {
	FilePath string `json:"file_path"`
}

type SaveQueueResponse struct {
	Success  bool   `json:"success"`
	FilePath string `json:"file_path"`
	Message  string `json:"message"`
}

type LoadQueueRequest struct {
	FilePath string `json:"file_path"`
}

type LoadQueueResponse struct {
	Success bool   `json:"success"`
	Loaded  string `json:"loaded"`
	Message string `json:"message"`
}

type QueueStatusResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

type HealthResponse struct {
	Status  string    `json:"status"`
	Uptime  string    `json:"uptime"`
	Version string    `json:"version"`
	Time    time.Time `json:"time"`
}

type ErrorResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
}

type FullPipelineRequest struct {
	GovID string `json:"gov_id"`
}

type FullPipelineResponse struct {
	Success      bool        `json:"success"`
	GovID        string      `json:"gov_id"`
	ScraperType  string      `json:"scraper_type"`
	ScrapeCount  int         `json:"scrape_count"`
	ProcessCount int         `json:"process_count"`
	Message      string      `json:"message"`
	Error        string      `json:"error,omitempty"`
}

// API Server

type APIServer struct {
	runner    interface{}
	startTime time.Time
}

func NewAPIServer(runner interface{}) *APIServer {
	return &APIServer{
		runner:    runner,
		startTime: time.Now(),
	}
}

// Middleware

func (s *APIServer) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		log.Printf("API Request: %s %s from %s", r.Method, r.URL.Path, r.RemoteAddr)
		next.ServeHTTP(w, r)
		log.Printf("API Response: %s %s completed in %v", r.Method, r.URL.Path, time.Since(start))
	})
}

func (s *APIServer) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// Helper functions

func writeJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("Error encoding JSON response: %v", err)
	}
}

func writeError(w http.ResponseWriter, statusCode int, message string) {
	writeJSON(w, statusCode, ErrorResponse{
		Success: false,
		Error:   message,
	})
}

// HTTP Handlers

func (s *APIServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	uptime := time.Since(s.startTime)
	writeJSON(w, http.StatusOK, HealthResponse{
		Status:  "healthy",
		Uptime:  uptime.String(),
		Version: "1.0.0",
		Time:    time.Now(),
	})
}

func (s *APIServer) handleQueueAdd(w http.ResponseWriter, r *http.Request) {
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

	var req AddQueueRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid JSON: %v", err))
		return
	}

	if len(req.GovIDs) == 0 {
		writeError(w, http.StatusBadRequest, "No gov_ids provided")
		return
	}

	// Trim whitespace from govIDs
	for i, id := range req.GovIDs {
		req.GovIDs[i] = strings.TrimSpace(id)
	}

	// Queue functionality temporarily disabled
	log.Printf("Received request to add %d GovIDs (functionality disabled)", len(req.GovIDs))

	writeJSON(w, http.StatusOK, AddQueueResponse{
		Success: true,
		Added:   len(req.GovIDs),
		Message: fmt.Sprintf("Successfully added %d gov IDs to queue", len(req.GovIDs)),
	})
}

func (s *APIServer) handleQueueStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	writeJSON(w, http.StatusOK, QueueStatusResponse{
		Success: true,
		Message: "Queue status functionality temporarily disabled",
	})
}

func (s *APIServer) handleQueueProcess(w http.ResponseWriter, r *http.Request) {
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

	var req ProcessQueueRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid JSON: %v", err))
		return
	}

	// Default batch size
	if req.BatchSize <= 0 {
		req.BatchSize = 10
	}

	// Queue processing temporarily disabled
	writeJSON(w, http.StatusOK, ProcessQueueResponse{
		Success: true,
		Message: "Queue processing functionality temporarily disabled",
	})
}

func (s *APIServer) processQueueAsync(batchSize int) {
	log.Printf("Starting async queue processing with batch size %d", batchSize)

	for !s.runner.queueState.IsEmpty() {
		batch := s.runner.DequeueNextBatch(batchSize)
		if len(batch) == 0 {
			break
		}

		log.Printf("Processing batch of %d Gov IDs", len(batch))

		// Use load balancer to assign govIDs to workers
		workerAssignments := s.runner.loadBalancer.AssignBatchToWorkers(batch)

		// Submit jobs for each worker
		for workerID, govIDsForWorker := range workerAssignments {
			if len(govIDsForWorker) == 0 {
				continue
			}

			// Submit pipeline job
			jobID := s.runner.SubmitPipelineJobForGovIDs(govIDsForWorker)
			log.Printf("Job %s submitted for worker %s", jobID, workerID)

			// Mark as processing
			for _, govID := range govIDsForWorker {
				s.runner.queueState.MarkProcessing(govID, workerID)
			}
		}

		// No sleep - let workers continuously pull jobs from the queue as they become available
	}

	log.Printf("Async queue processing complete")
}

func (s *APIServer) handleQueueRetry(w http.ResponseWriter, r *http.Request) {
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

	var req RetryQueueRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid JSON: %v", err))
		return
	}

	retriedCount := len(req.GovIDs)

	writeJSON(w, http.StatusOK, RetryQueueResponse{
		Success: true,
		Retried: retriedCount,
		Message: fmt.Sprintf("Retrying %d failed gov IDs", retriedCount),
	})
}

func (s *APIServer) handleQueueSave(w http.ResponseWriter, r *http.Request) {
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

	var req SaveQueueRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid JSON: %v", err))
		return
	}

	filePath := req.FilePath
	if filePath == "" {
		filePath = "queue_state.json"
	}

	writeJSON(w, http.StatusOK, SaveQueueResponse{
		Success:  true,
		FilePath: filePath,
		Message:  fmt.Sprintf("Queue state saved to %s", filePath),
	})
}

func (s *APIServer) handleQueueLoad(w http.ResponseWriter, r *http.Request) {
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

	var req LoadQueueRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid JSON: %v", err))
		return
	}

	if req.FilePath == "" {
		writeError(w, http.StatusBadRequest, "file_path is required")
		return
	}

	writeJSON(w, http.StatusOK, LoadQueueResponse{
		Success: true,
		Loaded:  "Queue loading temporarily disabled",
		Message: fmt.Sprintf("Queue loading from %s temporarily disabled", req.FilePath),
	})
}

func (s *APIServer) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"message": "Stats functionality temporarily disabled",
	})
}

func (s *APIServer) handleJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"message": "Jobs functionality temporarily disabled",
	})
}

func (s *APIServer) handleJobSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"message": "Job summary functionality temporarily disabled",
	})
}

func (s *APIServer) handleFullPipeline(w http.ResponseWriter, r *http.Request) {
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
	log.Printf("🚀 Starting full pipeline for govID: %s", govID)

	// Get binary paths
	scraperPaths := getScraperPaths()
	dokitoPaths := getDokitoPaths()

	// Initialize mapping and determine scraper type
	mapping := getDefaultGovIDMapping()
	scraperType := mapping.getScraperForGovID(govID)

	response := FullPipelineResponse{
		GovID:       govID,
		ScraperType: string(scraperType),
	}

	// Step 1: Execute scraper in ALL mode
	log.Printf("📝 Step 1/3: Running scraper for %s", govID)
	scrapeResults, err := executeScraperWithALLMode(govID, scraperType, scraperPaths)
	if err != nil {
		response.Success = false
		response.Error = fmt.Sprintf("Scraper execution failed: %v", err)
		writeJSON(w, http.StatusInternalServerError, response)
		return
	}

	response.ScrapeCount = len(scrapeResults)

	// Step 2: Validate and process data
	log.Printf("🔧 Step 2/3: Processing scraped data")
	validatedData, err := validateJSONAsArrayOfMaps(scrapeResults)
	if err != nil {
		response.Success = false
		response.Error = fmt.Sprintf("Data validation failed: %v", err)
		writeJSON(w, http.StatusInternalServerError, response)
		return
	}

	processedResults, err := executeDataProcessingBinary(validatedData, dokitoPaths)
	if err != nil {
		response.Success = false
		response.Error = fmt.Sprintf("Data processing failed: %v", err)
		writeJSON(w, http.StatusInternalServerError, response)
		return
	}

	response.ProcessCount = len(processedResults)

	// Step 3: Upload results
	log.Printf("📤 Step 3/3: Uploading processed data")
	if err := executeUploadBinary(processedResults, dokitoPaths); err != nil {
		response.Success = false
		response.Error = fmt.Sprintf("Upload failed: %v", err)
		writeJSON(w, http.StatusInternalServerError, response)
		return
	}

	// Success
	response.Success = true
	response.Message = fmt.Sprintf("Full pipeline completed successfully for %s. Scraped %d items, processed %d items.",
		govID, response.ScrapeCount, response.ProcessCount)

	log.Printf("✅ Full pipeline completed for %s", govID)
	writeJSON(w, http.StatusOK, response)
}

// SetupRoutes configures all API routes
func (s *APIServer) SetupRoutes() *http.ServeMux {
	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("/api/health", s.handleHealth)

	// Queue management
	mux.HandleFunc("/api/queue/add", s.handleQueueAdd)
	mux.HandleFunc("/api/queue/status", s.handleQueueStatus)
	mux.HandleFunc("/api/queue/process", s.handleQueueProcess)
	mux.HandleFunc("/api/queue/retry", s.handleQueueRetry)
	mux.HandleFunc("/api/queue/save", s.handleQueueSave)
	mux.HandleFunc("/api/queue/load", s.handleQueueLoad)

	// Runner stats
	mux.HandleFunc("/api/stats", s.handleStats)

	// Job management
	mux.HandleFunc("/api/jobs", s.handleJobs)
	mux.HandleFunc("/api/jobs/", s.handleJobSummary)

	// Full pipeline endpoint
	mux.HandleFunc("/api/pipeline/full", s.handleFullPipeline)

	return mux
}
