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
	Success bool   `json:"success"`
	Message string `json:"message"`
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
	Success bool       `json:"success"`
	Loaded  QueueStats `json:"loaded"`
	Message string     `json:"message"`
}

type QueueStatusResponse struct {
	Success       bool              `json:"success"`
	Queue         QueueStats        `json:"queue"`
	LoadBalancer  LoadBalanceStats  `json:"load_balancer"`
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

// API Server

type APIServer struct {
	runner    *Runner
	startTime time.Time
}

func NewAPIServer(runner *Runner) *APIServer {
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

	if err := s.runner.EnqueueGovIDs(req.GovIDs); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to enqueue: %v", err))
		return
	}

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

	queueStats := s.runner.GetQueueStats()
	loadBalanceStats := s.runner.GetLoadBalanceStats()

	writeJSON(w, http.StatusOK, QueueStatusResponse{
		Success:      true,
		Queue:        queueStats,
		LoadBalancer: loadBalanceStats,
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

	// Process queue
	if req.Async {
		// Process asynchronously
		go s.processQueueAsync(req.BatchSize)
		writeJSON(w, http.StatusAccepted, ProcessQueueResponse{
			Success: true,
			Message: "Queue processing started asynchronously",
		})
	} else {
		// Process synchronously (just dequeue one batch)
		batch := s.runner.DequeueNextBatch(req.BatchSize)
		if len(batch) == 0 {
			writeJSON(w, http.StatusOK, ProcessQueueResponse{
				Success: true,
				Message: "Queue is empty",
			})
			return
		}

		// Use load balancer to assign govIDs to workers
		workerAssignments := s.runner.loadBalancer.AssignBatchToWorkers(batch)

		jobIDs := make([]string, 0)
		for workerID, govIDsForWorker := range workerAssignments {
			if len(govIDsForWorker) == 0 {
				continue
			}

			// Submit pipeline job
			jobID := s.runner.SubmitPipelineJobForGovIDs(govIDsForWorker)
			jobIDs = append(jobIDs, jobID)

			// Mark as processing
			for _, govID := range govIDsForWorker {
				s.runner.queueState.MarkProcessing(govID, workerID)
			}
		}

		writeJSON(w, http.StatusOK, ProcessQueueResponse{
			Success: true,
			Message: fmt.Sprintf("Processing batch of %d gov IDs", len(batch)),
			JobIDs:  jobIDs,
		})
	}
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

	// Get count before retry
	failedCount := len(s.runner.queueState.FailedGovIDs)

	if err := s.runner.RetryFailedGovIDs(req.GovIDs); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to retry: %v", err))
		return
	}

	retriedCount := failedCount
	if len(req.GovIDs) > 0 {
		retriedCount = len(req.GovIDs)
	}

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

	// Use default path if not provided
	filePath := req.FilePath
	if filePath == "" {
		filePath = s.runner.queueStatePath
	}

	if err := s.runner.queueState.SaveToFile(filePath); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to save: %v", err))
		return
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

	loadedState, err := LoadQueueStateFromFile(req.FilePath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to load: %v", err))
		return
	}

	s.runner.queueState = loadedState

	writeJSON(w, http.StatusOK, LoadQueueResponse{
		Success: true,
		Loaded:  loadedState.GetStats(),
		Message: fmt.Sprintf("Queue state loaded from %s", req.FilePath),
	})
}

func (s *APIServer) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	stats := s.runner.GetStats()
	writeJSON(w, http.StatusOK, stats)
}

func (s *APIServer) handleJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	results := s.runner.GetResults()
	writeJSON(w, http.StatusOK, results)
}

func (s *APIServer) handleJobSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// Extract job ID from path
	path := strings.TrimPrefix(r.URL.Path, "/api/jobs/")
	path = strings.TrimSuffix(path, "/summary")
	jobID := path

	if jobID == "" {
		writeError(w, http.StatusBadRequest, "Job ID is required")
		return
	}

	summary := s.runner.GetJobSummary(jobID)
	if summary == nil {
		writeError(w, http.StatusNotFound, "Job not found")
		return
	}

	writeJSON(w, http.StatusOK, summary)
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

	return mux
}
