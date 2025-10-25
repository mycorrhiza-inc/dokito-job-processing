package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runner/internal/pipelines"
	"time"

	"github.com/vmihailenco/taskq/v3"
)

// PipelineTaskRequest represents the parameters for a pipeline task
type PipelineTaskRequest struct {
	GovID              string                         `json:"gov_id"`
	IntermediateSource pipelines.IntermediateSource  `json:"intermediate_source,omitempty"`
	RequestID          string                         `json:"request_id,omitempty"`
	Timestamp          time.Time                      `json:"timestamp"`
}

// PipelineTaskResult represents the result of a pipeline task
type PipelineTaskResult struct {
	Success      bool                            `json:"success"`
	GovID        string                          `json:"gov_id"`
	ScraperType  string                          `json:"scraper_type"`
	ScrapeCount  int                             `json:"scrape_count"`
	ProcessCount int                             `json:"process_count"`
	Message      string                          `json:"message"`
	Error        string                          `json:"error,omitempty"`
	Duration     time.Duration                   `json:"duration"`
	RequestID    string                          `json:"request_id,omitempty"`
	CompletedAt  time.Time                       `json:"completed_at"`
}

var (
	// PipelineTask is the registered task for processing pipelines
	PipelineTask *taskq.Task
)

// RegisterTasks registers all task handlers with the queue system
func RegisterTasks() {
	PipelineTask = taskq.RegisterTask(&taskq.TaskOptions{
		Name:       "process_pipeline",
		Handler:    processPipelineHandler,
		RetryLimit: 3,
		MinBackoff: 30 * time.Second,
		MaxBackoff: 5 * time.Minute,
	})

	log.Printf("📝 Registered pipeline processing task")
}

// processPipelineHandler is the actual handler function that processes pipeline tasks
func processPipelineHandler(ctx context.Context, req PipelineTaskRequest) error {
	startTime := time.Now()

	log.Printf("🔄 [Worker] Starting pipeline processing for GovID: %s (RequestID: %s)",
		req.GovID, req.RequestID)

	// Prepare pipeline configuration
	config := pipelines.NYPUCPipelineConfig{
		DebugMode:          false, // Disable debug mode for background processing
		IntermediateSource: req.IntermediateSource,
	}

	// Execute the pipeline
	result, err := pipelines.ExecuteNYPUCBasicPipelineWithConfig(req.GovID, config)

	duration := time.Since(startTime)

	if err != nil {
		log.Printf("❌ [Worker] Pipeline failed for GovID %s: %v (Duration: %v)",
			req.GovID, err, duration)

		// Store the failed result for potential retrieval
		failedResult := PipelineTaskResult{
			Success:      false,
			GovID:        req.GovID,
			Error:        err.Error(),
			Duration:     duration,
			RequestID:    req.RequestID,
			CompletedAt:  time.Now(),
		}

		// Store result in Redis for later retrieval (optional)
		storeTaskResult(req.RequestID, failedResult)

		return fmt.Errorf("pipeline processing failed: %w", err)
	}

	// Create successful result
	successResult := PipelineTaskResult{
		Success:      true,
		GovID:        result.GovID,
		ScraperType:  result.ScraperType,
		ScrapeCount:  result.ScrapeCount,
		ProcessCount: result.ProcessCount,
		Message:      fmt.Sprintf("Pipeline completed successfully for %s. Scraped %d items, processed %d items.",
			result.GovID, result.ScrapeCount, result.ProcessCount),
		Duration:     duration,
		RequestID:    req.RequestID,
		CompletedAt:  time.Now(),
	}

	// Store the successful result
	storeTaskResult(req.RequestID, successResult)

	log.Printf("✅ [Worker] Pipeline completed for GovID %s in %v (Scraped: %d, Processed: %d)",
		req.GovID, duration, result.ScrapeCount, result.ProcessCount)

	return nil
}

// EnqueuePipelineTask adds a new pipeline task to the queue
func EnqueuePipelineTask(govID string, intermediateSource pipelines.IntermediateSource) (string, error) {
	if PipelineQueue == nil {
		return "", ErrQueueNotInitialized
	}

	// Generate a unique request ID for tracking
	requestID := fmt.Sprintf("pipeline_%s_%d", govID, time.Now().Unix())

	req := PipelineTaskRequest{
		GovID:              govID,
		IntermediateSource: intermediateSource,
		RequestID:          requestID,
		Timestamp:          time.Now(),
	}

	ctx := context.Background()
	task := PipelineTask.WithArgs(ctx, req)

	err := PipelineQueue.Add(task)
	if err != nil {
		return "", fmt.Errorf("failed to enqueue pipeline task: %w", err)
	}

	log.Printf("📋 [Queue] Enqueued pipeline task for GovID: %s (RequestID: %s)", govID, requestID)
	return requestID, nil
}

// storeTaskResult stores the task result in Redis for later retrieval
func storeTaskResult(requestID string, result PipelineTaskResult) {
	if requestID == "" {
		return
	}

	// This is a simple implementation - in production you might want to use a more robust storage
	resultJSON, err := json.Marshal(result)
	if err != nil {
		log.Printf("⚠️  Failed to marshal task result: %v", err)
		return
	}

	// Store in Redis with a TTL of 24 hours
	ctx := context.Background()
	key := fmt.Sprintf("task_result:%s", requestID)

	// We'll need to get the Redis client from the queue factory
	// For now, just log that we would store it
	log.Printf("📦 [Storage] Would store result for RequestID: %s (Success: %t)",
		requestID, result.Success)

	// TODO: Implement actual Redis storage
	_ = resultJSON
	_ = ctx
	_ = key
}

// GetTaskResult retrieves a task result by request ID
func GetTaskResult(requestID string) (*PipelineTaskResult, error) {
	if requestID == "" {
		return nil, fmt.Errorf("request ID is required")
	}

	// TODO: Implement Redis retrieval
	log.Printf("🔍 [Storage] Would retrieve result for RequestID: %s", requestID)

	return nil, fmt.Errorf("task result retrieval not yet implemented")
}