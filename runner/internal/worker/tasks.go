package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"runner/internal/core"
	"runner/internal/pipelines"
	"time"

	"github.com/hibiken/asynq"
)

// PipelineTaskRequest represents the parameters for a pipeline task
type PipelineTaskRequest struct {
	GovID              string                         `json:"gov_id"`
	IntermediateSource pipelines.IntermediateSource  `json:"intermediate_source,omitempty"`
	RequestID          string                         `json:"request_id,omitempty"`
	Timestamp          time.Time                      `json:"timestamp"`
	DebugMode          bool                          `json:"debug_mode,omitempty"`
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

const (
	// TypePipelineTask is the task type for processing pipelines
	TypePipelineTask = "process_pipeline"
)

// RegisterTasks registers all task handlers with the queue system
func RegisterTasks() {
	// Register the pipeline processing handler
	ServeMux.HandleFunc(TypePipelineTask, processPipelineHandler)

	log.Printf("📝 Registered pipeline processing task with asynq")
}

// processPipelineHandler is the actual handler function that processes pipeline tasks
func processPipelineHandler(ctx context.Context, t *asynq.Task) error {
	// Parse the task payload
	var req PipelineTaskRequest
	if err := json.Unmarshal(t.Payload(), &req); err != nil {
		return fmt.Errorf("failed to unmarshal task payload: %w", err)
	}

	startTime := time.Now()

	log.Printf("🔄 [Worker] Starting pipeline processing for GovID: %s (RequestID: %s)",
		req.GovID, req.RequestID)

	// Prepare pipeline configuration
	// Use debug mode from request, but fall back to global debug mode if not specified
	debugMode := req.DebugMode || GlobalDebugMode
	config := pipelines.NYPUCPipelineConfig{
		DebugMode:          debugMode,
		IntermediateSource: req.IntermediateSource,
	}

	// Execute the pipeline
	result, err := pipelines.ExecuteNYPUCBasicPipelineWithConfig(ctx, req.GovID, config)

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
func EnqueuePipelineTask(ctx context.Context, govID string, intermediateSource pipelines.IntermediateSource) (string, error) {
	if Client == nil {
		return "", ErrQueueNotInitialized
	}

	// Generate a unique request ID for tracking
	requestID := fmt.Sprintf("pipeline_%s_%d", govID, time.Now().Unix())

	// Get debug mode from execution context
	config := core.GetExecutionConfig(ctx)

	req := PipelineTaskRequest{
		GovID:              govID,
		IntermediateSource: intermediateSource,
		RequestID:          requestID,
		Timestamp:          time.Now(),
		DebugMode:          config.DebugMode,
	}

	// Marshal the request to JSON
	payload, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("failed to marshal task payload: %w", err)
	}

	// Create asynq task
	task := asynq.NewTask(TypePipelineTask, payload, asynq.MaxRetry(3))

	// Enqueue the task
	info, err := Client.Enqueue(task)
	if err != nil {
		return "", fmt.Errorf("failed to enqueue pipeline task: %w", err)
	}

	log.Printf("📋 [Queue] Enqueued pipeline task for GovID: %s (RequestID: %s, TaskID: %s)",
		govID, requestID, info.ID)
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

// BulkQueueMissingGovIds finds missing govids and queues a random subset for processing
func BulkQueueMissingGovIds(ctx context.Context, limit int, intermediateSource pipelines.IntermediateSource) (*BulkQueueResult, error) {
	if Client == nil {
		return nil, ErrQueueNotInitialized
	}

	log.Printf("🔍 [Bulk Queue] Finding missing govids for bulk processing...")

	// Get missing govids using execution context
	missingGovIds, err := pipelines.GetMissingGovIds(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get missing govids: %w", err)
	}

	totalMissing := len(missingGovIds)
	log.Printf("📊 [Bulk Queue] Found %d missing govids", totalMissing)

	if totalMissing == 0 {
		return &BulkQueueResult{
			TotalMissing: 0,
			Queued:       0,
			QueuedGovIds: []string{},
			Message:      "No missing govids found - all dockets are already in the database",
		}, nil
	}

	// Randomize the order
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(missingGovIds), func(i, j int) {
		missingGovIds[i], missingGovIds[j] = missingGovIds[j], missingGovIds[i]
	})

	// Apply limit
	toQueue := missingGovIds
	if limit > 0 && limit < len(missingGovIds) {
		toQueue = missingGovIds[:limit]
		log.Printf("📋 [Bulk Queue] Limiting to %d govids (from %d total missing)", limit, totalMissing)
	}

	// Queue the tasks
	var queuedGovIds []string
	var queueErrors []string

	log.Printf("📤 [Bulk Queue] Queueing %d pipeline tasks...", len(toQueue))

	for i, govID := range toQueue {
		_, err := EnqueuePipelineTask(ctx, govID, intermediateSource)
		if err != nil {
			log.Printf("⚠️  [Bulk Queue] Failed to queue %s: %v", govID, err)
			queueErrors = append(queueErrors, fmt.Sprintf("%s: %v", govID, err))
			continue
		}

		queuedGovIds = append(queuedGovIds, govID)

		// Log progress every 100 items
		if (i+1)%100 == 0 {
			log.Printf("📊 [Bulk Queue] Progress: %d/%d queued", i+1, len(toQueue))
		}
	}

	result := &BulkQueueResult{
		TotalMissing: totalMissing,
		Queued:       len(queuedGovIds),
		QueuedGovIds: queuedGovIds,
		Errors:       queueErrors,
	}

	if len(queueErrors) > 0 {
		result.Message = fmt.Sprintf("Queued %d out of %d govids. %d errors occurred.",
			len(queuedGovIds), len(toQueue), len(queueErrors))
	} else {
		result.Message = fmt.Sprintf("Successfully queued %d govids for processing.", len(queuedGovIds))
	}

	log.Printf("✅ [Bulk Queue] Completed: %d queued, %d errors", len(queuedGovIds), len(queueErrors))
	return result, nil
}

// BulkQueueMissingGovIdsFromMetadata finds missing govids from provided metadata and queues a random subset for processing
func BulkQueueMissingGovIdsFromMetadata(ctx context.Context, limit int, intermediateSource pipelines.IntermediateSource, metadata []map[string]any) (*BulkQueueResult, error) {
	if Client == nil {
		return nil, ErrQueueNotInitialized
	}

	log.Printf("🔍 [Bulk Queue] Finding missing govids from provided metadata...")

	// Get missing govids using provided metadata
	missingGovIds, err := pipelines.GetMissingGovIdsFromMetadata(ctx, metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to get missing govids from metadata: %w", err)
	}

	totalMissing := len(missingGovIds)
	log.Printf("📊 [Bulk Queue] Found %d missing govids from metadata", totalMissing)

	if totalMissing == 0 {
		return &BulkQueueResult{
			TotalMissing: 0,
			Queued:       0,
			QueuedGovIds: []string{},
			Message:      "No missing govids found - all dockets from metadata are already in the database",
		}, nil
	}

	// Randomize the order
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(missingGovIds), func(i, j int) {
		missingGovIds[i], missingGovIds[j] = missingGovIds[j], missingGovIds[i]
	})

	// Apply limit
	toQueue := missingGovIds
	if limit > 0 && limit < len(missingGovIds) {
		toQueue = missingGovIds[:limit]
		log.Printf("📋 [Bulk Queue] Limiting to %d govids (from %d total missing)", limit, totalMissing)
	}

	// Queue the tasks
	var queuedGovIds []string
	var queueErrors []string

	log.Printf("📤 [Bulk Queue] Queueing %d pipeline tasks...", len(toQueue))

	for i, govID := range toQueue {
		_, err := EnqueuePipelineTask(ctx, govID, intermediateSource)
		if err != nil {
			log.Printf("⚠️  [Bulk Queue] Failed to queue %s: %v", govID, err)
			queueErrors = append(queueErrors, fmt.Sprintf("%s: %v", govID, err))
			continue
		}

		queuedGovIds = append(queuedGovIds, govID)

		// Log progress every 100 items
		if (i+1)%100 == 0 {
			log.Printf("📊 [Bulk Queue] Progress: %d/%d queued", i+1, len(toQueue))
		}
	}

	result := &BulkQueueResult{
		TotalMissing: totalMissing,
		Queued:       len(queuedGovIds),
		QueuedGovIds: queuedGovIds,
		Errors:       queueErrors,
	}

	if len(queueErrors) > 0 {
		result.Message = fmt.Sprintf("Queued %d out of %d govids from metadata. %d errors occurred.",
			len(queuedGovIds), len(toQueue), len(queueErrors))
	} else {
		result.Message = fmt.Sprintf("Successfully queued %d govids from metadata for processing.", len(queuedGovIds))
	}

	log.Printf("✅ [Bulk Queue] Completed from metadata: %d queued, %d errors", len(queuedGovIds), len(queueErrors))
	return result, nil
}

// BulkQueueResult represents the result of a bulk queueing operation
type BulkQueueResult struct {
	TotalMissing int      `json:"total_missing"`
	Queued       int      `json:"queued"`
	QueuedGovIds []string `json:"queued_gov_ids"`
	Errors       []string `json:"errors,omitempty"`
	Message      string   `json:"message"`
}