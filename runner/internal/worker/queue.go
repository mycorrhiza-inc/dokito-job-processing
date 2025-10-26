package worker

import (
	"context"
	"log"
	"os"
	"strconv"

	"github.com/hibiken/asynq"
)

var (
	// Client is the asynq client for enqueuing tasks
	Client *asynq.Client

	// Server is the asynq server for processing tasks
	Server *asynq.Server

	// ServeMux is the task handler multiplexer
	ServeMux *asynq.ServeMux

	// GlobalDebugMode controls whether debug logging is enabled globally for server mode
	GlobalDebugMode bool

	// WorkerConcurrency stores the current concurrency setting
	WorkerConcurrency int
)

// SetGlobalDebugMode sets the global debug mode for server operations
func SetGlobalDebugMode(enabled bool) {
	GlobalDebugMode = enabled
	if enabled {
		log.Printf("🐛 [Worker] Global debug mode enabled - subprocess logs will be shown")
	}
}

// SetWorkerConcurrency sets the number of concurrent workers
func SetWorkerConcurrency(concurrency int) {
	if concurrency > 0 {
		WorkerConcurrency = concurrency
	}
}

// InitializeQueues sets up the Redis-backed task queues using asynq
func InitializeQueues() error {
	// Get Redis URL from environment
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://127.0.0.1:6379"
	}

	// Set default concurrency if not already set
	if WorkerConcurrency == 0 {
		WorkerConcurrency = 5 // Default to 5 workers

		// Check environment variable for override
		if concurrencyStr := os.Getenv("DOKITO_WORKER_CONCURRENCY"); concurrencyStr != "" {
			if concurrency, err := strconv.Atoi(concurrencyStr); err == nil && concurrency > 0 {
				WorkerConcurrency = concurrency
			} else {
				log.Printf("⚠️  Invalid DOKITO_WORKER_CONCURRENCY value '%s', using default: %d", concurrencyStr, WorkerConcurrency)
			}
		}
	}

	// Parse Redis connection options
	redisOpt, err := asynq.ParseRedisURI(redisURL)
	if err != nil {
		return err
	}

	// Create asynq client for enqueuing tasks
	Client = asynq.NewClient(redisOpt)

	// Create asynq server for processing tasks with configurable concurrent workers
	Server = asynq.NewServer(redisOpt, asynq.Config{
		Concurrency: WorkerConcurrency,
		Queues: map[string]int{
			"default": 1, // All tasks go to default queue with priority 1
		},
		// Retry configuration
		RetryDelayFunc: asynq.DefaultRetryDelayFunc,
		// Logging
		LogLevel: asynq.InfoLevel,
	})

	// Create multiplexer for handling tasks
	ServeMux = asynq.NewServeMux()

	log.Printf("✅ Task queues initialized with Redis at %s", redisURL)
	log.Printf("📋 Asynq server configured with %d concurrent workers", WorkerConcurrency)

	return nil
}

// StartWorkers begins processing tasks from the queues
func StartWorkers(ctx context.Context) error {
	if Server == nil {
		return ErrQueueNotInitialized
	}

	log.Printf("🚀 Starting background workers...")

	// Start the asynq server in a goroutine
	go func() {
		if err := Server.Run(ServeMux); err != nil {
			log.Printf("❌ Asynq server error: %v", err)
		}
	}()

	log.Printf("✅ Background workers started and ready to process tasks")
	return nil
}

// StopWorkers gracefully shuts down all workers
func StopWorkers() error {
	if Server == nil {
		return nil
	}

	log.Printf("🛑 Stopping background workers...")

	Server.Shutdown()

	if Client != nil {
		Client.Close()
	}

	log.Printf("✅ Background workers stopped")
	return nil
}

// GetQueueStats returns statistics about queue usage
func GetQueueStats() map[string]interface{} {
	if Server == nil {
		return map[string]interface{}{
			"status": "not_initialized",
		}
	}

	return map[string]interface{}{
		"status":      "active",
		"queue_name":  "default",
		"concurrency": WorkerConcurrency,
		"library":     "asynq",
	}
}

// ClearQueue removes all pending and scheduled tasks from the queue
func ClearQueue() error {
	if Client == nil {
		return ErrQueueNotInitialized
	}

	// Get Redis connection options to create an inspector
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://127.0.0.1:6379"
	}

	redisOpt, err := asynq.ParseRedisURI(redisURL)
	if err != nil {
		return err
	}

	// Create an inspector to manage the queue
	inspector := asynq.NewInspector(redisOpt)
	defer inspector.Close()

	// Delete all pending tasks in the default queue
	deleted, err := inspector.DeleteAllPendingTasks("default")
	if err != nil {
		return err
	}

	// Delete all scheduled tasks in the default queue
	scheduledDeleted, err := inspector.DeleteAllScheduledTasks("default")
	if err != nil {
		return err
	}

	// Delete all retry tasks in the default queue
	retryDeleted, err := inspector.DeleteAllRetryTasks("default")
	if err != nil {
		return err
	}

	log.Printf("🗑️  Queue cleared: %d pending, %d scheduled, %d retry tasks deleted",
		deleted, scheduledDeleted, retryDeleted)

	return nil
}