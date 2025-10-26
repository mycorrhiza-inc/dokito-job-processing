package worker

import (
	"context"
	"log"
	"os"

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
)

// SetGlobalDebugMode sets the global debug mode for server operations
func SetGlobalDebugMode(enabled bool) {
	GlobalDebugMode = enabled
	if enabled {
		log.Printf("🐛 [Worker] Global debug mode enabled - subprocess logs will be shown")
	}
}

// InitializeQueues sets up the Redis-backed task queues using asynq
func InitializeQueues() error {
	// Get Redis URL from environment
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://127.0.0.1:6379"
	}

	// Parse Redis connection options
	redisOpt, err := asynq.ParseRedisURI(redisURL)
	if err != nil {
		return err
	}

	// Create asynq client for enqueuing tasks
	Client = asynq.NewClient(redisOpt)

	// Create asynq server for processing tasks with 5 concurrent workers
	Server = asynq.NewServer(redisOpt, asynq.Config{
		Concurrency: 5,
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
	log.Printf("📋 Asynq server configured with 5 concurrent workers")

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
		"concurrency": 5,
		"library":     "asynq",
	}
}