package worker

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/vmihailenco/taskq/v3"
	"github.com/vmihailenco/taskq/v3/redisq"
)

var (
	// QueueFactory is the global queue factory
	QueueFactory taskq.Factory

	// PipelineQueue handles pipeline processing tasks
	PipelineQueue taskq.Queue
)

// InitializeQueues sets up the Redis-backed task queues
func InitializeQueues() error {
	// Get Redis URL from environment
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://127.0.0.1:6379"
	}

	// Parse Redis options
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return err
	}

	// Create Redis client
	redisClient := redis.NewClient(opt)

	// Test Redis connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Printf("⚠️  Redis connection failed: %v", err)
		log.Printf("   Task queue will operate in degraded mode")
		return err
	}

	// Create queue factory with Redis
	QueueFactory = redisq.NewFactory()

	// Register the pipeline processing queue with 5 concurrent workers
	PipelineQueue = QueueFactory.RegisterQueue(&taskq.QueueOptions{
		Name:         "pipeline-worker",
		Redis:        redisClient,
		MinNumWorker: 5,
		MaxNumWorker: 5,
		// Optional: Add rate limiting if needed
		// RateLimit: redis_rate.PerSecond(10),
	})

	log.Printf("✅ Task queues initialized with Redis at %s", redisURL)
	log.Printf("📋 Pipeline queue configured with 5 concurrent workers")

	return nil
}

// StartWorkers begins processing tasks from the queues
func StartWorkers(ctx context.Context) error {
	if PipelineQueue == nil {
		return ErrQueueNotInitialized
	}

	log.Printf("🚀 Starting background workers...")

	// Start the pipeline queue workers
	if err := PipelineQueue.Consumer().Start(ctx); err != nil {
		return err
	}

	log.Printf("✅ Background workers started and ready to process tasks")
	return nil
}

// StopWorkers gracefully shuts down all workers
func StopWorkers() error {
	if PipelineQueue == nil {
		return nil
	}

	log.Printf("🛑 Stopping background workers...")

	if err := PipelineQueue.Close(); err != nil {
		return err
	}

	log.Printf("✅ Background workers stopped")
	return nil
}

// GetQueueStats returns statistics about queue usage
func GetQueueStats() map[string]interface{} {
	if PipelineQueue == nil {
		return map[string]interface{}{
			"status": "not_initialized",
		}
	}

	return map[string]interface{}{
		"status":         "active",
		"queue_name":     "pipeline-worker",
		"min_workers":    5,
		"max_workers":    5,
		"consumer_stats": PipelineQueue.Consumer().Stats(),
	}
}