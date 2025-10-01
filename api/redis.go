package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	client       *redis.Client
	jobQueue     string
	statusStream string
	logStream    string
	ctx          context.Context
}

// RedisJob represents the new job format as specified in newfix.md
type RedisJob struct {
	Task   string      `json:"task"`
	Data   interface{} `json:"data"`
	Status string      `json:"status"`
	Log    string      `json:"log"`
}

// NewRedisClient creates a new Redis client
func NewRedisClient() (*RedisClient, error) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379"
	}

	// Parse Redis URL
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		// Fallback to simple connection if URL parsing fails
		opts = &redis.Options{
			Addr:     getEnvOrDefault("REDIS_HOST", "localhost") + ":" + getEnvOrDefault("REDIS_PORT", "6379"),
			Password: getEnvOrDefault("REDIS_PASSWORD", ""),
			DB:       0,
		}
	}

	// Configure Redis client with reasonable defaults
	opts.PoolSize = 20
	opts.MinIdleConns = 5
	opts.ConnMaxLifetime = 30 * time.Minute
	opts.DialTimeout = 10 * time.Second
	opts.ReadTimeout = 10 * time.Second
	opts.WriteTimeout = 10 * time.Second

	rdb := redis.NewClient(opts)
	ctx := context.Background()

	// Test connection
	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}
	log.Printf("Connected to Redis: %s", pong)

	client := &RedisClient{
		client:       rdb,
		jobQueue:     getEnvOrDefault("REDIS_JOB_QUEUE", "jobrunner:jobs"),
		statusStream: getEnvOrDefault("REDIS_STATUS_STREAM", "jobrunner:status"),
		logStream:    getEnvOrDefault("REDIS_LOG_STREAM", "jobrunner:logs"),
		ctx:          ctx,
	}

	log.Printf("Redis client initialized with queue: %s", client.jobQueue)
	return client, nil
}

// Close closes the Redis client connection
func (r *RedisClient) Close() error {
	return r.client.Close()
}

// PublishJob publishes a job to the Redis queue with the new format
func (r *RedisClient) PublishJob(job *Job) error {
	// Convert the Job to the new RedisJob format
	redisJob := RedisJob{
		Task:   "web-worker",
		Data:   job,
		Status: "pending",
		Log:    "",
	}

	jobData, err := json.Marshal(redisJob)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	// Push job to the left of the queue (LPUSH) so workers can BRPOP (blocking right pop)
	err = r.client.LPush(r.ctx, r.jobQueue, jobData).Err()
	if err != nil {
		return fmt.Errorf("failed to publish job to Redis queue: %w", err)
	}

	log.Printf("Job %s published to Redis queue %s", job.ID, r.jobQueue)
	return nil
}

// PublishJobUpdate publishes a job status update to Redis stream
func (r *RedisClient) PublishJobUpdate(jobID string, status JobStatus, result interface{}, error string) error {
	update := map[string]interface{}{
		"job_id":    jobID,
		"status":    string(status),
		"timestamp": time.Now().Format(time.RFC3339),
	}

	if result != nil {
		update["result"] = result
	}

	if error != "" {
		update["error"] = error
	}

	// Publish to Redis stream for real-time updates
	err := r.client.XAdd(r.ctx, &redis.XAddArgs{
		Stream: r.statusStream,
		Values: update,
	}).Err()

	if err != nil {
		return fmt.Errorf("failed to publish job update to Redis stream: %w", err)
	}

	log.Printf("Job update published for job %s: %s", jobID, status)
	return nil
}

// PublishJobLog publishes a log entry for a job to Redis stream
func (r *RedisClient) PublishJobLog(jobID string, logEntry *LogEntry) error {
	logData := map[string]interface{}{
		"job_id":    jobID,
		"level":     string(logEntry.Level),
		"message":   logEntry.Message,
		"timestamp": logEntry.Timestamp.Format(time.RFC3339),
	}

	if logEntry.WorkerID != "" {
		logData["worker_id"] = logEntry.WorkerID
	}

	if logEntry.Metadata != nil {
		logData["metadata"] = logEntry.Metadata
	}

	// Publish to Redis stream for log aggregation
	err := r.client.XAdd(r.ctx, &redis.XAddArgs{
		Stream: r.logStream,
		Values: logData,
	}).Err()

	if err != nil {
		return fmt.Errorf("failed to publish log entry to Redis stream: %w", err)
	}

	return nil
}

// HealthCheck checks if Redis is responsive
func (r *RedisClient) HealthCheck() error {
	pong, err := r.client.Ping(r.ctx).Result()
	if err != nil {
		return fmt.Errorf("Redis health check failed: %w", err)
	}

	if pong != "PONG" {
		return fmt.Errorf("unexpected Redis ping response: %s", pong)
	}

	return nil
}

// GetQueueLength returns the current length of the job queue
func (r *RedisClient) GetQueueLength() (int64, error) {
	length, err := r.client.LLen(r.ctx, r.jobQueue).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get queue length: %w", err)
	}
	return length, nil
}

// StartConsumer starts consuming job status updates and logs from Redis streams
func (r *RedisClient) StartConsumer(ctx context.Context, db *DatabaseClient) error {
	log.Printf("Starting Redis stream consumer for status and logs")

	// Start goroutine to consume status updates
	go r.consumeStatusUpdates(ctx, db)

	// Start goroutine to consume log entries
	go r.consumeLogEntries(ctx, db)

	return nil
}

// consumeStatusUpdates consumes job status updates from Redis stream
func (r *RedisClient) consumeStatusUpdates(ctx context.Context, db *DatabaseClient) {
	consumerGroup := "api-status-consumers"
	consumerName := "api-consumer-" + getEnvOrDefault("HOSTNAME", "default")

	// Create consumer group if it doesn't exist
	r.client.XGroupCreateMkStream(ctx, r.statusStream, consumerGroup, "0")

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Read from stream
			streams, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    consumerGroup,
				Consumer: consumerName,
				Streams:  []string{r.statusStream, ">"},
				Count:    10,
				Block:    time.Second * 5,
			}).Result()

			if err != nil {
				if err != redis.Nil {
					log.Printf("Error reading from status stream: %v", err)
				}
				time.Sleep(time.Second)
				continue
			}

			for _, stream := range streams {
				for _, message := range stream.Messages {
					r.processStatusUpdate(message, db)
					// Acknowledge message
					r.client.XAck(ctx, r.statusStream, consumerGroup, message.ID)
				}
			}
		}
	}
}

// consumeLogEntries consumes log entries from Redis stream
func (r *RedisClient) consumeLogEntries(ctx context.Context, db *DatabaseClient) {
	consumerGroup := "api-log-consumers"
	consumerName := "api-consumer-" + getEnvOrDefault("HOSTNAME", "default")

	// Create consumer group if it doesn't exist
	r.client.XGroupCreateMkStream(ctx, r.logStream, consumerGroup, "0")

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Read from stream
			streams, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    consumerGroup,
				Consumer: consumerName,
				Streams:  []string{r.logStream, ">"},
				Count:    10,
				Block:    time.Second * 5,
			}).Result()

			if err != nil {
				if err != redis.Nil {
					log.Printf("Error reading from log stream: %v", err)
				}
				time.Sleep(time.Second)
				continue
			}

			for _, stream := range streams {
				for _, message := range stream.Messages {
					r.processLogEntry(message, db)
					// Acknowledge message
					r.client.XAck(ctx, r.logStream, consumerGroup, message.ID)
				}
			}
		}
	}
}

// processStatusUpdate processes a job status update message
func (r *RedisClient) processStatusUpdate(message redis.XMessage, db *DatabaseClient) {
	jobID, ok := message.Values["job_id"].(string)
	if !ok {
		log.Printf("Invalid status update: missing job_id")
		return
	}

	statusStr, ok := message.Values["status"].(string)
	if !ok {
		log.Printf("Invalid status update: missing status")
		return
	}

	status := JobStatus(statusStr)
	
	var errorMsg string

	// Note: We no longer extract the "result" field since results are stored in Redis
	// The status update may contain result_summary for immediate viewing

	if e, exists := message.Values["error"]; exists {
		if errStr, ok := e.(string); ok {
			errorMsg = errStr
		}
	}

	// Handle artifact storage information (fallback when Redis storage fails)
	var artifactURL string
	var artifactMetadata interface{}
	
	if artifactURLVal, exists := message.Values["artifact_storage_url"]; exists {
		if artifactURLStr, ok := artifactURLVal.(string); ok {
			artifactURL = artifactURLStr
		}
	}
	
	if artifactMetadataVal, exists := message.Values["artifact_metadata"]; exists {
		artifactMetadata = artifactMetadataVal
	}

	// Update worker ID if provided
	if workerID, exists := message.Values["worker_id"]; exists {
		if workerIDStr, ok := workerID.(string); ok {
			if err := db.UpdateJobWorkerID(jobID, workerIDStr); err != nil {
				log.Printf("Failed to update worker_id for job %s: %v", jobID, err)
			}
		}
	}

	// Update job status in database (without result data)
	if artifactURL != "" || artifactMetadata != nil {
		if err := db.UpdateJobStatusWithArtifacts(jobID, status, nil, errorMsg, artifactURL, artifactMetadata); err != nil {
			log.Printf("Failed to update job status with artifacts for job %s: %v", jobID, err)
		}
	} else {
		if err := db.UpdateJobStatus(jobID, status, nil, errorMsg); err != nil {
			log.Printf("Failed to update job status for job %s: %v", jobID, err)
		}
	}

	log.Printf("Processed status update for job %s: %s", jobID, status)
}

// processLogEntry processes a log entry message
func (r *RedisClient) processLogEntry(message redis.XMessage, db *DatabaseClient) {
	jobID, ok := message.Values["job_id"].(string)
	if !ok {
		log.Printf("Invalid log entry: missing job_id")
		return
	}

	level, ok := message.Values["level"].(string)
	if !ok {
		log.Printf("Invalid log entry: missing level")
		return
	}

	messageText, ok := message.Values["message"].(string)
	if !ok {
		log.Printf("Invalid log entry: missing message")
		return
	}

	_ = message.Values["worker_id"] // workerID not used in this function

	var metadata map[string]interface{}
	if m, exists := message.Values["metadata"]; exists {
		if mStr, ok := m.(string); ok {
			// Try to unmarshal metadata JSON
			json.Unmarshal([]byte(mStr), &metadata)
		}
	}

	// Note: Database log storage removed - using file-based logging instead
	log.Printf("Log entry for job %s: [%s] %s", jobID, level, messageText)
}

// CheckJobActivity checks if a job has had recent activity in Redis streams
func (r *RedisClient) CheckJobActivity(jobID string, thresholdMinutes int) (bool, error) {
	// Check both status and log streams for recent activity
	thresholdTime := time.Now().Add(-time.Duration(thresholdMinutes) * time.Minute)
	thresholdTimeStr := fmt.Sprintf("%d-0", thresholdTime.UnixMilli())
	
	// Check status stream
	statusResults, err := r.client.XRead(r.ctx, &redis.XReadArgs{
		Streams: []string{r.statusStream, thresholdTimeStr},
		Count:   100,
	}).Result()
	
	if err != nil && err != redis.Nil {
		return false, fmt.Errorf("failed to read status stream: %w", err)
	}
	
	// Look for messages related to this job
	for _, stream := range statusResults {
		for _, message := range stream.Messages {
			if jobIDVal, exists := message.Values["job_id"]; exists {
				if jobIDStr, ok := jobIDVal.(string); ok && jobIDStr == jobID {
					log.Printf("Found recent status activity for job %s in Redis", jobID)
					return true, nil
				}
			}
		}
	}
	
	// Check log stream
	logResults, err := r.client.XRead(r.ctx, &redis.XReadArgs{
		Streams: []string{r.logStream, thresholdTimeStr},
		Count:   100,
	}).Result()
	
	if err != nil && err != redis.Nil {
		return false, fmt.Errorf("failed to read log stream: %w", err)
	}
	
	// Look for messages related to this job
	for _, stream := range logResults {
		for _, message := range stream.Messages {
			if jobIDVal, exists := message.Values["job_id"]; exists {
				if jobIDStr, ok := jobIDVal.(string); ok && jobIDStr == jobID {
					log.Printf("Found recent log activity for job %s in Redis", jobID)
					return true, nil
				}
			}
		}
	}
	
	return false, nil
}

// IsJobInQueue checks if a job is currently in the Redis job queue
func (r *RedisClient) IsJobInQueue(jobID string) (bool, error) {
	// Get all jobs in the queue
	queueLength, err := r.client.LLen(r.ctx, r.jobQueue).Result()
	if err != nil {
		return false, fmt.Errorf("failed to get queue length: %w", err)
	}
	
	if queueLength == 0 {
		return false, nil
	}
	
	// Check the jobs in the queue (limited to first 100 to avoid performance issues)
	checkCount := queueLength
	if checkCount > 100 {
		checkCount = 100
	}
	
	jobs, err := r.client.LRange(r.ctx, r.jobQueue, 0, checkCount-1).Result()
	if err != nil {
		return false, fmt.Errorf("failed to get jobs from queue: %w", err)
	}
	
	// Parse each job and check if it matches our job ID
	for _, jobData := range jobs {
		var redisJob RedisJob
		if err := json.Unmarshal([]byte(jobData), &redisJob); err != nil {
			log.Printf("Warning: Failed to unmarshal job data from queue: %v", err)
			continue
		}
		
		// The job data is nested in the Data field
		if jobMap, ok := redisJob.Data.(map[string]interface{}); ok {
			if id, exists := jobMap["id"]; exists {
				if idStr, ok := id.(string); ok && idStr == jobID {
					return true, nil
				}
			}
		}
	}
	
	return false, nil
}

// CheckWorkerAlive checks if a worker is still active by looking for recent heartbeats
func (r *RedisClient) CheckWorkerAlive(workerID string) (bool, error) {
	if workerID == "" {
		return false, nil
	}
	
	// For now, we'll implement a simple check
	// In the future, workers could publish heartbeats to a dedicated stream
	// For now, assume worker is alive if we can't prove otherwise
	
	// Check if worker has published any recent activity (last 2 minutes)
	thresholdTime := time.Now().Add(-2 * time.Minute)
	thresholdTimeStr := fmt.Sprintf("%d-0", thresholdTime.UnixMilli())
	
	// Check status stream for recent worker activity
	results, err := r.client.XRead(r.ctx, &redis.XReadArgs{
		Streams: []string{r.statusStream, thresholdTimeStr},
		Count:   100,
	}).Result()
	
	if err != nil && err != redis.Nil {
		log.Printf("Warning: Failed to check worker activity in Redis: %v", err)
		// If we can't check Redis, assume worker is alive to be safe
		return true, nil
	}
	
	// Look for messages from this worker
	for _, stream := range results {
		for _, message := range stream.Messages {
			if workerIDVal, exists := message.Values["worker_id"]; exists {
				if workerIDStr, ok := workerIDVal.(string); ok && workerIDStr == workerID {
					log.Printf("Found recent activity from worker %s", workerID)
					return true, nil
				}
			}
		}
	}
	
	log.Printf("No recent activity found for worker %s", workerID)
	return false, nil
}

// GetJobResult retrieves job result data from Redis
func (r *RedisClient) GetJobResult(jobID string) (interface{}, error) {
	resultKey := fmt.Sprintf("jobrunner:results:%s", jobID)
	
	resultJSON, err := r.client.Get(r.ctx, resultKey).Result()
	if err != nil {
		if err == redis.Nil {
			// Key doesn't exist - no result stored
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get job result from Redis: %w", err)
	}
	
	// Parse the JSON result
	var result interface{}
	if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
		log.Printf("Warning: Failed to unmarshal job result for %s: %v", jobID, err)
		// Return the raw JSON string if unmarshaling fails
		return resultJSON, nil
	}
	
	return result, nil
}

