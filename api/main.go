package main

import (
	"context"
	"log"
	"os"

	"github.com/gin-gonic/gin"
)

func main() {
	// Initialize Redis client
	redisClient, err := NewRedisClient()
	if err != nil {
		log.Fatalf("Failed to initialize Redis client: %v", err)
	}
	defer redisClient.Close()

	// Initialize database client
	dbClient, err := NewDatabaseClient()
	if err != nil {
		log.Fatalf("Failed to initialize database client: %v", err)
	}
	defer dbClient.Close()

	// Start Redis stream consumer for job updates and logs
	ctx := context.Background()
	if err := redisClient.StartConsumer(ctx, dbClient); err != nil {
		log.Fatalf("Failed to start Redis consumer: %v", err)
	}

	// Initialize log manager
	logsDir := getEnvOrDefault("LOGS_DIRECTORY", "/shared/logs")
	logManager := NewLogManager(logsDir)
	if err := logManager.EnsureLogDirectory(); err != nil {
		log.Printf("Warning: Failed to create logs directory: %v", err)
	}

	// Initialize dokito client
	dokitoClient := NewDokitoClient()
	
	// Test dokito backend connectivity
	if err := dokitoClient.HealthCheck(); err != nil {
		log.Printf("Warning: Dokito backend health check failed: %v", err)
		log.Printf("Dokito backend features may not be available")
	} else {
		log.Printf("Dokito backend connection successful")
	}

	// Initialize server
	server := NewServer(redisClient, dbClient, logManager, dokitoClient)
	
	// Start background job scanner
	go server.startJobScanner(ctx)
	
	// Start background log cleanup service
	go server.startLogCleanup(ctx)
	
	startGinServer(server)
}

func startGinServer(server *Server) {
	// Set up Gin router
	r := gin.Default()

	// Add CORS middleware
	r.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	})

	// Health check endpoint
	r.GET("/health", server.HealthCheck)

	// API routes
	api := r.Group("/api/v1")
	{
		// Job management
		api.POST("/jobs", server.CreateJob)
		api.GET("/jobs", server.ListJobs)
		api.GET("/jobs/:id", server.GetJob)
		api.DELETE("/jobs/:id", server.DeleteJob)

		// Job logs (file-based)
		api.GET("/jobs/:id/logs", server.GetJobLogsFromFile)
		api.GET("/jobs/:id/logs/stream", server.StreamJobLogs)
		api.GET("/jobs/:id/logs/search", server.SearchJobLogs)

		// Queue status
		api.GET("/queue/status", server.GetQueueStatus)
		
		// Job scanning
		api.GET("/jobs/scan", server.ScanJobs)
		
		// Log management
		api.GET("/logs/stats", server.GetLogStats)
		api.POST("/logs/cleanup", server.CleanupLogs)
		
		// Dokito Backend proxy endpoints
		dokito := api.Group("/dokito")
		{
			// Docket processing endpoints
			dokito.POST("/dockets/:state/:jurisdiction/submit", server.SubmitRawDockets)
			dokito.POST("/dockets/:state/:jurisdiction/process-by-ids", server.ProcessDocketsByIds)
			dokito.POST("/dockets/:state/:jurisdiction/process-jurisdiction", server.ProcessDocketsByJurisdiction)
			dokito.POST("/dockets/:state/:jurisdiction/process-daterange", server.ProcessDocketsByDateRange)
			
			// Case data endpoints
			dokito.GET("/cases/:state/:jurisdiction/:caseName", server.GetDokitoCase)
			dokito.GET("/caselist/:state/:jurisdiction/all", server.ListDokitoCases)
			dokito.POST("/caselist/:state/:jurisdiction/differential", server.GetDokitoCaseDataDifferential)
			
			// Attachment endpoints
			dokito.GET("/attachments/:hash/metadata", server.GetDokitoAttachmentMetadata)
			dokito.GET("/attachments/:hash/file", server.GetDokitoAttachmentFile)
			
			// Administrative endpoints
			dokito.DELETE("/cases/:state/:jurisdiction/purge", server.PurgeDokitoJurisdiction)
			dokito.GET("/s3/*path", server.ReadDokitoS3File)
		}
	}

	// Get port from environment or default to 8080
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Starting server on port %s", port)
	log.Fatal(r.Run(":" + port))
}

// getEnvOrDefault gets an environment variable with a default value
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}