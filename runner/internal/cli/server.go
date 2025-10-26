package cli

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"runner/internal"
	"runner/internal/api"
	"runner/internal/worker"
)

var (
	serverPort        int
	serverConcurrency int
)

// serverCmd represents the server command
var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the Dokito HTTP API server",
	Long: `Start the HTTP API server for the Dokito job processing system.
The server provides REST endpoints for pipeline operations, queue management,
and health monitoring.`,
	Example: `  dokito-cli server --port=8080
  dokito-cli server --port=3000 --debug
  dokito-cli server --port=8080 --concurrency=15`,
	Args: cobra.NoArgs,
	RunE: runServer,
}

func init() {
	rootCmd.AddCommand(serverCmd)

	// Server-specific flags
	serverCmd.Flags().IntVar(&serverPort, "port", 8080, "Port to run the server on")
	serverCmd.Flags().IntVar(&serverConcurrency, "concurrency", 5, "Number of concurrent workers for background processing")
}

func runServer(cmd *cobra.Command, args []string) error {
	// Get debug mode from global flag
	debugMode := GetDebugMode()

	log.Printf("🚀 Starting Dokito Job Processing Server on port %d (debug: %t)", serverPort, debugMode)

	// Set global debug mode for worker tasks
	worker.SetGlobalDebugMode(debugMode)

	// Set worker concurrency from command line flag
	worker.SetWorkerConcurrency(serverConcurrency)

	// Initialize background task queues
	log.Printf("🔧 Initializing task queues...")
	if err := worker.InitializeQueues(); err != nil {
		log.Printf("⚠️  Failed to initialize task queues: %v", err)
		log.Printf("   Server will continue without background processing")
	} else {
		// Register task handlers
		worker.RegisterTasks()

		// Start background workers
		ctx := context.Background()
		if err := worker.StartWorkers(ctx); err != nil {
			log.Printf("⚠️  Failed to start background workers: %v", err)
		}
	}

	// Setup routes
	mux := api.SetupRoutes()

	// Apply middleware
	handler := internal.LoggingMiddleware(internal.CorsMiddleware(mux))

	// Start server
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", serverPort),
		Handler: handler,
	}

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("🛑 Received signal %s, shutting down gracefully...", sig)

		// Stop background workers
		if err := worker.StopWorkers(); err != nil {
			log.Printf("⚠️  Error stopping workers: %v", err)
		}

		// Shutdown HTTP server
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			log.Printf("❌ Server shutdown error: %v", err)
		}
	}()

	log.Printf("✅ Server ready at http://localhost:%d", serverPort)
	log.Printf("📋 Health check: http://localhost:%d/api/health", serverPort)
	log.Printf("🎯 Full pipeline: POST http://localhost:%d/api/pipeline/full", serverPort)
	log.Printf("⚡ Async pipeline: POST http://localhost:%d/api/pipeline/async", serverPort)
	log.Printf("📦 Bulk queue: POST http://localhost:%d/api/pipeline/bulk-queue", serverPort)
	log.Printf("📊 Queue status: GET http://localhost:%d/api/queue/status", serverPort)

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("server failed to start: %w", err)
	}

	log.Printf("✅ Server shutdown complete")
	return nil
}