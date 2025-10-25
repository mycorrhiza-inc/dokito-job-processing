// Package main provides the Dokito Job Processing API
//
// @title Dokito Job Processing API
// @version 1.0
// @description API for managing data scraping, processing, and upload pipelines for government documents
// @termsOfService http://swagger.io/terms/
//
// @contact.name API Support
// @contact.url http://www.swagger.io/support
// @contact.email support@swagger.io
//
// @license.name MIT
// @license.url https://opensource.org/licenses/MIT
//
// @host localhost:8080
// @BasePath /
// @schemes http https
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"runner/internal"
	"runner/internal/api"
	"runner/internal/worker"
)

func main() {
	// Parse command line arguments for server mode
	port := 8080
	if len(os.Args) > 1 {
		if p, err := strconv.Atoi(os.Args[1]); err == nil {
			port = p
		}
	}

	// Check for -port flag
	for i, arg := range os.Args {
		if arg == "-port" && i+1 < len(os.Args) {
			if p, err := strconv.Atoi(os.Args[i+1]); err == nil {
				port = p
			}
		}
	}

	log.Printf("🚀 Starting Dokito Job Processing Server on port %d", port)

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
		Addr:    fmt.Sprintf(":%d", port),
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

	log.Printf("✅ Server ready at http://localhost:%d", port)
	log.Printf("📋 Health check: http://localhost:%d/api/health", port)
	log.Printf("🎯 Full pipeline: POST http://localhost:%d/api/pipeline/full", port)
	log.Printf("⚡ Async pipeline: POST http://localhost:%d/api/pipeline/async", port)

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("❌ Server failed to start: %v", err)
	}

	log.Printf("✅ Server shutdown complete")
}