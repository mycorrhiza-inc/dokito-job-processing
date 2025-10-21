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
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	"runner/internal"
	"runner/internal/api"
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

	// Setup routes
	mux := api.SetupRoutes()

	// Apply middleware
	handler := internal.LoggingMiddleware(internal.CorsMiddleware(mux))

	// Start server
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: handler,
	}

	log.Printf("✅ Server ready at http://localhost:%d", port)
	log.Printf("📋 Health check: http://localhost:%d/api/health", port)
	log.Printf("🎯 Full pipeline: POST http://localhost:%d/api/pipeline/full", port)

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("❌ Server failed to start: %v", err)
	}
}