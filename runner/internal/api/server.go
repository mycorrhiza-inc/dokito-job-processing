package api

import (
	"net/http"

	httpSwagger "github.com/swaggo/http-swagger"
	_ "runner/docs"
)

// SetupRoutes configures all API routes
func SetupRoutes() *http.ServeMux {
	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("/api/health", HandleHealth)

	// Pipeline endpoints
	mux.HandleFunc("/api/pipeline/async", HandleAsyncPipeline)
	mux.HandleFunc("/api/pipeline/bulk-queue", HandleBulkQueue)

	// Queue management endpoints
	mux.HandleFunc("/api/queue/status", HandleQueueStatus)

	// Swagger documentation
	mux.Handle("/swagger/", httpSwagger.Handler(
		httpSwagger.URL("/swagger/doc.json"),
	))

	return mux
}