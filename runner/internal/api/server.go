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

	// Full pipeline endpoint
	mux.HandleFunc("/api/pipeline/full", HandleFullPipeline)

	// Swagger documentation
	mux.Handle("/swagger/", httpSwagger.Handler(
		httpSwagger.URL("http://localhost:8080/swagger/doc.json"),
	))

	return mux
}