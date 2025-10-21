package api

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"runner/internal/core"
)

type HealthResponse struct {
	Status    string            `json:"status"`
	Timestamp string            `json:"timestamp"`
	Services  map[string]string `json:"services"`
}

type FullPipelineRequest struct {
	GovID string `json:"gov_id"`
}

type FullPipelineResponse struct {
	Success      bool   `json:"success"`
	GovID        string `json:"gov_id"`
	ScraperType  string `json:"scraper_type"`
	ScrapeCount  int    `json:"scrape_count"`
	ProcessCount int    `json:"process_count"`
	Message      string `json:"message"`
	Error        string `json:"error,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}

// @Summary Health check endpoint
// @Description Get the health status of the API and all configured services
// @Tags health
// @Accept json
// @Produce json
// @Success 200 {object} HealthResponse
// @Failure 405 {object} map[string]string
// @Router /api/health [get]
func HandleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// Check scraper paths
	scraperPaths := core.GetScraperPaths()
	dokitoPaths := core.GetDokitoPaths()

	services := make(map[string]string)

	if scraperPaths.NYPUCPath != "" {
		services["nypuc_scraper"] = "configured"
	} else {
		services["nypuc_scraper"] = "not_configured"
	}

	if scraperPaths.COPUCPath != "" {
		services["copuc_scraper"] = "configured"
	} else {
		services["copuc_scraper"] = "not_configured"
	}

	if scraperPaths.UtahCoalPath != "" {
		services["utahcoal_scraper"] = "configured"
	} else {
		services["utahcoal_scraper"] = "not_configured"
	}

	if dokitoPaths.ProcessDocketsPath != "" {
		services["process_dockets"] = "configured"
	} else {
		services["process_dockets"] = "not_configured"
	}

	if dokitoPaths.UploadDocketsPath != "" {
		services["upload_dockets"] = "configured"
	} else {
		services["upload_dockets"] = "not_configured"
	}

	response := HealthResponse{
		Status:    "ok",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Services:  services,
	}

	writeJSON(w, http.StatusOK, response)
}

// @Summary Execute full pipeline
// @Description Execute the complete data pipeline for a given government ID including scraping, processing, and uploading
// @Tags pipeline
// @Accept json
// @Produce json
// @Param request body FullPipelineRequest true "Pipeline request with government ID"
// @Success 200 {object} FullPipelineResponse
// @Failure 400 {object} map[string]string
// @Failure 405 {object} map[string]string
// @Failure 500 {object} FullPipelineResponse
// @Router /api/pipeline/full [post]
func HandleFullPipeline(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Failed to read request body")
		return
	}
	defer r.Body.Close()

	var req FullPipelineRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid JSON: %v", err))
		return
	}

	if req.GovID == "" {
		writeError(w, http.StatusBadRequest, "gov_id is required")
		return
	}

	govID := strings.TrimSpace(req.GovID)
	log.Printf("🚀 Starting full pipeline for govID: %s", govID)

	// Get binary paths
	scraperPaths := core.GetScraperPaths()
	dokitoPaths := core.GetDokitoPaths()

	// Initialize mapping and determine scraper type
	mapping := core.GetDefaultGovIDMapping()
	scraperType := mapping.GetScraperForGovID(govID)

	response := FullPipelineResponse{
		GovID:       govID,
		ScraperType: string(scraperType),
	}

	// Step 1: Execute scraper in ALL mode
	log.Printf("📝 Step 1/3: Running scraper for %s", govID)
	scrapeResults, err := core.ExecuteScraperWithALLMode(govID, scraperType, scraperPaths)
	if err != nil {
		response.Success = false
		response.Error = fmt.Sprintf("Scraper execution failed: %v", err)
		writeJSON(w, http.StatusInternalServerError, response)
		return
	}

	response.ScrapeCount = len(scrapeResults)

	// Step 2: Validate and process data
	log.Printf("🔧 Step 2/3: Processing scraped data")
	validatedData, err := core.ValidateJSONAsArrayOfMaps(scrapeResults)
	if err != nil {
		response.Success = false
		response.Error = fmt.Sprintf("Data validation failed: %v", err)
		writeJSON(w, http.StatusInternalServerError, response)
		return
	}

	processedResults, err := core.ExecuteDataProcessingBinary(validatedData, dokitoPaths)
	if err != nil {
		response.Success = false
		response.Error = fmt.Sprintf("Data processing failed: %v", err)
		writeJSON(w, http.StatusInternalServerError, response)
		return
	}

	response.ProcessCount = len(processedResults)

	// Step 3: Upload results
	log.Printf("📤 Step 3/3: Uploading processed data")
	if err := core.ExecuteUploadBinary(processedResults, dokitoPaths); err != nil {
		response.Success = false
		response.Error = fmt.Sprintf("Upload failed: %v", err)
		writeJSON(w, http.StatusInternalServerError, response)
		return
	}

	// Success
	response.Success = true
	response.Message = fmt.Sprintf("Full pipeline completed successfully for %s. Scraped %d items, processed %d items.",
		govID, response.ScrapeCount, response.ProcessCount)

	log.Printf("✅ Full pipeline completed for %s", govID)
	writeJSON(w, http.StatusOK, response)
}