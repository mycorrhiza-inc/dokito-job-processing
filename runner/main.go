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
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type Runner struct {
	scraperPaths ScraperBinaryPaths
	dokitoPaths  DokitoBinaryPaths
}

type ScraperBinaryPaths struct {
	NYPUCPath    string
	COPUCPath    string
	UtahCoalPath string
}

func getScraperPaths() ScraperBinaryPaths {
	return ScraperBinaryPaths{
		NYPUCPath:    os.Getenv("OPENSCRAPER_PATH_NYPUC"),
		COPUCPath:    os.Getenv("OPENSCRAPER_PATH_COPUC"),
		UtahCoalPath: os.Getenv("OPENSCRAPER_PATH_UTAHCOAL"),
	}
}

type DokitoBinaryPaths struct {
	ProcessDocketsPath      string
	UploadDocketsPath       string
	DownloadAttachmentsPath string
}

func getDokitoPaths() DokitoBinaryPaths {
	return DokitoBinaryPaths{
		ProcessDocketsPath:      os.Getenv("DOKITO_PROCESS_DOCKETS_BINARY_PATH"),
		UploadDocketsPath:       os.Getenv("DOKITO_UPLOAD_DOCKETS_BINARY_PATH"),
		DownloadAttachmentsPath: os.Getenv("DOKITO_DOWNLOAD_ATTACHMENTS_BINARY_PATH"),
	}
}

type ScraperType string

const (
	NYPUC    ScraperType = "nypuc"
	COPUC    ScraperType = "copuc"
	UtahCoal ScraperType = "utahcoal"
)

type GovIDMapping struct {
	directMappings map[string]ScraperType
	defaultScraper ScraperType
}

func getDefaultGovIDMapping() *GovIDMapping {
	return &GovIDMapping{
		directMappings: map[string]ScraperType{
			"24A-0125E":        COPUC,
			"24A-0168E":        COPUC,
			"24A-0169E":        COPUC,
			"24AL-0024-EL-UNC": COPUC,
			"24AL-0158-EL-UNC": COPUC,
			"24AL-0159-EL-UNC": COPUC,
			"24AL-0160-EL-UNC": COPUC,
			"24AL-0193-EL-UNC": COPUC,
			"24AL-0194-EL-UNC": COPUC,
			"24AL-0195-EL-UNC": COPUC,
			"24AL-0197-EL-UNC": COPUC,
			"24AL-0205-EL-UNC": COPUC,
			"24AL-0206-EL-UNC": COPUC,
			"24AL-0207-EL-UNC": COPUC,
			"24AL-0208-EL-UNC": COPUC,
			"24AL-0209-EL-UNC": COPUC,
			"24AL-0210-EL-UNC": COPUC,
			"24AL-0211-EL-UNC": COPUC,
			"24AL-0212-EL-UNC": COPUC,
		},
		defaultScraper: NYPUC,
	}
}

func (m *GovIDMapping) getScraperForGovID(govID string) ScraperType {
	if scraper, exists := m.directMappings[govID]; exists {
		return scraper
	}
	return m.defaultScraper
}

func executeScraperWithALLMode(govID string, scraperType ScraperType, paths ScraperBinaryPaths) ([]map[string]interface{}, error) {
	var binaryPath string
	switch scraperType {
	case NYPUC:
		binaryPath = paths.NYPUCPath
	case COPUC:
		binaryPath = paths.COPUCPath
	case UtahCoal:
		binaryPath = paths.UtahCoalPath
	default:
		return nil, fmt.Errorf("unknown scraper type: %s", scraperType)
	}

	if binaryPath == "" {
		return nil, fmt.Errorf("binary path not configured for scraper type: %s", scraperType)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, binaryPath, govID, "ALL")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("scraper execution failed: %v", err)
	}

	var results []map[string]interface{}
	if err := json.Unmarshal(output, &results); err != nil {
		return nil, fmt.Errorf("failed to parse scraper output as JSON: %v", err)
	}

	return results, nil
}

func validateJSONAsArrayOfMaps(data []map[string]interface{}) ([]map[string]interface{}, error) {
	if data == nil {
		return nil, fmt.Errorf("data is nil")
	}
	return data, nil
}

func executeDataProcessingBinary(data []map[string]interface{}, paths DokitoBinaryPaths) ([]map[string]interface{}, error) {
	if paths.ProcessDocketsPath == "" {
		return nil, fmt.Errorf("process dockets binary path not configured")
	}

	inputJSON, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal input data: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, paths.ProcessDocketsPath, "process")
	cmd.Stdin = strings.NewReader(string(inputJSON))

	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("data processing failed: %v", err)
	}

	var results []map[string]interface{}
	if err := json.Unmarshal(output, &results); err != nil {
		return nil, fmt.Errorf("failed to parse processing output as JSON: %v", err)
	}

	return results, nil
}

func executeUploadBinary(data []map[string]interface{}, paths DokitoBinaryPaths) error {
	if paths.UploadDocketsPath == "" {
		return fmt.Errorf("upload dockets binary path not configured")
	}

	inputJSON, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal input data: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, paths.UploadDocketsPath, "upload")
	cmd.Stdin = strings.NewReader(string(inputJSON))

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("upload failed: %v", err)
	}

	return nil
}

func main() {
	// Check if we're running in CLI mode
	if len(os.Args) > 1 {
		firstArg := os.Args[1]
		// If first argument is not a number and not -port, assume CLI mode
		if _, err := strconv.Atoi(firstArg); err != nil && firstArg != "-port" {
			runCLI()
			return
		}
	}

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

	// Initialize runner
	runner := &Runner{
		scraperPaths: getScraperPaths(),
		dokitoPaths:  getDokitoPaths(),
	}

	// Create API server
	apiServer := &APIServer{runner: runner}

	// Setup routes
	mux := apiServer.SetupRoutes()

	// Apply middleware
	handler := apiServer.loggingMiddleware(apiServer.corsMiddleware(mux))

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

