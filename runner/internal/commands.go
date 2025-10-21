package internal

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"runner/internal/core"
)

func RunCLI() {
	if len(os.Args) < 2 {
		PrintUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "pipeline":
		RunPipeline()
	case "scrape":
		RunScrapeOnly()
	case "process":
		RunProcessOnly()
	case "upload":
		RunUploadOnly()
	case "env":
		ShowEnvironment()
	case "help", "-h", "--help":
		PrintUsage()
	default:
		fmt.Printf("Unknown command: %s\n", command)
		PrintUsage()
		os.Exit(1)
	}
}

func PrintUsage() {
	fmt.Println("Dokito CLI - Debug tool for the job processing pipeline")
	fmt.Println("")
	fmt.Println("Usage:")
	fmt.Println("  dokito-cli pipeline <gov_id>    - Run the full pipeline")
	fmt.Println("  dokito-cli scrape <gov_id>      - Run scraper only and print results")
	fmt.Println("  dokito-cli process <json_file>  - Run processing only on JSON file")
	fmt.Println("  dokito-cli upload <json_file>   - Run upload only on JSON file")
	fmt.Println("  dokito-cli env                  - Show environment configuration")
	fmt.Println("  dokito-cli help                 - Show this help message")
	fmt.Println("")
	fmt.Println("Examples:")
	fmt.Println("  dokito-cli pipeline 00-F-0229")
	fmt.Println("  dokito-cli scrape 00-F-0229")
	fmt.Println("  dokito-cli env")
}

func RunPipeline() {
	if len(os.Args) < 3 {
		fmt.Println("Error: gov_id required")
		fmt.Println("Usage: dokito-cli pipeline <gov_id>")
		os.Exit(1)
	}

	govID := strings.TrimSpace(os.Args[2])
	log.Printf("🚀 Starting full pipeline for govID: %s", govID)

	// Get binary paths
	scraperPaths := core.GetScraperPaths()
	dokitoPaths := core.GetDokitoPaths()

	// Initialize mapping and determine scraper type
	mapping := core.GetDefaultGovIDMapping()
	scraperType := mapping.GetScraperForGovID(govID)

	log.Printf("📋 Using scraper type: %s", scraperType)

	// Step 1: Execute scraper in ALL mode
	log.Printf("📝 Step 1/3: Running scraper for %s", govID)
	scrapeResults, err := core.ExecuteScraperWithALLMode(govID, scraperType, scraperPaths)
	if err != nil {
		log.Printf("❌ Scraper execution failed: %v", err)
		os.Exit(1)
	}

	log.Printf("✅ Scraper completed. Found %d results", len(scrapeResults))

	// Step 2: Validate and process data
	log.Printf("🔧 Step 2/3: Processing scraped data")
	validatedData, err := core.ValidateJSONAsArrayOfMaps(scrapeResults)
	if err != nil {
		log.Printf("❌ Data validation failed: %v", err)
		os.Exit(1)
	}

	processedResults, err := core.ExecuteDataProcessingBinary(validatedData, dokitoPaths)
	if err != nil {
		log.Printf("❌ Data processing failed: %v", err)
		os.Exit(1)
	}

	log.Printf("✅ Processing completed. Processed %d results", len(processedResults))

	// Step 3: Upload results
	log.Printf("📤 Step 3/3: Uploading processed data")
	if err := core.ExecuteUploadBinary(processedResults, dokitoPaths); err != nil {
		log.Printf("❌ Upload failed: %v", err)
		os.Exit(1)
	}

	log.Printf("🎉 Full pipeline completed successfully for %s. Scraped %d items, processed %d items.",
		govID, len(scrapeResults), len(processedResults))
}

func RunScrapeOnly() {
	if len(os.Args) < 3 {
		fmt.Println("Error: gov_id required")
		fmt.Println("Usage: dokito-cli scrape <gov_id>")
		os.Exit(1)
	}

	govID := strings.TrimSpace(os.Args[2])
	log.Printf("🔍 Running scraper only for govID: %s", govID)

	// Get binary paths
	scraperPaths := core.GetScraperPaths()

	// Initialize mapping and determine scraper type
	mapping := core.GetDefaultGovIDMapping()
	scraperType := mapping.GetScraperForGovID(govID)

	log.Printf("📋 Using scraper type: %s", scraperType)

	// Execute scraper
	results, err := core.ExecuteScraperWithALLMode(govID, scraperType, scraperPaths)
	if err != nil {
		log.Printf("❌ Scraper execution failed: %v", err)
		os.Exit(1)
	}

	log.Printf("✅ Scraper completed. Found %d results", len(results))

	// Pretty print results
	output, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		log.Printf("❌ Failed to marshal results: %v", err)
		os.Exit(1)
	}

	fmt.Println(string(output))
}

func RunProcessOnly() {
	if len(os.Args) < 3 {
		fmt.Println("Error: json_file required")
		fmt.Println("Usage: dokito-cli process <json_file>")
		os.Exit(1)
	}

	jsonFile := os.Args[2]
	log.Printf("🔧 Processing JSON file: %s", jsonFile)

	// Read JSON file
	data, err := os.ReadFile(jsonFile)
	if err != nil {
		log.Printf("❌ Failed to read file: %v", err)
		os.Exit(1)
	}

	var inputData []map[string]any
	if err := json.Unmarshal(data, &inputData); err != nil {
		log.Printf("❌ Failed to parse JSON: %v", err)
		os.Exit(1)
	}

	// Get dokito paths
	dokitoPaths := core.GetDokitoPaths()

	// Process data
	results, err := core.ExecuteDataProcessingBinary(inputData, dokitoPaths)
	if err != nil {
		log.Printf("❌ Data processing failed: %v", err)
		os.Exit(1)
	}

	log.Printf("✅ Processing completed. Processed %d results", len(results))

	// Pretty print results
	output, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		log.Printf("❌ Failed to marshal results: %v", err)
		os.Exit(1)
	}

	fmt.Println(string(output))
}

func RunUploadOnly() {
	if len(os.Args) < 3 {
		fmt.Println("Error: json_file required")
		fmt.Println("Usage: dokito-cli upload <json_file>")
		os.Exit(1)
	}

	jsonFile := os.Args[2]
	log.Printf("📤 Uploading JSON file: %s", jsonFile)

	// Read JSON file
	data, err := os.ReadFile(jsonFile)
	if err != nil {
		log.Printf("❌ Failed to read file: %v", err)
		os.Exit(1)
	}

	var inputData []map[string]any
	if err := json.Unmarshal(data, &inputData); err != nil {
		log.Printf("❌ Failed to parse JSON: %v", err)
		os.Exit(1)
	}

	// Get dokito paths
	dokitoPaths := core.GetDokitoPaths()

	// Upload data
	if err := core.ExecuteUploadBinary(inputData, dokitoPaths); err != nil {
		log.Printf("❌ Upload failed: %v", err)
		os.Exit(1)
	}

	log.Printf("✅ Upload completed successfully")
}

func ShowEnvironment() {
	fmt.Println("🔧 Environment Configuration:")
	fmt.Println("")

	scraperPaths := core.GetScraperPaths()
	dokitoPaths := core.GetDokitoPaths()

	fmt.Println("Scraper Binaries:")
	fmt.Printf("  NYPUC:    %s\n", getStatus(scraperPaths.NYPUCPath))
	fmt.Printf("  COPUC:    %s\n", getStatus(scraperPaths.COPUCPath))
	fmt.Printf("  UtahCoal: %s\n", getStatus(scraperPaths.UtahCoalPath))

	fmt.Println("")
	fmt.Println("Dokito Binaries:")
	fmt.Printf("  Process:  %s\n", getStatus(dokitoPaths.ProcessDocketsPath))
	fmt.Printf("  Upload:   %s\n", getStatus(dokitoPaths.UploadDocketsPath))
	fmt.Printf("  Download: %s\n", getStatus(dokitoPaths.DownloadAttachmentsPath))

	fmt.Println("")
	fmt.Println("Environment Variables:")
	fmt.Printf("  OPENSCRAPER_PATH_NYPUC: %s\n", os.Getenv("OPENSCRAPER_PATH_NYPUC"))
	fmt.Printf("  OPENSCRAPER_PATH_COPUC: %s\n", os.Getenv("OPENSCRAPER_PATH_COPUC"))
	fmt.Printf("  OPENSCRAPER_PATH_UTAHCOAL: %s\n", os.Getenv("OPENSCRAPER_PATH_UTAHCOAL"))
	fmt.Printf("  DOKITO_PROCESS_DOCKETS_BINARY_PATH: %s\n", os.Getenv("DOKITO_PROCESS_DOCKETS_BINARY_PATH"))
	fmt.Printf("  DOKITO_UPLOAD_DOCKETS_BINARY_PATH: %s\n", os.Getenv("DOKITO_UPLOAD_DOCKETS_BINARY_PATH"))
	fmt.Printf("  DOKITO_DOWNLOAD_ATTACHMENTS_BINARY_PATH: %s\n", os.Getenv("DOKITO_DOWNLOAD_ATTACHMENTS_BINARY_PATH"))
}

func getStatus(path string) string {
	if path == "" {
		return "❌ not configured"
	}

	if _, err := os.Stat(path); err != nil {
		return fmt.Sprintf("❌ configured but missing (%s)", path)
	}

	return fmt.Sprintf("✅ %s", path)
}