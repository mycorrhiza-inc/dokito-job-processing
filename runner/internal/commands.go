// Package internal Takes care of combined functionality needed for both binaries
package internal

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runner/internal/core"
	"runner/internal/pipelines"
	"strings"
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
	fmt.Println("  dokito-cli pipeline [--intermediate-source=<source>] <gov_id>  - Run the full pipeline")
	fmt.Println("  dokito-cli scrape <gov_id>                                     - Run scraper only and print results")
	fmt.Println("  dokito-cli process <json_file>                                 - Run processing only on JSON file")
	fmt.Println("  dokito-cli upload <json_file>                                  - Run upload only on JSON file")
	fmt.Println("  dokito-cli env                                                 - Show environment configuration")
	fmt.Println("")
	fmt.Println("Intermediate Source Options:")
	fmt.Println("  --intermediate-source=none           - Run full scraping (default)")
	fmt.Println("  --intermediate-source=html           - Process from HTML snapshots in S3")
	fmt.Println("  --intermediate-source=raw_json       - Retrieve from raw JSON objects in S3")
	fmt.Println("  --intermediate-source=processed_json - Retrieve from processed JSON objects in S3")
	fmt.Println("")
	fmt.Println("Examples:")
	fmt.Println("  dokito-cli pipeline 00-F-0229")
	fmt.Println("  dokito-cli pipeline 00-F-0229 --intermediate-source=html")
	fmt.Println("  dokito-cli pipeline --intermediate-source=html 00-F-0229")
	fmt.Println("  dokito-cli pipeline --intermediate-source=raw_json 00-F-0229")
	fmt.Println("  dokito-cli pipeline --intermediate-source=processed_json 00-F-0229")
	fmt.Println("  dokito-cli scrape 00-F-0229")
	fmt.Println("  dokito-cli env")
}

// parseIntermediateSource converts a string to IntermediateSource enum
func parseIntermediateSource(source string) (pipelines.IntermediateSource, error) {
	switch strings.ToLower(source) {
	case "none", "":
		return pipelines.IntermediateSourceNone, nil
	case "html":
		return pipelines.IntermediateSourceHTML, nil
	case "raw_json", "raw-json":
		return pipelines.IntermediateSourceRawJSON, nil
	case "processed_json", "processed-json":
		return pipelines.IntermediateSourceProcessedJSON, nil
	default:
		return "", fmt.Errorf("invalid intermediate source: %s. Valid options: none, html, raw_json, processed_json", source)
	}
}

// parseCommandLineArgs parses command line arguments for the pipeline command
func parseCommandLineArgs(args []string) (govID string, config pipelines.NYPUCPipelineConfig, err error) {
	config = pipelines.NYPUCPipelineConfig{
		DebugMode:          true, // Enable debug mode for CLI usage
		IntermediateSource: pipelines.IntermediateSourceNone, // Default to no intermediate source
	}

	var foundGovID bool

	// Parse all arguments, looking for flags and gov_id
	for i := 0; i < len(args); i++ {
		arg := args[i]

		if strings.HasPrefix(arg, "--intermediate-source=") {
			sourceStr := strings.TrimPrefix(arg, "--intermediate-source=")
			source, parseErr := parseIntermediateSource(sourceStr)
			if parseErr != nil {
				return "", config, parseErr
			}
			config.IntermediateSource = source
		} else if !strings.HasPrefix(arg, "--") {
			// This is not a flag, assume it's the gov_id
			if foundGovID {
				return "", config, fmt.Errorf("multiple gov_ids provided: %s and %s", govID, arg)
			}
			govID = strings.TrimSpace(arg)
			foundGovID = true
		} else {
			return "", config, fmt.Errorf("unknown argument: %s", arg)
		}
	}

	if !foundGovID {
		return "", config, fmt.Errorf("gov_id required")
	}

	return govID, config, nil
}

func RunPipeline() {
	if len(os.Args) < 3 {
		fmt.Println("Error: gov_id required")
		fmt.Println("Usage: dokito-cli pipeline [--intermediate-source=<source>] <gov_id>")
		PrintUsage()
		os.Exit(1)
	}

	// Parse command line arguments
	govID, config, err := parseCommandLineArgs(os.Args[2:])
	if err != nil {
		fmt.Printf("❌ Error parsing arguments: %v\n", err)
		fmt.Println("")
		PrintUsage()
		os.Exit(1)
	}

	// Show configuration for transparency
	fmt.Printf("🔧 Pipeline Configuration:\n")
	fmt.Printf("  Gov ID: %s\n", govID)
	fmt.Printf("  Intermediate Source: %s\n", config.IntermediateSource)
	fmt.Printf("  Debug Mode: %t\n", config.DebugMode)
	fmt.Println("")

	// Execute the shared NY PUC pipeline
	result, err := pipelines.ExecuteNYPUCBasicPipelineWithConfig(govID, config)
	if err != nil {
		log.Printf("❌ Pipeline failed: %v", err)
		os.Exit(1)
	}

	log.Printf("🎉 Full pipeline completed successfully for %s using %s source. Scraped %d items, processed %d items.",
		result.GovID, config.IntermediateSource, result.ScrapeCount, result.ProcessCount)
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

