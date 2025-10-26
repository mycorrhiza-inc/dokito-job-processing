// Package pipelines Does most of the heavy lifting for pipelines
package pipelines

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runner/internal/core"
	"runner/internal/storage"
	"time"
)

func GetGovidFromJsonData(obj map[string]any) (string, error) {
	idOptions := []string{"docket_govid", "case_govid", "docket_id"}

	for _, key := range idOptions {
		if val, ok := obj[key]; ok {
			if str, ok := val.(string); ok && str != "" {
				return str, nil
			}
		}
	}

	return "", errors.New("error: could not extract govid")
}

func GovidsFromJsonDataFiltered(list []map[string]any) []string {
	results := []string{}
	for _, obj := range list {
		govid, err := GetGovidFromJsonData(obj)
		if err == nil {
			results = append(results, govid)
		}
	}
	return results
}

// IntermediateSource represents the source for intermediate data retrieval
type IntermediateSource string

const (
	IntermediateSourceNone          IntermediateSource = "none"           // No intermediate source, run full scraping
	IntermediateSourceHTML          IntermediateSource = "html"           // Retrieve from HTML snapshots in S3/local storage
	IntermediateSourceRawJSON       IntermediateSource = "raw_json"       // Retrieve from raw JSON objects storage
	IntermediateSourceProcessedJSON IntermediateSource = "processed_json" // Retrieve from processed JSON objects storage
)

// NYPUCPipelineConfig contains configuration options for pipeline execution
type NYPUCPipelineConfig struct {
	DebugMode          bool               // Enable real-time subprocess output streaming
	IntermediateSource IntermediateSource // Source for intermediate data retrieval
}

// NYPUCPipelineResult contains the results from the NY PUC pipeline execution
type NYPUCPipelineResult struct {
	GovID            string
	ScraperType      string
	ScrapeResults    []map[string]any
	ProcessedResults []map[string]any
	ScrapeCount      int
	ProcessCount     int
}

// NYPUCPipelineError represents an error that occurred during pipeline execution
type NYPUCPipelineError struct {
	Step    string
	Message string
	Err     error
}

func (e NYPUCPipelineError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s failed: %s: %v", e.Step, e.Message, e.Err)
	}
	return fmt.Sprintf("%s failed: %s", e.Step, e.Message)
}

// retrieveIntermediateData retrieves data from the specified intermediate source
func retrieveIntermediateData(ctx context.Context, govID string, source IntermediateSource, config NYPUCPipelineConfig) ([]map[string]any, error) {
	switch source {
	case IntermediateSourceNone:
		return nil, fmt.Errorf("IntermediateSourceNone should not be used for data retrieval")

	case IntermediateSourceRawJSON:
		log.Printf("☁️ Retrieving raw JSON data from S3 for %s", govID)
		rawLocation := storage.RawDocketLocation{
			JurisdictionInfo: storage.NypucJurisdictionInfo,
			DocketGovID:      govID,
		}
		var rawData map[string]any
		if err := storage.RetriveJSONFromRemoteAndUpdateLocal(ctx, rawLocation, &rawData); err != nil {
			return nil, fmt.Errorf("failed to retrieve raw JSON data: %v", err)
		}
		return []map[string]any{rawData}, nil

	case IntermediateSourceProcessedJSON:
		log.Printf("☁️ Retrieving processed JSON data from S3 for %s", govID)
		processedLocation := storage.ProcessedDocketLocation{
			JurisdictionInfo: storage.NypucJurisdictionInfo,
			DocketGovID:      govID,
		}
		var processedData map[string]any
		if err := storage.RetriveJSONFromRemoteAndUpdateLocal(ctx, processedLocation, &processedData); err != nil {
			return nil, fmt.Errorf("failed to retrieve processed JSON data: %v", err)
		}
		return []map[string]any{processedData}, nil

	case IntermediateSourceHTML:
		log.Printf("🌐 Running scraper with --source-s3 flag for HTML snapshots for %s", govID)

		// Get scraper type for the govID
		mapping := core.GetDefaultGovIDMapping()
		scraperType := mapping.GetScraperForGovID(govID)

		// Execute scraper with --source-s3 flag to read from HTML snapshots
		var scrapeResults []map[string]any
		var err error

		scrapeResults, err = core.ExecuteScraperWithALLMode(ctx, govID, scraperType, "--source-s3")

		if err != nil {
			return nil, fmt.Errorf("failed to process HTML snapshots with --source-s3: %v", err)
		}
		return scrapeResults, nil

	default:
		return nil, fmt.Errorf("unknown intermediate source: %s", source)
	}
}

// ExecuteNYPUCBasicPipeline runs the complete NY PUC pipeline for a given government ID
// This includes scraping, saving raw data, processing, saving processed data, and uploading
func ExecuteNYPUCBasicPipeline(govID string) (*NYPUCPipelineResult, error) {
	// Create execution context with debug disabled
	ctx := core.WithExecutionConfig(context.Background(), core.NewExecutionConfig())
	return ExecuteNYPUCBasicPipelineWithConfig(ctx, govID, NYPUCPipelineConfig{DebugMode: false})
}

// ExecuteNYPUCBasicPipelineWithConfig runs the NY PUC pipeline with configuration options
func ExecuteNYPUCBasicPipelineWithConfig(ctx context.Context, govID string, config NYPUCPipelineConfig) (*NYPUCPipelineResult, error) {
	pipelineStart := time.Now()
	log.Printf("🚀 Starting NY PUC pipeline for govID: %s", govID)

	// Default to no intermediate source if not specified
	if config.IntermediateSource == "" {
		config.IntermediateSource = IntermediateSourceNone
	}

	// Initialize mapping and determine scraper type
	mapping := core.GetDefaultGovIDMapping()
	scraperType := mapping.GetScraperForGovID(govID)

	log.Printf("📋 Using scraper type: %s, intermediate source: %s", scraperType, config.IntermediateSource)

	result := &NYPUCPipelineResult{
		GovID:       govID,
		ScraperType: string(scraperType),
	}

	// Step 1: Get raw data based on intermediate source
	var scrapeResults []map[string]any
	var err error

	// Handle different intermediate sources
	if config.IntermediateSource != IntermediateSourceNone {
		step1Start := time.Now()
		log.Printf("📦 Step 1/4: Retrieving data from intermediate source: %s", config.IntermediateSource)

		scrapeResults, err = retrieveIntermediateData(ctx, govID, config.IntermediateSource, config)
		if err != nil {
			return result, NYPUCPipelineError{
				Step:    "Intermediate Data Retrieval",
				Message: fmt.Sprintf("failed to retrieve data from %s", config.IntermediateSource),
				Err:     err,
			}
		}

		step1Duration := time.Since(step1Start)
		log.Printf("✅ Retrieved %d results from %s (took %v)", len(scrapeResults), config.IntermediateSource, step1Duration)
	} else {
		step1Start := time.Now()
		log.Printf("📝 Step 1/4: Running scraper for %s", govID)

		scrapeResults, err = core.ExecuteScraperWithALLMode(ctx, govID, scraperType)

		if err != nil {
			return result, NYPUCPipelineError{
				Step:    "Scraping",
				Message: "scraper execution failed",
				Err:     err,
			}
		}

		step1Duration := time.Since(step1Start)
		log.Printf("✅ Scraper completed. Found %d results (took %v)", len(scrapeResults), step1Duration)

		// Step 1.5: Save raw data to both local and remote locations (for fresh scraping or HTML processing)
		if config.IntermediateSource == IntermediateSourceNone || config.IntermediateSource == IntermediateSourceHTML {
			step1_5Start := time.Now()
			log.Printf("💾 Step 1.5/4: Saving raw scraped data to local and remote storage")
			for _, scrapeResult := range scrapeResults {
				scrapeGovid, err := storage.TryAndExtractGovid(scrapeResult)
				if err != nil {
					return result, NYPUCPipelineError{
						Step:    "Raw Data Storage",
						Message: "Data did not have an associated govid",
						Err:     err,
					}
				}
				rawLocation := storage.RawDocketLocation{
					JurisdictionInfo: storage.NypucJurisdictionInfo,
					DocketGovID:      scrapeGovid,
				}
				if err := storage.WriteJSONToLocalAndRemote(ctx, rawLocation, scrapeResult); err != nil {
					return result, NYPUCPipelineError{
						Step:    "Raw Data Storage",
						Message: "failed to save raw data to local and remote storage",
						Err:     err,
					}
				}

				log.Printf("✅ Raw data saved to both local and remote storage")
			}
			step1_5Duration := time.Since(step1_5Start)
			log.Printf("✅ Raw data storage completed (took %v)", step1_5Duration)
		}
	}

	result.ScrapeResults = scrapeResults
	result.ScrapeCount = len(scrapeResults)

	// Step 2: Validate and process data (skip if we already have processed data)
	var processedResults []map[string]any
	if config.IntermediateSource == IntermediateSourceProcessedJSON {
		// Data is already processed, use it directly
		log.Printf("⏭️  Step 2/4: Skipping processing - using processed data from intermediate source")
		processedResults = scrapeResults
		result.ProcessedResults = processedResults
		result.ProcessCount = len(processedResults)
	} else {
		// Need to process the data
		step2Start := time.Now()
		log.Printf("🔧 Step 2/4: Processing scraped data")
		validatedData, err := core.ValidateJSONAsArrayOfMaps(scrapeResults)
		if err != nil {
			return result, NYPUCPipelineError{
				Step:    "Data Validation",
				Message: "data validation failed",
				Err:     err,
			}
		}

		processedResults, err = core.ExecuteDataProcessingBinary(ctx, validatedData)

		if err != nil {
			return result, NYPUCPipelineError{
				Step:    "Data Processing",
				Message: "data processing failed",
				Err:     err,
			}
		}

		result.ProcessedResults = processedResults
		result.ProcessCount = len(processedResults)
		step2Duration := time.Since(step2Start)
		log.Printf("✅ Processing completed. Processed %d results (took %v)", len(processedResults), step2Duration)
	}

	// Step 2.5: Save processed data to both local and remote locations (skip if source is processed_json)
	if config.IntermediateSource != IntermediateSourceProcessedJSON {
		step2_5Start := time.Now()
		log.Printf("💾 Step 2.5/4: Saving processed data to local and remote storage")

		for _, processedResult := range processedResults {
			processedGovid, err := storage.TryAndExtractGovid(processedResult)
			if err != nil {
				return result, NYPUCPipelineError{
					Step:    "Processed Data Storage",
					Message: "Data did not have an associated govid",
					Err:     err,
				}
			}
			processedLocation := storage.ProcessedDocketLocation{
				JurisdictionInfo: storage.NypucJurisdictionInfo,
				DocketGovID:      processedGovid,
			}

			if err := storage.WriteJSONToLocalAndRemote(ctx, processedLocation, processedResult); err != nil {
				return result, NYPUCPipelineError{
					Step:    "Processed Data Storage",
					Message: "failed to save processed data to local and remote storage",
					Err:     err,
				}
			}

			log.Printf("✅ Processed data saved to both local and remote storage")
		}
		step2_5Duration := time.Since(step2_5Start)
		log.Printf("✅ Processed data storage completed (took %v)", step2_5Duration)
	} else {
		log.Printf("⏭️  Step 2.5/4: Skipping processed data storage - source is already processed_json")
	}

	// Step 3: Upload results
	step3Start := time.Now()
	log.Printf("📤 Step 3/4: Uploading processed data")
	err = core.ExecuteUploadBinary(ctx, processedResults)

	if err != nil {
		return result, NYPUCPipelineError{
			Step:    "Upload",
			Message: "upload failed",
			Err:     err,
		}
	}

	step3Duration := time.Since(step3Start)
	totalPipelineDuration := time.Since(pipelineStart)
	log.Printf("✅ Upload completed (took %v)", step3Duration)

	log.Printf("🎉 NY PUC pipeline completed successfully for %s using %s source. Scraped %d items, processed %d items. Total time: %v",
		govID, config.IntermediateSource, result.ScrapeCount, result.ProcessCount, totalPipelineDuration)

	return result, nil
}
