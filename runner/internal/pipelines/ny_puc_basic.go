// Package pipelines Does most of the heavy lifting for pipelines
package pipelines

import (
	"context"
	"fmt"
	"log"
	"path"
	"runner/internal/core"
	"runner/internal/storage"
	"time"
)

// IntermediateSource represents the source for intermediate data retrieval
type IntermediateSource string

const (
	IntermediateSourceNone         IntermediateSource = "none"         // No intermediate source, run full scraping
	IntermediateSourceHTML         IntermediateSource = "html"         // Retrieve from HTML snapshots in S3/local storage
	IntermediateSourceRawJSON      IntermediateSource = "raw_json"     // Retrieve from raw JSON objects storage
	IntermediateSourceProcessedJSON IntermediateSource = "processed_json" // Retrieve from processed JSON objects storage
)

// NYPUCPipelineConfig contains configuration options for pipeline execution
type NYPUCPipelineConfig struct {
	DebugMode          bool               // Enable real-time subprocess output streaming
	FromRemote         bool               // Skip scraping and retrieve data from S3 instead (deprecated, use IntermediateSource)
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

		// Get binary paths and scraper type
		scraperPaths := core.GetScraperPaths()
		mapping := core.GetDefaultGovIDMapping()
		scraperType := mapping.GetScraperForGovID(govID)

		// Execute scraper with --source-s3 flag to read from HTML snapshots
		var scrapeResults []map[string]any
		var err error

		if config.DebugMode {
			scrapeResults, err = core.ExecuteScraperWithS3SourceDebug(govID, scraperType, scraperPaths)
		} else {
			scrapeResults, err = core.ExecuteScraperWithS3Source(govID, scraperType, scraperPaths)
		}

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
	return ExecuteNYPUCBasicPipelineWithConfig(govID, NYPUCPipelineConfig{DebugMode: false})
}

// ExecuteNYPUCBasicPipelineWithConfig runs the NY PUC pipeline with configuration options
func ExecuteNYPUCBasicPipelineWithConfig(govID string, config NYPUCPipelineConfig) (*NYPUCPipelineResult, error) {
	pipelineStart := time.Now()
	log.Printf("🚀 Starting NY PUC pipeline for govID: %s", govID)

	// Get binary paths
	scraperPaths := core.GetScraperPaths()
	dokitoPaths := core.GetDokitoPaths()

	// Initialize mapping and determine scraper type
	mapping := core.GetDefaultGovIDMapping()
	scraperType := mapping.GetScraperForGovID(govID)

	log.Printf("📋 Using scraper type: %s", scraperType)

	result := &NYPUCPipelineResult{
		GovID:       govID,
		ScraperType: string(scraperType),
	}

	// Step 1: Get raw data (either scrape or retrieve from remote)
	var scrapeResults []map[string]any
	var err error
	ctx := context.Background()

	if config.FromRemote {
		step1Start := time.Now()
		log.Printf("☁️ Step 1/4: Retrieving raw data from remote storage for %s", govID)
		rawLocation := storage.RawDocketLocation{
			JurisdictionInfo: storage.NypucJurisdictionInfo,
			DocketGovID:      govID,
		}

		var rawData map[string]any
		if err := storage.RetriveJSONFromRemoteAndUpdateLocal(ctx, rawLocation, &rawData); err != nil {
			return result, NYPUCPipelineError{
				Step:    "Remote Data Retrieval",
				Message: "failed to retrieve raw data from remote storage",
				Err:     err,
			}
		}

		scrapeResults = []map[string]any{rawData}

		step1Duration := time.Since(step1Start)
		log.Printf("✅ Retrieved %d results from remote storage (took %v)", len(scrapeResults), step1Duration)
	} else {
		step1Start := time.Now()
		log.Printf("📝 Step 1/4: Running scraper for %s", govID)

		if config.DebugMode {
			log.Printf("🐛 Debug mode enabled - streaming subprocess output")
			scrapeResults, err = core.ExecuteScraperWithALLModeDebug(govID, scraperType, scraperPaths)
		} else {
			scrapeResults, err = core.ExecuteScraperWithALLMode(govID, scraperType, scraperPaths)
		}

		if err != nil {
			return result, NYPUCPipelineError{
				Step:    "Scraping",
				Message: "scraper execution failed",
				Err:     err,
			}
		}

		step1Duration := time.Since(step1Start)
		log.Printf("✅ Scraper completed. Found %d results (took %v)", len(scrapeResults), step1Duration)

		// Step 1.5: Save raw data to both local and remote locations
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

	result.ScrapeResults = scrapeResults
	result.ScrapeCount = len(scrapeResults)

	// Step 2: Validate and process data
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

	var processedResults []map[string]any
	if config.DebugMode {
		processedResults, err = core.ExecuteDataProcessingBinaryDebug(validatedData, dokitoPaths)
	} else {
		processedResults, err = core.ExecuteDataProcessingBinary(validatedData, dokitoPaths)
	}

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

	// Step 2.5: Save processed data to both local and remote locations
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

	// Step 3: Upload results
	step3Start := time.Now()
	log.Printf("📤 Step 3/4: Uploading processed data")
	if config.DebugMode {
		err = core.ExecuteUploadBinaryDebug(processedResults, dokitoPaths)
	} else {
		err = core.ExecuteUploadBinary(processedResults, dokitoPaths)
	}

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

	log.Printf("🎉 NY PUC pipeline completed successfully for %s. Scraped %d items, processed %d items. Total time: %v",
		govID, result.ScrapeCount, result.ProcessCount, totalPipelineDuration)

	return result, nil
}
