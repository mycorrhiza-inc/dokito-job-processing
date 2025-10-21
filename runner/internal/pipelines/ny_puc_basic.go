// Package pipelines Does most of the heavy lifting for pipelines
package pipelines

import (
	"context"
	"fmt"
	"log"
	"runner/internal/core"
	"runner/internal/storage"
)

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

// ExecuteNYPUCBasicPipeline runs the complete NY PUC pipeline for a given government ID
// This includes scraping, saving raw data, processing, saving processed data, and uploading
func ExecuteNYPUCBasicPipeline(govID string) (*NYPUCPipelineResult, error) {
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

	// Step 1: Execute scraper in ALL mode
	log.Printf("📝 Step 1/4: Running scraper for %s", govID)
	scrapeResults, err := core.ExecuteScraperWithALLMode(govID, scraperType, scraperPaths)
	if err != nil {
		return result, NYPUCPipelineError{
			Step:    "Scraping",
			Message: "scraper execution failed",
			Err:     err,
		}
	}

	result.ScrapeResults = scrapeResults
	result.ScrapeCount = len(scrapeResults)
	log.Printf("✅ Scraper completed. Found %d results", len(scrapeResults))

	// Step 1.5: Save raw data to both local and remote locations
	log.Printf("💾 Step 1.5/4: Saving raw scraped data to local and remote storage")
	rawLocation := storage.RawDocketLocation{
		JurisdictionInfo: storage.NypucJurisdictionInfo,
		DocketGovID:      govID,
	}

	ctx := context.Background()
	if err := storage.WriteJSONToLocalAndRemote(ctx, rawLocation, scrapeResults); err != nil {
		return result, NYPUCPipelineError{
			Step:    "Raw Data Storage",
			Message: "failed to save raw data to local and remote storage",
			Err:     err,
		}
	}

	log.Printf("✅ Raw data saved to both local and remote storage")

	// Step 2: Validate and process data
	log.Printf("🔧 Step 2/4: Processing scraped data")
	validatedData, err := core.ValidateJSONAsArrayOfMaps(scrapeResults)
	if err != nil {
		return result, NYPUCPipelineError{
			Step:    "Data Validation",
			Message: "data validation failed",
			Err:     err,
		}
	}

	processedResults, err := core.ExecuteDataProcessingBinary(validatedData, dokitoPaths)
	if err != nil {
		return result, NYPUCPipelineError{
			Step:    "Data Processing",
			Message: "data processing failed",
			Err:     err,
		}
	}

	result.ProcessedResults = processedResults
	result.ProcessCount = len(processedResults)
	log.Printf("✅ Processing completed. Processed %d results", len(processedResults))

	// Step 2.5: Save processed data to both local and remote locations
	log.Printf("💾 Step 2.5/4: Saving processed data to local and remote storage")
	processedLocation := storage.ProcessedDocketLocation{
		JurisdictionInfo: storage.NypucJurisdictionInfo,
		DocketGovID:      govID,
	}

	if err := storage.WriteJSONToLocalAndRemote(ctx, processedLocation, processedResults); err != nil {
		return result, NYPUCPipelineError{
			Step:    "Processed Data Storage",
			Message: "failed to save processed data to local and remote storage",
			Err:     err,
		}
	}

	log.Printf("✅ Processed data saved to both local and remote storage")

	// Step 3: Upload results
	log.Printf("📤 Step 3/4: Uploading processed data")
	if err := core.ExecuteUploadBinary(processedResults, dokitoPaths); err != nil {
		return result, NYPUCPipelineError{
			Step:    "Upload",
			Message: "upload failed",
			Err:     err,
		}
	}

	log.Printf("🎉 NY PUC pipeline completed successfully for %s. Scraped %d items, processed %d items.",
		govID, result.ScrapeCount, result.ProcessCount)

	return result, nil
}
