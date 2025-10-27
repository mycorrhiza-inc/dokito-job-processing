// Package pipelines - Attachment download and reprocessing pipeline
package pipelines

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"runner/internal/core"
	"runner/internal/storage"
	"time"
)

// AttachmentReprocessingResult contains the results from the attachment reprocessing pipeline
type AttachmentReprocessingResult struct {
	GovID            string
	DataChanged      bool
	OriginalData     []map[string]any
	ProcessedData    []map[string]any
	UploadPerformed  bool
}

// AttachmentReprocessingError represents an error that occurred during attachment reprocessing
type AttachmentReprocessingError struct {
	Step    string
	Message string
	Err     error
}

func (e AttachmentReprocessingError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s failed: %s: %v", e.Step, e.Message, e.Err)
	}
	return fmt.Sprintf("%s failed: %s", e.Step, e.Message)
}

// ExecuteAttachmentReprocessingPipeline reprocesses attachments for a docket that already has processed JSON
// This pipeline:
// 1. Fetches processed JSON from S3
// 2. Runs it through the attachment downloader
// 3. Compares original vs processed data
// 4. Uploads to S3 only if data changed
func ExecuteAttachmentReprocessingPipeline(govID string) (*AttachmentReprocessingResult, error) {
	ctx := core.WithExecutionConfig(context.Background(), core.NewExecutionConfig())
	return ExecuteAttachmentReprocessingPipelineWithConfig(ctx, govID)
}

// ExecuteAttachmentReprocessingPipelineWithConfig runs the attachment reprocessing pipeline with configuration
func ExecuteAttachmentReprocessingPipelineWithConfig(ctx context.Context, govID string) (*AttachmentReprocessingResult, error) {
	pipelineStart := time.Now()
	log.Printf("🔄 Starting attachment reprocessing pipeline for govID: %s", govID)

	result := &AttachmentReprocessingResult{
		GovID:           govID,
		DataChanged:     false,
		UploadPerformed: false,
	}

	// Step 1: Fetch processed JSON from S3
	step1Start := time.Now()
	log.Printf("📦 Step 1/4: Fetching processed JSON from S3 for %s", govID)

	processedLocation := storage.ProcessedDocketLocation{
		JurisdictionInfo: storage.NypucJurisdictionInfo,
		DocketGovID:      govID,
	}

	var originalData map[string]any
	if err := storage.RetriveJSONFromRemoteAndUpdateLocal(ctx, processedLocation, &originalData); err != nil {
		return result, AttachmentReprocessingError{
			Step:    "Fetch Processed JSON",
			Message: fmt.Sprintf("failed to retrieve processed JSON for govID %s", govID),
			Err:     err,
		}
	}

	// Convert to array format expected by the pipeline
	originalDataArray := []map[string]any{originalData}
	result.OriginalData = originalDataArray

	step1Duration := time.Since(step1Start)
	log.Printf("✅ Retrieved processed JSON from S3 (took %v)", step1Duration)

	// Step 2: Run through attachment downloader
	step2Start := time.Now()
	log.Printf("📥 Step 2/4: Running attachment downloader")

	processedDataArray, err := core.ExecuteDownloadAttachmentsBinary(ctx, originalDataArray)
	if err != nil {
		return result, AttachmentReprocessingError{
			Step:    "Download Attachments",
			Message: "attachment download failed",
			Err:     err,
		}
	}

	result.ProcessedData = processedDataArray
	step2Duration := time.Since(step2Start)
	log.Printf("✅ Attachment download completed (took %v)", step2Duration)

	// Step 3: Compare original vs processed data
	step3Start := time.Now()
	log.Printf("🔍 Step 3/4: Comparing original vs processed data")

	dataChanged := !reflect.DeepEqual(originalDataArray, processedDataArray)
	result.DataChanged = dataChanged

	step3Duration := time.Since(step3Start)
	if dataChanged {
		log.Printf("✅ Data comparison completed - CHANGES DETECTED (took %v)", step3Duration)
	} else {
		log.Printf("✅ Data comparison completed - NO CHANGES (took %v)", step3Duration)
	}

	// Step 4: Upload to S3 only if data changed
	if dataChanged {
		step4Start := time.Now()
		log.Printf("📤 Step 4/4: Uploading changed data to S3")

		// Save processed data to both local and remote locations
		for _, processedResult := range processedDataArray {
			processedGovid, err := storage.TryAndExtractGovid(processedResult)
			if err != nil {
				return result, AttachmentReprocessingError{
					Step:    "Data Upload",
					Message: "processed data did not have an associated govid",
					Err:     err,
				}
			}

			processedLocation := storage.ProcessedDocketLocation{
				JurisdictionInfo: storage.NypucJurisdictionInfo,
				DocketGovID:      processedGovid,
			}

			if err := storage.WriteJSONToLocalAndRemote(ctx, processedLocation, processedResult); err != nil {
				return result, AttachmentReprocessingError{
					Step:    "Data Upload",
					Message: "failed to save processed data to local and remote storage",
					Err:     err,
				}
			}

			log.Printf("✅ Updated data saved to both local and remote storage")
		}

		result.UploadPerformed = true
		step4Duration := time.Since(step4Start)
		log.Printf("✅ Upload completed (took %v)", step4Duration)
	} else {
		log.Printf("⏭️  Step 4/4: Skipping upload - no changes detected")
	}

	totalPipelineDuration := time.Since(pipelineStart)
	log.Printf("🎉 Attachment reprocessing pipeline completed for %s. Data changed: %t, Upload performed: %t. Total time: %v",
		govID, result.DataChanged, result.UploadPerformed, totalPipelineDuration)

	return result, nil
}

// ExecuteAttachmentReprocessingPipelineWithDebug runs the attachment reprocessing pipeline with debug output
func ExecuteAttachmentReprocessingPipelineWithDebug(govID string) (*AttachmentReprocessingResult, error) {
	ctx := core.WithExecutionConfig(context.Background(), core.NewExecutionConfigWithDebug())
	return ExecuteAttachmentReprocessingPipelineWithConfig(ctx, govID)
}

// PrintDataDifferences prints detailed information about what changed between original and processed data
func PrintDataDifferences(original, processed []map[string]any) {
	if len(original) != len(processed) {
		log.Printf("📊 Data length changed: %d → %d", len(original), len(processed))
		return
	}

	for i := 0; i < len(original); i++ {
		originalJSON, _ := json.MarshalIndent(original[i], "", "  ")
		processedJSON, _ := json.MarshalIndent(processed[i], "", "  ")

		if string(originalJSON) != string(processedJSON) {
			log.Printf("📊 Data changes detected in item %d:", i)
			log.Printf("   Original size: %d bytes", len(originalJSON))
			log.Printf("   Processed size: %d bytes", len(processedJSON))

			// For detailed debugging, you could add field-by-field comparison here
			// This is left simple to avoid too much output
		}
	}
}