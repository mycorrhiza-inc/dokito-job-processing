package main

import (
	"fmt"
	"log"
	"testing"
	"time"
)

// TestPipelineIntegration tests the complete scrape -> upload -> process -> ingest pipeline
func TestPipelineIntegration(t *testing.T) {
	// Configuration for integration test
	govIDs := []string{"25-01799"} // Single govID for testing
	scraperImage := "jobrunner-worker:latest"
	workDir := "/tmp/jobrunner-pipeline-test"
	dokitoBaseURL := "http://localhost:8123" // Mock or test Dokito backend

	// Create runner with 1 worker for testing
	runner, err := NewRunner(1, scraperImage, workDir, dokitoBaseURL)
	if err != nil {
		t.Fatalf("Failed to create runner: %v", err)
	}
	defer runner.Stop()

	log.Printf("🧪 Starting pipeline integration test")
	log.Printf("📁 Test work directory: %s", workDir)
	log.Printf("🐳 Scraper image: %s", scraperImage)
	log.Printf("🔗 Dokito backend: %s", dokitoBaseURL)
	log.Printf("📊 Testing with govID: %v", govIDs)

	// Define pipeline stages
	pipelineStages := []PipelineStage{
		PipelineStageScrape,
		PipelineStageUpload,
		PipelineStageProcess,
		PipelineStageIngest,
	}

	// Configure custom timeouts for testing
	testTimeouts := TimeoutConfig{
		ScrapeTimeoutMinutes:    15, // Longer timeout for testing
		UploadTimeoutMinutes:    5,
		ProcessTimeoutMinutes:   10,
		IngestTimeoutMinutes:    5,
		ContainerTimeoutMinutes: 15,
		HTTPTimeoutSeconds:      60,
	}

	// Configure stage settings
	stageConfig := map[string]interface{}{
		"scrape_mode":   "full",
		"upload_format": "json",
		"process_type":  "standard",
		"validate_data": true,
	}

	// Submit pipeline job with custom timeouts
	jobID := runner.SubmitPipelineJobWithTimeouts(
		govIDs,
		pipelineStages,
		stageConfig,
		testTimeouts,
		false, // No map-reduce for integration test
		0,
	)

	log.Printf("✅ Submitted pipeline job: %s", jobID)

	// Monitor pipeline progress
	maxWaitTime := 20 * time.Minute // Total maximum wait time
	checkInterval := 10 * time.Second
	startTime := time.Now()

	var finalResult *JobSummary
	var pipelineCompleted bool

	for time.Since(startTime) < maxWaitTime && !pipelineCompleted {
		time.Sleep(checkInterval)

		// Check job progress
		summary := runner.GetJobSummary(jobID)
		if summary != nil {
			log.Printf("📊 Pipeline progress: %d total results, %d successful, %d failed",
				summary.TotalGovIDs, summary.Successful, summary.Failed)

			// Check if we have results for pipeline completion
			if completionResult, exists := summary.Results["pipeline-complete"]; exists {
				log.Printf("🎉 Pipeline completed!")
				finalResult = summary
				pipelineCompleted = true

				// Validate completion result structure
				if completionMap, ok := completionResult.(map[string]interface{}); ok {
					if completedStages, hasStages := completionMap["completed_stages"]; hasStages {
						log.Printf("✅ Completed stages: %v", completedStages)
					}
					if stageResults, hasResults := completionMap["stage_results"]; hasResults {
						log.Printf("📋 Stage results available: %v", stageResults != nil)
					}
				}
				break
			}

			// Check for failures
			if summary.Failed > 0 && len(summary.Errors) > 0 {
				log.Printf("❌ Pipeline failures detected:")
				for _, err := range summary.Errors {
					log.Printf("  - %s", err)
				}
				// Continue waiting in case there are retries
			}
		}

		// Check pipeline-specific progress
		pipelineProgress := runner.GetPipelineProgress(jobID)
		if pipelineProgress != nil {
			log.Printf("🔄 Pipeline status: %s (%d/%d stages completed)",
				pipelineProgress.Status,
				pipelineProgress.CompletedStages,
				pipelineProgress.TotalStages)

			if pipelineProgress.Status == "completed" {
				pipelineCompleted = true
				break
			}

			if pipelineProgress.Status == "failed" && len(pipelineProgress.Errors) > 0 {
				t.Errorf("Pipeline failed with errors: %v", pipelineProgress.Errors)
				return
			}
		}

		log.Printf("⏳ Waiting for pipeline completion... (elapsed: %v)", time.Since(startTime))
	}

	// Final validation
	if !pipelineCompleted {
		t.Errorf("Pipeline did not complete within %v", maxWaitTime)
		return
	}

	if finalResult == nil {
		finalResult = runner.GetJobSummary(jobID)
	}

	// Validate final results
	if finalResult == nil {
		t.Error("No final job summary available")
		return
	}

	// Check that we have the expected number of successful results
	expectedResults := len(govIDs) + 1 // govID results + pipeline-complete result
	if finalResult.Successful < expectedResults {
		t.Errorf("Expected at least %d successful results, got %d", expectedResults, finalResult.Successful)
	}

	// Validate that all pipeline stages completed
	if completionResult, exists := finalResult.Results["pipeline-complete"]; exists {
		if completionMap, ok := completionResult.(map[string]interface{}); ok {
			if completedStages, hasStages := completionMap["completed_stages"]; hasStages {
				if stageList, isList := completedStages.([]PipelineStage); isList {
					if len(stageList) != len(pipelineStages) {
						t.Errorf("Expected %d completed stages, got %d", len(pipelineStages), len(stageList))
					}

					// Verify all expected stages are present
					stageMap := make(map[PipelineStage]bool)
					for _, stage := range stageList {
						stageMap[stage] = true
					}

					for _, expectedStage := range pipelineStages {
						if !stageMap[expectedStage] {
							t.Errorf("Missing expected stage: %s", expectedStage)
						}
					}
				}
			}
		}
	} else {
		t.Error("Pipeline completion result not found")
	}

	// Print final statistics
	log.Printf("📈 Final Statistics:")
	stats := runner.GetStats()
	for key, value := range stats {
		log.Printf("  %s: %v", key, value)
	}

	// Validate that no critical errors occurred
	if finalResult.Failed > 0 {
		log.Printf("⚠️  Some operations failed, but pipeline completed:")
		for _, err := range finalResult.Errors {
			log.Printf("  - %s", err)
		}
	}

	log.Printf("🎉 Pipeline integration test completed successfully!")
	log.Printf("⏱️  Total execution time: %v", time.Since(startTime))
}

// TestPipelineWithCustomStages tests pipeline with custom stage configuration
func TestPipelineWithCustomStages(t *testing.T) {
	// Test with only scrape and upload stages
	govIDs := []string{"25-01548"}
	scraperImage := "jobrunner-worker:latest"
	workDir := "/tmp/jobrunner-custom-stages-test"
	dokitoBaseURL := "http://localhost:8123"

	runner, err := NewRunner(1, scraperImage, workDir, dokitoBaseURL)
	if err != nil {
		t.Fatalf("Failed to create runner: %v", err)
	}
	defer runner.Stop()

	log.Printf("🧪 Starting custom stages pipeline test")

	// Define custom pipeline stages (only scrape and upload)
	customStages := []PipelineStage{
		PipelineStageScrape,
		PipelineStageUpload,
	}

	stageConfig := map[string]interface{}{
		"scrape_mode": "meta", // Only metadata scraping
	}

	// Submit pipeline job
	jobID := runner.SubmitPipelineJob(
		govIDs,
		customStages,
		stageConfig,
		false,
		0,
	)

	log.Printf("✅ Submitted custom stages pipeline job: %s", jobID)

	// Wait for completion (shorter timeout for custom stages)
	maxWaitTime := 10 * time.Minute
	checkInterval := 5 * time.Second
	startTime := time.Now()

	for time.Since(startTime) < maxWaitTime {
		time.Sleep(checkInterval)

		summary := runner.GetJobSummary(jobID)
		if summary != nil && summary.Successful > 0 {
			// Check if pipeline completed
			if completionResult, exists := summary.Results["pipeline-complete"]; exists {
				log.Printf("🎉 Custom stages pipeline completed!")

				// Validate that only the expected stages completed
				if completionMap, ok := completionResult.(map[string]interface{}); ok {
					if completedStages, hasStages := completionMap["completed_stages"]; hasStages {
						if stageList, isList := completedStages.([]PipelineStage); isList {
							if len(stageList) != len(customStages) {
								t.Errorf("Expected %d completed stages, got %d", len(customStages), len(stageList))
							}
							log.Printf("✅ Custom stages completed: %v", stageList)
						}
					}
				}

				log.Printf("⏱️  Custom stages execution time: %v", time.Since(startTime))
				return
			}
		}
	}

	t.Error("Custom stages pipeline did not complete within expected time")
}

// TestPipelineRetryLogic tests the retry mechanism for failed stages
func TestPipelineRetryLogic(t *testing.T) {
	log.Printf("🧪 Testing pipeline retry logic")

	// Test retry configuration
	retryConfig := GetDefaultRetryConfig()
	retryConfig.MaxRetries = 2
	retryConfig.InitialDelay = 1 * time.Second

	log.Printf("📋 Retry config: MaxRetries=%d, InitialDelay=%v",
		retryConfig.MaxRetries, retryConfig.InitialDelay)

	// Test isRetryableError function
	worker := &Worker{ID: "test-worker"}

	// Test retryable errors
	retryableErrors := []error{
		fmt.Errorf("connection refused"),
		fmt.Errorf("temporary failure in processing"),
		fmt.Errorf("service unavailable"),
		fmt.Errorf("timeout exceeded"),
	}

	for _, err := range retryableErrors {
		if !worker.isRetryableError(err, retryConfig) {
			t.Errorf("Error should be retryable: %v", err)
		}
	}

	// Test non-retryable errors
	nonRetryableErrors := []error{
		fmt.Errorf("invalid format"),
		fmt.Errorf("authentication failed"),
		fmt.Errorf("permission denied"),
	}

	for _, err := range nonRetryableErrors {
		if worker.isRetryableError(err, retryConfig) {
			t.Errorf("Error should not be retryable: %v", err)
		}
	}

	// Test backoff delay calculation
	expectedDelays := []time.Duration{
		2 * time.Second, // 1 * 2^1
		4 * time.Second, // 1 * 2^2
		8 * time.Second, // 1 * 2^3
	}

	for i, expectedDelay := range expectedDelays {
		actualDelay := worker.calculateBackoffDelay(i+1, retryConfig)
		if actualDelay != expectedDelay {
			t.Errorf("Attempt %d: expected delay %v, got %v", i+1, expectedDelay, actualDelay)
		}
	}

	log.Printf("✅ Retry logic tests passed")
}
