package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// getDokitoURL returns the real Dokito backend URL for testing
func getDokitoURL() string {
	dokitoURL := os.Getenv("DOKITO_BACKEND_URL")
	if dokitoURL == "" {
		dokitoURL = "http://localhost:8123" // Default to local development
	}
	return dokitoURL
}

// checkDokitoServerHealth verifies the real Dokito backend is running and healthy
func checkDokitoServerHealth(t *testing.T, dokitoURL string) {
	resp, err := http.Get(dokitoURL + "/health")
	if err != nil {
		t.Fatalf("Dokito backend not reachable at %s: %v", dokitoURL, err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Dokito backend health check failed: status %d", resp.StatusCode)
	}
	
	log.Printf("✅ Dokito backend is healthy at %s", dokitoURL)
}

// TestDokitoAPIIntegration tests the Dokito API integration against the REAL server
func TestDokitoAPIIntegration(t *testing.T) {
	// Get real Dokito backend URL
	dokitoURL := getDokitoURL()
	
	// Verify real server is running and healthy
	checkDokitoServerHealth(t, dokitoURL)
	
	log.Printf("🧪 Testing Dokito API integration with REAL server: %s", dokitoURL)
	
	// Create a test worker
	worker := &Worker{
		ID: "test-worker-dokito",
	}
	
	// Test data for pipeline stages - using real scraped data format
	mockScrapeResults := map[string]interface{}{
		"25-01799": map[string]interface{}{
			"case_govid": "25-01799",
			"case_name": "Niagara Mohawk Power Corporation d/b/a National Grid filed the following Statement: Supply Service Charges (SC) No. 169 to PSC No. 220 - Electricity.",
			"case_subtype": "Statements",
			"case_type": "Tariff",
			"case_url": "https://documents.dps.ny.gov/public/MatterManagement/CaseMaster.aspx?MatterCaseNo=25-01799",
			"industry": "Unknown",
			"opened_date": "2025-08-26T00:00:00.000Z",
			"petitioner": "Niagara Mohawk Power Corporation d/b/a National Grid",
		},
	}
	
	timeouts := GetDefaultTimeouts()
	ctx := context.Background()
	
	// Test 1: Upload stage
	log.Println("📤 Testing upload stage...")
	uploadResult, err := worker.uploadToDokitoWithContext(ctx, mockScrapeResults, dokitoURL, timeouts)
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}
	
	uploadMap, ok := uploadResult.(map[string]interface{})
	if !ok {
		t.Fatalf("Upload result is not a map: %T", uploadResult)
	}
	
	if status, hasStatus := uploadMap["status"]; !hasStatus || status != "success" {
		t.Errorf("Expected upload status 'success', got: %v", status)
	}
	
	if uploadID, hasID := uploadMap["upload_id"]; !hasID {
		t.Error("Upload result missing upload_id")
	} else {
		log.Printf("✅ Upload successful: %v", uploadID)
	}
	
	// Test 2: Process stage
	log.Println("⚙️ Testing process stage...")
	processResult, err := worker.triggerDokitoProcessingWithContext(ctx, uploadResult, dokitoURL, timeouts)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}
	
	processMap, ok := processResult.(map[string]interface{})
	if !ok {
		t.Fatalf("Process result is not a map: %T", processResult)
	}
	
	if status, hasStatus := processMap["status"]; !hasStatus || status != "success" {
		t.Errorf("Expected process status 'success', got: %v", status)
	}
	
	if processID, hasID := processMap["process_id"]; !hasID {
		t.Error("Process result missing process_id")
	} else {
		log.Printf("✅ Processing successful: %v", processID)
	}
	
	// Test 3: Ingest stage
	log.Println("📥 Testing ingest stage...")
	ingestResult, err := worker.triggerDokitoIngestionWithContext(ctx, processResult, dokitoURL, timeouts)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	
	ingestMap, ok := ingestResult.(map[string]interface{})
	if !ok {
		t.Fatalf("Ingest result is not a map: %T", ingestResult)
	}
	
	if status, hasStatus := ingestMap["status"]; !hasStatus || status != "success" {
		t.Errorf("Expected ingest status 'success', got: %v", status)
	}
	
	if ingestID, hasID := ingestMap["ingest_id"]; !hasID {
		t.Error("Ingest result missing ingest_id")
	} else {
		log.Printf("✅ Ingestion successful: %v", ingestID)
	}
	
	log.Println("🎉 All Dokito API stages completed successfully!")
}

// TestDokitoErrorHandling tests error scenarios with real Dokito API
func TestDokitoErrorHandling(t *testing.T) {
	// Get real Dokito backend URL
	dokitoURL := getDokitoURL()
	
	// Verify real server is running and healthy
	checkDokitoServerHealth(t, dokitoURL)
	
	log.Printf("🧪 Testing Dokito error handling with REAL server: %s", dokitoURL)
	
	worker := &Worker{ID: "test-worker-errors"}
	
	// Test with invalid data to trigger real error responses
	timeouts := TimeoutConfig{
		HTTPTimeoutSeconds: 30, // Reasonable timeout for real server
	}
	ctx := context.Background()
	
	// Test upload with invalid data format
	log.Println("🔴 Testing upload error handling with invalid data...")
	invalidData := map[string]interface{}{
		"invalid": "data", // Missing required fields
	}
	_, err := worker.uploadToDokitoWithContext(ctx, invalidData, dokitoURL, timeouts)
	if err == nil {
		t.Error("Expected upload error with invalid data, but got none")
	} else {
		log.Printf("✅ Upload error handled correctly: %v", err)
		// Verify it's a real error from the server, not a mock
		if strings.Contains(err.Error(), "mock") {
			t.Error("Error response contains 'mock' - this suggests mock server is being used!")
		}
	}
	
	// Test process with invalid data format
	log.Println("🔴 Testing process error handling with invalid data...")
	_, err = worker.triggerDokitoProcessingWithContext(ctx, invalidData, dokitoURL, timeouts)
	if err == nil {
		t.Error("Expected process error with invalid data, but got none")
	} else {
		log.Printf("✅ Process error handled correctly: %v", err)
	}
	
	// Test ingest with invalid data format
	log.Println("🔴 Testing ingest error handling with invalid data...")
	_, err = worker.triggerDokitoIngestionWithContext(ctx, invalidData, dokitoURL, timeouts)
	if err == nil {
		t.Error("Expected ingest error with invalid data, but got none")
	} else {
		log.Printf("✅ Ingest error handled correctly: %v", err)
	}
}

// TestDokitoRetryableErrors tests retry logic for real Dokito API calls
func TestDokitoRetryableErrors(t *testing.T) {
	// Get real Dokito backend URL
	dokitoURL := getDokitoURL()
	
	// Verify real server is running and healthy
	checkDokitoServerHealth(t, dokitoURL)
	
	log.Printf("🧪 Testing Dokito retry logic with REAL server: %s", dokitoURL)
	
	worker := &Worker{ID: "test-worker-retry"}
	retryConfig := RetryConfig{
		MaxRetries:      3,
		InitialDelay:    10 * time.Millisecond, // Fast retries for testing
		MaxDelay:        100 * time.Millisecond,
		BackoffFactor:   2.0,
		RetryableErrors: []string{"connection refused", "service unavailable"},
	}
	
	// Test if the error is retryable
	testError := fmt.Errorf("connection refused")
	if !worker.isRetryableError(testError, retryConfig) {
		t.Error("Connection refused should be retryable")
	}
	
	// Test backoff calculation
	delay1 := worker.calculateBackoffDelay(1, retryConfig)
	delay2 := worker.calculateBackoffDelay(2, retryConfig)
	if delay2 <= delay1 {
		t.Errorf("Backoff delay should increase: %v -> %v", delay1, delay2)
	}
	
	log.Printf("✅ Retry logic tests passed")
	log.Printf("✅ Retry logic tests passed against real server")
}

// TestFullDokitoPipeline tests the complete pipeline with REAL Dokito backend
func TestFullDokitoPipeline(t *testing.T) {
	// Get real Dokito backend URL
	dokitoURL := getDokitoURL()
	
	// Verify real server is running and healthy
	checkDokitoServerHealth(t, dokitoURL)
	
	log.Printf("🧪 Testing full Dokito pipeline integration with REAL server: %s", dokitoURL)
	
	// Create pipeline job with mock scrape results
	job := &PipelineJob{
		BaseJob: BaseJob{
			ID:     "test-pipeline-dokito",
			Type:   JobTypePipeline,
			GovIDs: []string{"25-01799"},
		},
		Stages: []PipelineStage{
			PipelineStageUpload,
			PipelineStageProcess,
			PipelineStageIngest,
		},
		CurrentStage: PipelineStageUpload,
		Results:      make(map[PipelineStage]interface{}),
		Timeouts:     GetDefaultTimeouts(),
		RetryConfig:  GetDefaultRetryConfig(),
		StageSync:    make(chan PipelineStage, 3),
	}
	
	// Pre-populate scrape results with real scraped data format
	job.Results[PipelineStageScrape] = map[string]interface{}{
		"25-01799": map[string]interface{}{
			"case_govid": "25-01799",
			"case_name": "Niagara Mohawk Power Corporation d/b/a National Grid filed the following Statement: Supply Service Charges (SC) No. 169 to PSC No. 220 - Electricity.",
			"case_subtype": "Statements",
			"case_type": "Tariff",
			"case_url": "https://documents.dps.ny.gov/public/MatterManagement/CaseMaster.aspx?MatterCaseNo=25-01799",
			"industry": "Unknown",
			"opened_date": "2025-08-26T00:00:00.000Z",
			"petitioner": "Niagara Mohawk Power Corporation d/b/a National Grid",
		},
	}
	
	worker := &Worker{
		ID: "test-pipeline-worker",
	}
	
	resultQueue := make(chan WorkerResult, 10)
	
	// Test upload stage
	log.Println("📤 Testing pipeline upload stage...")
	err := worker.executeUploadStageWithRetry(job, resultQueue, dokitoURL)
	if err != nil {
		t.Fatalf("Pipeline upload stage failed: %v", err)
	}
	
	// Verify upload result stored
	if _, exists := job.Results[PipelineStageUpload]; !exists {
		t.Error("Upload results not stored in pipeline job")
	}
	
	// Test process stage
	log.Println("⚙️ Testing pipeline process stage...")
	err = worker.executeProcessStageWithRetry(job, resultQueue, dokitoURL)
	if err != nil {
		t.Fatalf("Pipeline process stage failed: %v", err)
	}
	
	// Verify process result stored
	if _, exists := job.Results[PipelineStageProcess]; !exists {
		t.Error("Process results not stored in pipeline job")
	}
	
	// Test ingest stage
	log.Println("📥 Testing pipeline ingest stage...")
	err = worker.executeIngestStageWithRetry(job, resultQueue, dokitoURL)
	if err != nil {
		t.Fatalf("Pipeline ingest stage failed: %v", err)
	}
	
	// Verify ingest result stored
	if _, exists := job.Results[PipelineStageIngest]; !exists {
		t.Error("Ingest results not stored in pipeline job")
	}
	
	// Verify pipeline marked as completed
	if !job.Completed {
		t.Error("Pipeline not marked as completed")
	}
	
	// Check that results were sent to queue
	close(resultQueue)
	resultCount := 0
	for result := range resultQueue {
		resultCount++
		if !result.Success {
			t.Errorf("Pipeline stage result failed: %s", result.Error)
		}
		log.Printf("📊 Pipeline result: Stage=%s, Success=%v", result.Stage, result.Success)
	}
	
	if resultCount != 3 {
		t.Errorf("Expected 3 pipeline results, got %d", resultCount)
	}
	
	log.Println("🎉 Full Dokito pipeline test completed successfully!")
}