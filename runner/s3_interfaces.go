package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
)

// S3Location represents an S3 object location with bucket and key
type S3Location struct {
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	Region    string `json:"region,omitempty"`
	Endpoint  string `json:"endpoint,omitempty"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
}

// RetrieveJsonFrom downloads JSON data from S3 and unmarshals it into the provided interface
func (s3loc *S3Location) RetrieveJsonFrom(ctx context.Context, target interface{}) error {
	// For testing purposes, we'll use a mock implementation
	// In production, this would use AWS SDK for Go v2
	log.Printf("🔍 Retrieving JSON from S3: s3://%s/%s", s3loc.Bucket, s3loc.Key)

	// Mock implementation - would normally use s3.GetObject
	mockData := map[string]interface{}{
		"source": "s3",
		"bucket": s3loc.Bucket,
		"key":    s3loc.Key,
		"data":   "mock_retrieved_data",
	}

	jsonData, err := json.Marshal(mockData)
	if err != nil {
		return fmt.Errorf("failed to marshal mock data: %w", err)
	}

	return json.Unmarshal(jsonData, target)
}

// UploadJsonTo marshals the provided data to JSON and uploads it to S3
func (s3loc *S3Location) UploadJsonTo(ctx context.Context, data interface{}) error {
	log.Printf("📤 Uploading JSON to S3: s3://%s/%s", s3loc.Bucket, s3loc.Key)

	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data for upload: %w", err)
	}

	// Mock implementation - would normally use s3.PutObject
	log.Printf("✅ Mock upload successful, data size: %d bytes", len(jsonData))
	return nil
}
