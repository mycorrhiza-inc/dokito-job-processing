package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// S3Location represents an S3 object location with bucket and key
type S3Location struct {
	Bucket   string `json:"bucket"`
	Key      string `json:"key"`
	Region   string `json:"region,omitempty"`
	Endpoint string `json:"endpoint,omitempty"`
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

// String returns a string representation of the S3 location
func (s3loc *S3Location) String() string {
	return fmt.Sprintf("s3://%s/%s", s3loc.Bucket, s3loc.Key)
}

// CannonicalS3AddressGenerator defines the interface for generating S3 locations from objects
type CannonicalS3AddressGenerator interface {
	GenerateS3Location(obj interface{}) (*S3Location, error)
	GetBucket(obj interface{}) string
	GetKeyPrefix() string
}

// CannonicalS3Address is a concrete implementation for generating S3 locations
type CannonicalS3Address struct {
	BaseBucket  string `json:"base_bucket"`
	KeyPrefix   string `json:"key_prefix"`
	Region      string `json:"region,omitempty"`
	Endpoint    string `json:"endpoint,omitempty"`
}

// NewCannonicalS3Address creates a new CannonicalS3Address with environment defaults
func NewCannonicalS3Address() *CannonicalS3Address {
	return &CannonicalS3Address{
		BaseBucket: getEnvOrDefault("OPENSCRAPERS_S3_OBJECT_BUCKET", "openscrapers"),
		KeyPrefix:  "objects",
		Region:     getEnvOrDefault("DIGITALOCEAN_S3_REGION", "sfo3"),
		Endpoint:   getEnvOrDefault("DIGITALOCEAN_S3_ENDPOINT", "https://sfo3.digitaloceanspaces.com"),
	}
}

// GenerateS3Location creates an S3Location for the given object
func (csa *CannonicalS3Address) GenerateS3Location(obj interface{}) (*S3Location, error) {
	key, err := csa.generateObjectKey(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to generate object key: %w", err)
	}

	return &S3Location{
		Bucket:   csa.GetBucket(obj),
		Key:      key,
		Region:   csa.Region,
		Endpoint: csa.Endpoint,
	}, nil
}

// GetBucket returns the bucket name for the given object
func (csa *CannonicalS3Address) GetBucket(obj interface{}) string {
	return csa.BaseBucket
}

// GetKeyPrefix returns the base key prefix
func (csa *CannonicalS3Address) GetKeyPrefix() string {
	return csa.KeyPrefix
}

// generateObjectKey creates an S3 key based on the object's properties
func (csa *CannonicalS3Address) generateObjectKey(obj interface{}) (string, error) {
	switch v := obj.(type) {
	case map[string]interface{}:
		return csa.generateKeyFromMap(v)
	case string:
		// If it's just a string, treat it as a case ID
		return fmt.Sprintf("%s/cases/%s.json", csa.KeyPrefix, v), nil
	default:
		// Try to marshal to JSON and extract identifiers
		jsonData, err := json.Marshal(obj)
		if err != nil {
			return "", fmt.Errorf("cannot marshal object to generate key: %w", err)
		}

		var objMap map[string]interface{}
		if err := json.Unmarshal(jsonData, &objMap); err != nil {
			return "", fmt.Errorf("cannot unmarshal object to map: %w", err)
		}

		return csa.generateKeyFromMap(objMap)
	}
}

// generateKeyFromMap creates an S3 key from a map of properties
func (csa *CannonicalS3Address) generateKeyFromMap(objMap map[string]interface{}) (string, error) {
	// Try to extract jurisdiction and case information
	var country, state, jurisdiction, caseID string

	// Look for case_govid or similar
	if govID, exists := objMap["case_govid"]; exists {
		if govIDStr, ok := govID.(string); ok {
			caseID = govIDStr
		}
	} else if id, exists := objMap["id"]; exists {
		if idStr, ok := id.(string); ok {
			caseID = idStr
		}
	}

	// Look for jurisdiction information
	if jur, exists := objMap["jurisdiction"]; exists {
		if jurStr, ok := jur.(string); ok {
			jurisdiction = jurStr
		}
	}

	if st, exists := objMap["state"]; exists {
		if stStr, ok := st.(string); ok {
			state = stStr
		}
	}

	if ctry, exists := objMap["country"]; exists {
		if ctryStr, ok := ctry.(string); ok {
			country = ctryStr
		}
	}

	// Set defaults if not found
	if country == "" {
		country = "usa"
	}
	if state == "" {
		state = "ny"
	}
	if jurisdiction == "" {
		jurisdiction = "nypuc"
	}
	if caseID == "" {
		caseID = "unknown"
	}

	return fmt.Sprintf("%s/%s/%s/%s/%s.json", csa.KeyPrefix, country, state, jurisdiction, caseID), nil
}

// getEnvOrDefault returns environment variable value or default
func getEnvOrDefault(envVar, defaultValue string) string {
	if value := os.Getenv(envVar); value != "" {
		return value
	}
	return defaultValue
}

