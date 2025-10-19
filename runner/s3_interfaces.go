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

func NewOpenscrapersBucketLocation(key string) (S3Location, error) {
	// Here are the important env variables.
	// DIGITALOCEAN_S3_ENDPOINT=https://sfo3.digitaloceanspaces.com
	// DIGITALOCEAN_S3_REGION=sfo3
	//
	// DIGITALOCEAN_S3_ACCESS_KEY=
	// DIGITALOCEAN_S3_SECRET_KEY=
	// OPENSCRAPERS_S3_OBJECT_BUCKET=openscrapers
	// If any are undefined return an error. Else return an object with the key defined
}

// RetrieveJsonFrom downloads JSON data from S3 and unmarshals it into the provided interface
func (s3loc *S3Location) RetrieveJsonFrom(ctx context.Context, target *interface{}) error {
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

type CannonicalS3Address interface {
	GenerateS3Location() (S3Location, error)
}

func UploadJsonToAddress[T CannonicalS3Address](ctx context.Context, data interface{}, address T) error {
	location, err := address.GenerateS3Location()
	if err != nil {
		return err
	}
	return location.UploadJsonTo(ctx, data)
}

func DownloadJsonFromAddress[T CannonicalS3Address](ctx context.Context, target *interface{}, address T) error {
	location, err := address.GenerateS3Location()
	if err != nil {
		return err
	}
	return location.RetrieveJsonFrom(ctx, target)
}

type JurisdictionInfo struct {
	Country      string
	State        string
	Jurisdiction string
}

type RawDocketLocation struct {
	JurInfo     JurisdictionInfo
	DocketGovId string
}

func (self *RawDocketLocation) GenerateS3Location() (S3Location, error) {
	key :=   format!("objects_raw/{country}/{state}/{jurisdiction}/{case_name}")
}

type ProcessedDocketLocation struct {
	JurInfo     JurisdictionInfo
	DocketGovId string
}

func (self *ProcessedDocketLocation) GenerateS3Location() (S3Location, error) {
	key :=   format!("objects/{country}/{state}/{jurisdiction}/{case_name}")
}
