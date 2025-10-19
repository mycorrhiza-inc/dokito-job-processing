package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	// Get required environment variables
	endpoint := os.Getenv("DIGITALOCEAN_S3_ENDPOINT")
	region := os.Getenv("DIGITALOCEAN_S3_REGION")
	accessKey := os.Getenv("DIGITALOCEAN_S3_ACCESS_KEY")
	secretKey := os.Getenv("DIGITALOCEAN_S3_SECRET_KEY")
	bucket := os.Getenv("OPENSCRAPERS_S3_OBJECT_BUCKET")

	// Check that all required variables are defined
	if endpoint == "" {
		return S3Location{}, fmt.Errorf("DIGITALOCEAN_S3_ENDPOINT environment variable is required")
	}
	if region == "" {
		return S3Location{}, fmt.Errorf("DIGITALOCEAN_S3_REGION environment variable is required")
	}
	if accessKey == "" {
		return S3Location{}, fmt.Errorf("DIGITALOCEAN_S3_ACCESS_KEY environment variable is required")
	}
	if secretKey == "" {
		return S3Location{}, fmt.Errorf("DIGITALOCEAN_S3_SECRET_KEY environment variable is required")
	}
	if bucket == "" {
		return S3Location{}, fmt.Errorf("OPENSCRAPERS_S3_OBJECT_BUCKET environment variable is required")
	}

	return S3Location{
		Bucket:    bucket,
		Key:       key,
		Region:    region,
		Endpoint:  endpoint,
		AccessKey: accessKey,
		SecretKey: secretKey,
	}, nil
}

// createS3Client creates an AWS S3 client configured for the S3Location
func (s3loc *S3Location) createS3Client(ctx context.Context) (*s3.Client, error) {
	// Create static credentials
	staticCreds := credentials.NewStaticCredentialsProvider(s3loc.AccessKey, s3loc.SecretKey, "")

	// Load the AWS configuration
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(staticCreds),
		config.WithRegion(s3loc.Region),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client with custom endpoint if specified
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if s3loc.Endpoint != "" {
			o.BaseEndpoint = aws.String(s3loc.Endpoint)
			o.UsePathStyle = true // Required for DigitalOcean Spaces and other S3-compatible services
		}
	})

	return s3Client, nil
}

// RetrieveJsonFrom downloads JSON data from S3 and unmarshals it into the provided interface
func (s3loc *S3Location) RetrieveJsonFrom(ctx context.Context, target *interface{}) error {
	log.Printf("🔍 Retrieving JSON from S3: s3://%s/%s", s3loc.Bucket, s3loc.Key)

	// Create S3 client
	s3Client, err := s3loc.createS3Client(ctx)
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	// Get object from S3
	result, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s3loc.Bucket),
		Key:    aws.String(s3loc.Key),
	})
	if err != nil {
		return fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer result.Body.Close()

	// Read the response body
	jsonData, err := io.ReadAll(result.Body)
	if err != nil {
		return fmt.Errorf("failed to read S3 object body: %w", err)
	}

	// Unmarshal JSON into target
	if err := json.Unmarshal(jsonData, target); err != nil {
		return fmt.Errorf("failed to unmarshal JSON from S3: %w", err)
	}

	log.Printf("✅ Successfully retrieved JSON from S3, data size: %d bytes", len(jsonData))
	return nil
}

// UploadJsonTo marshals the provided data to JSON and uploads it to S3
func (s3loc *S3Location) UploadJsonTo(ctx context.Context, data interface{}) error {
	log.Printf("📤 Uploading JSON to S3: s3://%s/%s", s3loc.Bucket, s3loc.Key)

	// Marshal data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data for upload: %w", err)
	}

	// Create S3 client
	s3Client, err := s3loc.createS3Client(ctx)
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	// Upload to S3
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s3loc.Bucket),
		Key:         aws.String(s3loc.Key),
		Body:        bytes.NewReader(jsonData),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("failed to upload object to S3: %w", err)
	}

	log.Printf("✅ Successfully uploaded JSON to S3, data size: %d bytes", len(jsonData))
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
	key := fmt.Sprintf("objects_raw/%s/%s/%s/%s",
		self.JurInfo.Country,
		self.JurInfo.State,
		self.JurInfo.Jurisdiction,
		self.DocketGovId)

	return NewOpenscrapersBucketLocation(key)
}

type ProcessedDocketLocation struct {
	JurInfo     JurisdictionInfo
	DocketGovId string
}

func (self *ProcessedDocketLocation) GenerateS3Location() (S3Location, error) {
	key := fmt.Sprintf("objects/%s/%s/%s/%s",
		self.JurInfo.Country,
		self.JurInfo.State,
		self.JurInfo.Jurisdiction,
		self.DocketGovId)

	return NewOpenscrapersBucketLocation(key)
}
