package storage

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

type CannonicalS3Path interface {
	GenerateCannonicalS3Path() S3Location
}

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

func ReadS3Json[T any](ctx context.Context, pathGenerator CannonicalS3Path, target *T) error {
	s3Loc := pathGenerator.GenerateCannonicalS3Path()
	err := ReadS3LocationJSON(ctx, s3Loc, target)
	return err
}

func WriteS3Json(ctx context.Context, pathGenerator CannonicalS3Path, data any) error {
	s3Loc := pathGenerator.GenerateCannonicalS3Path()
	err := s3Loc.WriteJSON(ctx, data)
	return err
}

func (loc S3Location) WriteJSON(ctx context.Context, data any) error {
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal data to JSON: %v", err)
	}

	return loc.WriteBytes(ctx, jsonData)
}

func (loc S3Location) WriteBytes(ctx context.Context, data []byte) error {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			loc.AccessKey,
			loc.SecretKey,
			"",
		)),
		config.WithRegion(loc.Region),
	)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if loc.Endpoint != "" {
			o.BaseEndpoint = aws.String(loc.Endpoint)
		}
		o.UsePathStyle = true
	})

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(loc.Bucket),
		Key:    aws.String(loc.Key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("failed to upload to S3: %v", err)
	}

	log.Printf("Successfully uploaded %d bytes to s3://%s/%s", len(data), loc.Bucket, loc.Key)
	return nil
}

func (loc S3Location) ReadBytes(ctx context.Context) ([]byte, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			loc.AccessKey,
			loc.SecretKey,
			"",
		)),
		config.WithRegion(loc.Region),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if loc.Endpoint != "" {
			o.BaseEndpoint = aws.String(loc.Endpoint)
		}
		o.UsePathStyle = true
	})

	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(loc.Bucket),
		Key:    aws.String(loc.Key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to download from S3: %v", err)
	}
	defer result.Body.Close()

	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read S3 object body: %v", err)
	}

	log.Printf("Successfully downloaded %d bytes from s3://%s/%s", len(data), loc.Bucket, loc.Key)
	return data, nil
}

func ReadS3LocationJSON[T any](ctx context.Context, loc S3Location, target *T) error {
	data, err := loc.ReadBytes(ctx)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %v", err)
	}

	return nil
}

type CannonicalLocalAndRemote interface {
	CannonicalS3Path
	CannonicalLocalPath
}

// WriteJSONToLocalAndRemote saves JSON data to both local and remote storage simultaneously
func WriteJSONToLocalAndRemote(ctx context.Context, pathProvider CannonicalLocalAndRemote, data any) error {
	// Save to local storage
	localErr := WriteJSON(ctx, pathProvider, data)

	// Get S3 location and save to remote storage
	s3Location := pathProvider.GenerateCannonicalS3Path()
	remoteErr := s3Location.WriteJSON(ctx, data)

	// Return combined errors if any occurred
	if localErr != nil && remoteErr != nil {
		return fmt.Errorf("failed to save to both locations - local: %v, remote: %v", localErr, remoteErr)
	} else if localErr != nil {
		return fmt.Errorf("failed to save to local storage: %v", localErr)
	} else if remoteErr != nil {
		return fmt.Errorf("failed to save to remote storage: %v", remoteErr)
	}

	return nil
}

func RetriveJSONFromRemoteAndUpdateLocal[T any](ctx context.Context, pathProvider CannonicalLocalAndRemote, target *T) error {
	err := ReadS3Json(ctx, pathProvider, target)
	if err != nil {
		return err
	}
	err = WriteJSON(ctx, pathProvider, *target)
	if err != nil {
		return err
	}
	return nil
}
