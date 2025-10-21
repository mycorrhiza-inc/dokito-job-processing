package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
)

func GetBinaryExecutionPath() string {
	binaryEnvironmentPath := os.Getenv("BINARY_EXECUTION_PATH")
	if binaryEnvironmentPath == "" {
		binaryEnvironmentPath, _ = os.Getwd()
	}
	return binaryEnvironmentPath
}

func GetProcessingIntermediateDir() string {
	basePath := GetBinaryExecutionPath()
	return path.Join(basePath, "processing_objects")
}

func GetPlaywrightIntermediateDir() string {
	basePath := GetBinaryExecutionPath()
	return path.Join(basePath, "playwright_intermediates")
}

// CannonicalLocalPath represents something that can generate a canonical local file path
type CannonicalLocalPath interface {
	GenerateCannonicalPath() string
}

// WriteJSON marshals data to JSON and writes it using a CannonicalLocalPath
func WriteJSON(ctx context.Context, pathProvider CannonicalLocalPath, data any) error {
	path := pathProvider.GenerateCannonicalPath()
	return WriteJSONToPath(ctx, path, data)
}

// ReadJSON reads and unmarshals JSON using a CannonicalLocalPath
func ReadJSON(ctx context.Context, pathProvider CannonicalLocalPath, target *any) error {
	path := pathProvider.GenerateCannonicalPath()
	return ReadJSONFromPath(ctx, path, target)
}

// WriteBytes writes raw bytes using a CannonicalLocalPath
func WriteBytes(ctx context.Context, pathProvider CannonicalLocalPath, data []byte) error {
	path := pathProvider.GenerateCannonicalPath()
	return WriteBytesToPath(ctx, path, data)
}

// ReadBytes reads raw bytes using a CannonicalLocalPath
func ReadBytes(ctx context.Context, pathProvider CannonicalLocalPath) ([]byte, error) {
	path := pathProvider.GenerateCannonicalPath()
	return ReadBytesFromPath(ctx, path)
}

// WriteJSONToPath marshals data to JSON and writes it to the specified file path
func WriteJSONToPath(ctx context.Context, path string, data any) error {
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal data to JSON: %v", err)
	}

	return WriteBytesToPath(ctx, path, jsonData)
}

// WriteBytesToPath writes raw bytes to the specified file path
func WriteBytesToPath(ctx context.Context, path string, data []byte) error {
	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %v", dir, err)
	}

	// Write file
	err := os.WriteFile(path, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write file %s: %v", path, err)
	}

	log.Printf("Successfully wrote %d bytes to local file: %s", len(data), path)
	return nil
}

// ReadBytesFromPath reads raw bytes from the specified file path
func ReadBytesFromPath(ctx context.Context, path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %v", path, err)
	}

	log.Printf("Successfully read %d bytes from local file: %s", len(data), path)
	return data, nil
}

// ReadJSONFromPath reads and unmarshals JSON from the specified file path
func ReadJSONFromPath(ctx context.Context, path string, target any) error {
	data, err := ReadBytesFromPath(ctx, path)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("failed to unmarshal JSON from %s: %v", path, err)
	}

	return nil
}

