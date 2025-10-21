package core

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

func ValidateJSONAsArrayOfMaps(data []map[string]interface{}) ([]map[string]interface{}, error) {
	if data == nil {
		return nil, fmt.Errorf("data is nil")
	}
	return data, nil
}

func ExecuteDataProcessingBinary(data []map[string]interface{}, paths DokitoBinaryPaths) ([]map[string]interface{}, error) {
	if paths.ProcessDocketsPath == "" {
		return nil, fmt.Errorf("process dockets binary path not configured")
	}

	inputJSON, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal input data: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, paths.ProcessDocketsPath, "process")
	cmd.Stdin = strings.NewReader(string(inputJSON))

	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("data processing failed: %v", err)
	}

	var results []map[string]interface{}
	if err := json.Unmarshal(output, &results); err != nil {
		return nil, fmt.Errorf("failed to parse processing output as JSON: %v", err)
	}

	return results, nil
}

func ExecuteUploadBinary(data []map[string]interface{}, paths DokitoBinaryPaths) error {
	if paths.UploadDocketsPath == "" {
		return fmt.Errorf("upload dockets binary path not configured")
	}

	inputJSON, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal input data: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, paths.UploadDocketsPath, "upload")
	cmd.Stdin = strings.NewReader(string(inputJSON))

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("upload failed: %v", err)
	}

	return nil
}