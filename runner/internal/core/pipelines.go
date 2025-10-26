// Package core I dont know what this package does and I dont like it, claude giveth and claude taketh away
package core

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

func ValidateJSONAsArrayOfMaps(data []map[string]any) ([]map[string]any, error) {
	if data == nil {
		return nil, fmt.Errorf("data is nil")
	}
	return data, nil
}

func ExecuteDataProcessingBinary(ctx context.Context, data []map[string]any) ([]map[string]any, error) {
	config := GetExecutionConfig(ctx)
	if config.DokitoPaths.ProcessDocketsPath == "" {
		return nil, fmt.Errorf("process dockets binary path not configured")
	}

	inputJSON, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal input data: %v", err)
	}

	// Create debug-aware command
	cmd := CommandContext(ctx, "🔧 [Processing]", config.DokitoPaths.ProcessDocketsPath, "--fixed-jur", "new_york_puc")
	cmd.Stdin = strings.NewReader(string(inputJSON))

	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("data processing failed: %v", err)
	}

	var results []map[string]any
	if err := json.Unmarshal(output, &results); err != nil {
		return nil, fmt.Errorf("failed to parse processing output as JSON: %v", err)
	}

	return results, nil
}

func ExecuteUploadBinary(ctx context.Context, data []map[string]any) error {
	config := GetExecutionConfig(ctx)
	if config.DokitoPaths.UploadDocketsPath == "" {
		return fmt.Errorf("upload dockets binary path not configured")
	}

	inputJSON, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal input data: %v", err)
	}

	// Create debug-aware command
	cmd := CommandContext(ctx, "📤 [Upload]", config.DokitoPaths.UploadDocketsPath, "--fixed-jur", "new_york_puc")
	cmd.Stdin = strings.NewReader(string(inputJSON))

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("upload failed: %v", err)
	}

	return nil
}

