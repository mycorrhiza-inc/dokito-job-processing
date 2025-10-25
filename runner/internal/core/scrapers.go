package core

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"time"

	"runner/internal/storage"
)

func ExecuteScraperWithALLMode(govID string, scraperType ScraperType, paths ScraperBinaryPaths, extraArgs ...string) ([]map[string]any, error) {
	var binaryPath string
	switch scraperType {
	case NYPUC:
		binaryPath = paths.NYPUCPath
	case COPUC:
		binaryPath = paths.COPUCPath
	case UtahCoal:
		binaryPath = paths.UtahCoalPath
	default:
		return nil, fmt.Errorf("unknown scraper type: %s", scraperType)
	}

	if binaryPath == "" {
		return nil, fmt.Errorf("binary path not configured for scraper type: %s", scraperType)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	intermediateDir := storage.GetPlaywrightIntermediateDir()

	// Build base arguments
	args := []string{"--gov-ids", govID, "--mode", "all", "--intermediate-dir", intermediateDir}

	// Append extra arguments if provided
	args = append(args, extraArgs...)

	cmd := exec.CommandContext(ctx, binaryPath, args...)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("scraper execution failed: %v", err)
	}

	var results []map[string]any
	if err := json.Unmarshal(output, &results); err != nil {
		return nil, fmt.Errorf("failed to parse scraper output as JSON: %v", err)
	}

	return results, nil
}

// ExecuteScraperWithALLModeDebug runs scraper with real-time stdout streaming for debugging
func ExecuteScraperWithALLModeDebug(govID string, scraperType ScraperType, paths ScraperBinaryPaths, extraArgs ...string) ([]map[string]any, error) {
	var binaryPath string
	switch scraperType {
	case NYPUC:
		binaryPath = paths.NYPUCPath
	case COPUC:
		binaryPath = paths.COPUCPath
	case UtahCoal:
		binaryPath = paths.UtahCoalPath
	default:
		return nil, fmt.Errorf("unknown scraper type: %s", scraperType)
	}

	if binaryPath == "" {
		return nil, fmt.Errorf("binary path not configured for scraper type: %s", scraperType)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	intermediateDir := storage.GetPlaywrightIntermediateDir()

	// Build base arguments
	args := []string{"--gov-ids", govID, "--mode", "all", "--intermediate-dir", intermediateDir}

	// Append extra arguments if provided
	args = append(args, extraArgs...)

	cmd := exec.CommandContext(ctx, binaryPath, args...)
	label := fmt.Sprintf("🔍 [%s]", scraperType)

	// Use helper function for debug streaming
	output, err := executeWithDebugStreaming(cmd, label)
	if err != nil {
		return nil, fmt.Errorf("scraper execution failed: %v", err)
	}

	var results []map[string]any
	if err := json.Unmarshal(output, &results); err != nil {
		return nil, fmt.Errorf("failed to parse scraper output as JSON: %v", err)
	}

	return results, nil
}

