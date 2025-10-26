package core

import (
	"context"
	"encoding/json"
	"fmt"

	"runner/internal/storage"
)

func ExecuteScraperWithALLMode(ctx context.Context, govID string, scraperType ScraperType, extraArgs ...string) ([]map[string]any, error) {
	config := GetExecutionConfig(ctx)
	var binaryPath string
	switch scraperType {
	case NYPUC:
		binaryPath = config.ScraperPaths.NYPUCPath
	case COPUC:
		binaryPath = config.ScraperPaths.COPUCPath
	case UtahCoal:
		binaryPath = config.ScraperPaths.UtahCoalPath
	default:
		return nil, fmt.Errorf("unknown scraper type: %s", scraperType)
	}

	if binaryPath == "" {
		return nil, fmt.Errorf("binary path not configured for scraper type: %s", scraperType)
	}

	intermediateDir := storage.GetPlaywrightIntermediateDir()

	// Build base arguments
	args := []string{"--gov-ids", govID, "--mode", "all", "--intermediate-dir", intermediateDir}

	// Append extra arguments if provided
	args = append(args, extraArgs...)

	// Create debug-aware command
	label := fmt.Sprintf("🔍 [%s]", scraperType)
	cmd := CommandContext(ctx, label, binaryPath, args...)

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


