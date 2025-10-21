package core

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"time"
)

func ExecuteScraperWithALLMode(govID string, scraperType ScraperType, paths ScraperBinaryPaths) ([]map[string]any, error) {
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

	cmd := exec.CommandContext(ctx, binaryPath, govID, "ALL")
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

