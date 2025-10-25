package pipelines

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"runner/internal/core"
	"time"
)

// ExecuteMetadataFetcher runs the NY PUC scraper to fetch all docket metadata
func ExecuteMetadataFetcher(paths core.ScraperBinaryPaths) ([]map[string]any, error) {
	if paths.NYPUCPath == "" {
		return nil, fmt.Errorf("NY PUC scraper binary path not configured")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	args := []string{"--mode", "ids_only", "--fetch-all-docket-metadata"}
	cmd := exec.CommandContext(ctx, paths.NYPUCPath, args...)

	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("metadata fetcher execution failed: %v", err)
	}

	var results []map[string]any
	if err := json.Unmarshal(output, &results); err != nil {
		return nil, fmt.Errorf("failed to parse metadata fetcher output as JSON: %v", err)
	}

	return results, nil
}

// ExecuteDatabaseUtilsListDocketIds runs database-utils to list all docket govids for a jurisdiction
func ExecuteDatabaseUtilsListDocketIds(jurisdiction string, paths core.DokitoBinaryPaths) ([]string, error) {
	if paths.DatabaseUtilsPath == "" {
		return nil, fmt.Errorf("database utils binary path not configured")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	args := []string{"list-docket-ids", "--fixed-jur", jurisdiction}
	cmd := exec.CommandContext(ctx, paths.DatabaseUtilsPath, args...)

	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("database utils execution failed: %v", err)
	}

	// Parse the output as a JSON array of strings
	var govids []string
	if err := json.Unmarshal(output, &govids); err != nil {
		return nil, fmt.Errorf("failed to parse database utils output as JSON: %v", err)
	}

	return govids, nil
}

// GetMissingGovIds returns a slice of govids that are not currently in the database
func GetMissingGovIds(jurisdiction string, scraperPaths core.ScraperBinaryPaths, dokitoPaths core.DokitoBinaryPaths) ([]string, error) {
	// Fetch all metadata from the scraper
	allMetadata, err := ExecuteMetadataFetcher(scraperPaths)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata: %v", err)
	}
	allGovIds := GovidsFromJsonDataFiltered(allMetadata)

	// Get govids currently in database
	dbGovIds, err := ExecuteDatabaseUtilsListDocketIds(jurisdiction, dokitoPaths)
	if err != nil {
		return nil, fmt.Errorf("failed to get database govids: %v", err)
	}

	// Create a map for efficient lookup
	dbGovIDSet := make(map[string]bool)
	for _, govid := range dbGovIds {
		dbGovIDSet[govid] = true
	}

	// Find govids that are not in the database
	var missingGovIds []string
	for _, govid := range allGovIds {
		if !dbGovIDSet[govid] {
			missingGovIds = append(missingGovIds, govid)
		}
	}

	return missingGovIds, nil
}
