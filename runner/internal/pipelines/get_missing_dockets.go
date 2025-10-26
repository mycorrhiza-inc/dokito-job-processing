package pipelines

import (
	"context"
	"encoding/json"
	"fmt"
	"runner/internal/core"
)

// ExecuteMetadataFetcher runs the NY PUC scraper to fetch all docket metadata
func ExecuteMetadataFetcher(ctx context.Context) ([]map[string]any, error) {
	config := core.GetExecutionConfig(ctx)
	if config.ScraperPaths.NYPUCPath == "" {
		return nil, fmt.Errorf("NY PUC scraper binary path not configured")
	}

	args := []string{"--mode", "ids_only", "--fetch-all-docket-metadata"}

	// Create debug-aware command
	cmd := core.CommandContext(ctx, "🔍 [Metadata Fetcher]", config.ScraperPaths.NYPUCPath, args...)

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
func ExecuteDatabaseUtilsListDocketIds(ctx context.Context, jurisdiction string) ([]string, error) {
	config := core.GetExecutionConfig(ctx)
	if config.DokitoPaths.DatabaseUtilsPath == "" {
		return nil, fmt.Errorf("database utils binary path not configured")
	}

	args := []string{"list-docket-ids", "--fixed-jur", jurisdiction}

	// Create debug-aware command
	cmd := core.CommandContext(ctx, "🗃️ [Database Utils]", config.DokitoPaths.DatabaseUtilsPath, args...)

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

// GetMissingGovIds returns a slice of NY PUC govids that are not currently in the database
// Note: This function only works for new_york_puc jurisdiction since metadata fetching
// is only implemented for the NY PUC scraper
func GetMissingGovIds(ctx context.Context) ([]string, error) {
	// Fetch all metadata from the NY PUC scraper
	allMetadata, err := ExecuteMetadataFetcher(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata: %v", err)
	}
	allGovIds := GovidsFromJsonDataFiltered(allMetadata)

	// Get govids currently in database for NY PUC jurisdiction
	dbGovIds, err := ExecuteDatabaseUtilsListDocketIds(ctx, "new_york_puc")
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

// GetMissingGovIdsFromMetadata returns a slice of NY PUC govids that are not currently in the database
// This version takes pre-fetched metadata instead of scraping
func GetMissingGovIdsFromMetadata(ctx context.Context, metadata []map[string]any) ([]string, error) {
	// Extract govids from provided metadata
	allGovIds := GovidsFromJsonDataFiltered(metadata)

	// Get govids currently in database for NY PUC jurisdiction
	dbGovIds, err := ExecuteDatabaseUtilsListDocketIds(ctx, "new_york_puc")
	if err != nil {
		return nil, fmt.Errorf("failed to get database govids: %v", err)
	}

	// Create a map for efficient lookup
	dbGovIDSet := make(map[string]bool)
	for _, govid := range dbGovIds {
		dbGovIDSet[govid] = true
	}

	// Find govids that are in scraped data but not in database
	var missingGovIds []string
	for _, govid := range allGovIds {
		if !dbGovIDSet[govid] {
			missingGovIds = append(missingGovIds, govid)
		}
	}

	return missingGovIds, nil
}
