package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/spf13/cobra"
	"runner/internal/core"
	"runner/internal/pipelines"
)

// missingGovidsCmd represents the missing-govids command
var missingGovidsCmd = &cobra.Command{
	Use:   "missing-govids",
	Short: "Get NY PUC govids not currently in database",
	Long: `Find all NY PUC government IDs that are not currently stored in the database.
This command compares scraped metadata against existing database records.

The command accepts metadata via stdin to avoid repeated scraping:
  cat metadata.json | dokito-cli missing-govids`,
	Example: `  dokito-cli missing-govids
  cat metadata.json | dokito-cli missing-govids
  dokito-cli missing-govids --no-debug`,
	Args: cobra.NoArgs,
	RunE: runMissingGovIds,
}

func init() {
	rootCmd.AddCommand(missingGovidsCmd)
}

func runMissingGovIds(cmd *cobra.Command, args []string) error {
	log.Printf("🔍 Finding NY PUC govids not in database")

	// Get debug mode from global flag
	debugMode := GetDebugMode()

	// Create execution context with debug mode
	var ctx context.Context
	if debugMode {
		ctx = core.WithExecutionConfig(context.Background(), core.NewExecutionConfigWithDebug())
	} else {
		ctx = core.WithExecutionConfig(context.Background(), core.NewExecutionConfig())
	}

	var missingGovIds []string
	var err error

	// Try to read metadata from stdin
	if metadata, hasStdin := tryReadMetadataFromStdin(); hasStdin {
		// Get missing govids using provided metadata
		missingGovIds, err = pipelines.GetMissingGovIdsFromMetadata(ctx, metadata)
		if err != nil {
			return fmt.Errorf("failed to get missing govids from metadata: %w", err)
		}
	} else {
		// No stdin data, fetch metadata by scraping
		log.Printf("🔍 No stdin data detected, fetching metadata by scraping...")

		// Get missing govids using scraping
		missingGovIds, err = pipelines.GetMissingGovIds(ctx)
		if err != nil {
			return fmt.Errorf("failed to get missing govids: %w", err)
		}
	}

	log.Printf("✅ Found %d govids not currently in database", len(missingGovIds))

	// Print results as JSON
	output, err := json.MarshalIndent(missingGovIds, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal results: %w", err)
	}

	fmt.Println(string(output))
	return nil
}