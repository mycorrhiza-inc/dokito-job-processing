package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/spf13/cobra"
	"runner/internal/core"
	"runner/internal/worker"
)

var (
	bulkQueueLimit  int
	bulkQueueSource string
)

// bulkQueueCmd represents the bulk-queue command
var bulkQueueCmd = &cobra.Command{
	Use:   "bulk-queue",
	Short: "Queue random missing govids for processing",
	Long: `Find missing govids, randomize them, and queue a limited number for
background processing. This command is useful for bulk processing operations.

The command accepts metadata via stdin to avoid repeated scraping:
  cat metadata.json | dokito-cli bulk-queue --limit=100`,
	Example: `  dokito-cli bulk-queue --limit=500 --source=html
  dokito-cli bulk-queue --limit=30 --source=html --no-debug
  cat metadata.json | dokito-cli bulk-queue --limit=100`,
	Args: cobra.NoArgs,
	RunE: runBulkQueue,
}

func init() {
	rootCmd.AddCommand(bulkQueueCmd)

	// Bulk queue specific flags
	bulkQueueCmd.Flags().IntVar(&bulkQueueLimit, "limit", 1000, "Maximum number of govids to queue (default 1000)")
	bulkQueueCmd.Flags().StringVar(&bulkQueueSource, "source", "none", "Intermediate source for processing (none, html, raw_json, processed_json)")
}

func runBulkQueue(cmd *cobra.Command, args []string) error {
	// Parse intermediate source
	intermediateSource, err := parseIntermediateSource(bulkQueueSource)
	if err != nil {
		return fmt.Errorf("invalid source: %w", err)
	}

	// Get debug mode from global flag
	debugMode := GetDebugMode()

	log.Printf("🚀 Starting bulk queue operation (limit: %d, source: %s, debug: %t)",
		bulkQueueLimit, intermediateSource, debugMode)

	// Create execution context with debug mode
	var config *core.ExecutionConfig
	if debugMode {
		config = core.NewExecutionConfigWithDebug()
	} else {
		config = core.NewExecutionConfig()
	}
	ctx := core.WithExecutionConfig(context.Background(), config)

	// Try to read metadata from stdin
	metadata, hasStdin := tryReadMetadataFromStdin()
	if hasStdin {
		log.Printf("📤 Bulk queue operation will use metadata from stdin")
	}

	// Initialize queues first
	if err := worker.InitializeQueues(); err != nil {
		return fmt.Errorf("failed to initialize task queues: %w", err)
	}

	// Register tasks
	worker.RegisterTasks()

	// Execute bulk queue operation with optional metadata
	var result *worker.BulkQueueResult
	if hasStdin {
		result, err = worker.BulkQueueMissingGovIdsFromMetadata(ctx, bulkQueueLimit, intermediateSource, metadata)
	} else {
		result, err = worker.BulkQueueMissingGovIds(ctx, bulkQueueLimit, intermediateSource)
	}
	if err != nil {
		return fmt.Errorf("bulk queue operation failed: %w", err)
	}

	log.Printf("✅ Bulk queue operation completed")

	// Print results as JSON
	output, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal results: %w", err)
	}

	fmt.Println(string(output))
	return nil
}