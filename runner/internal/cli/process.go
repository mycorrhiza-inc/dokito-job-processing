package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
	"runner/internal/core"
)

// processCmd represents the process command
var processCmd = &cobra.Command{
	Use:   "process <json_file>",
	Short: "Run processing only on a JSON file",
	Long: `Execute only the data processing phase on a given JSON file containing
scraped data. This command will process the data and output the processed
results as JSON without performing any uploading.`,
	Example: `  dokito-cli process scraped_data.json
  dokito-cli process --no-debug scraped_data.json`,
	Args: cobra.ExactArgs(1),
	RunE: runProcess,
}

func init() {
	rootCmd.AddCommand(processCmd)
}

func runProcess(cmd *cobra.Command, args []string) error {
	jsonFile := args[0]
	log.Printf("🔧 Processing JSON file: %s", jsonFile)

	// Read JSON file
	data, err := os.ReadFile(jsonFile)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	var inputData []map[string]any
	if err := json.Unmarshal(data, &inputData); err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Get debug mode from global flag
	debugMode := GetDebugMode()

	// Create execution context with debug mode
	var ctx context.Context
	if debugMode {
		ctx = core.WithExecutionConfig(context.Background(), core.NewExecutionConfigWithDebug())
	} else {
		ctx = core.WithExecutionConfig(context.Background(), core.NewExecutionConfig())
	}

	// Process data
	results, err := core.ExecuteDataProcessingBinary(ctx, inputData)
	if err != nil {
		return fmt.Errorf("data processing failed: %w", err)
	}

	log.Printf("✅ Processing completed. Processed %d results", len(results))

	// Pretty print results
	output, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal results: %w", err)
	}

	fmt.Println(string(output))
	return nil
}