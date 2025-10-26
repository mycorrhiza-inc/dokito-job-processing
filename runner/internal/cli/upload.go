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

// uploadCmd represents the upload command
var uploadCmd = &cobra.Command{
	Use:   "upload <json_file>",
	Short: "Run upload only on a JSON file",
	Long: `Execute only the upload phase on a given JSON file containing
processed data. This command will upload the data to the database without
performing any scraping or processing.`,
	Example: `  dokito-cli upload processed_data.json
  dokito-cli upload --no-debug processed_data.json`,
	Args: cobra.ExactArgs(1),
	RunE: runUpload,
}

func init() {
	rootCmd.AddCommand(uploadCmd)
}

func runUpload(cmd *cobra.Command, args []string) error {
	jsonFile := args[0]
	log.Printf("📤 Uploading JSON file: %s", jsonFile)

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

	// Upload data
	if err := core.ExecuteUploadBinary(ctx, inputData); err != nil {
		return fmt.Errorf("upload failed: %w", err)
	}

	log.Printf("✅ Upload completed successfully")
	return nil
}