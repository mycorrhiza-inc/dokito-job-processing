package cli

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/spf13/cobra"
	"runner/internal/core"
	"runner/internal/pipelines"
)

// reprocessAttachmentsCmd represents the reprocess-attachments command
var reprocessAttachmentsCmd = &cobra.Command{
	Use:   "reprocess-attachments [flags] <gov_id>",
	Short: "Reprocess attachments for a docket with existing processed JSON",
	Long: `Fetch processed JSON from S3, run it through the attachment downloader,
and upload back to S3 only if the data changed. This is useful for:

- Reprocessing dockets when attachment download logic has been updated
- Ensuring all attachments are properly downloaded and hashed
- Updating metadata without re-scraping or re-processing the entire docket

The pipeline will:
1. Fetch the existing processed JSON from S3
2. Run it through the Rust attachment downloader
3. Compare the original vs processed data
4. Upload to S3 only if changes were detected`,
	Example: `  dokito-cli reprocess-attachments 00-F-0229
  dokito-cli reprocess-attachments --debug 12-E-0544
  dokito-cli reprocess-attachments --no-debug 00-F-0229`,
	Args: cobra.ExactArgs(1),
	RunE: runReprocessAttachments,
}

func init() {
	rootCmd.AddCommand(reprocessAttachmentsCmd)
}

func runReprocessAttachments(cmd *cobra.Command, args []string) error {
	govID := strings.TrimSpace(args[0])

	// Get debug mode from global flag
	debugMode := GetDebugMode()

	// Show configuration for transparency
	fmt.Printf("🔄 Attachment Reprocessing Configuration:\n")
	fmt.Printf("  Gov ID: %s\n", govID)
	fmt.Printf("  Debug Mode: %t\n", debugMode)
	fmt.Println("")

	// Create execution context with config from CLI args
	var execConfig *core.ExecutionConfig
	if debugMode {
		execConfig = core.NewExecutionConfigWithDebug()
	} else {
		execConfig = core.NewExecutionConfig()
	}
	ctx := core.WithExecutionConfig(context.Background(), execConfig)

	// Execute the attachment reprocessing pipeline
	result, err := pipelines.ExecuteAttachmentReprocessingPipelineWithConfig(ctx, govID)
	if err != nil {
		return fmt.Errorf("attachment reprocessing failed: %w", err)
	}

	// Print detailed results
	if result.DataChanged {
		log.Printf("✅ Attachment reprocessing completed for %s - DATA CHANGED and uploaded to S3", result.GovID)

		if debugMode {
			fmt.Printf("\n📊 Details:\n")
			fmt.Printf("  Original data items: %d\n", len(result.OriginalData))
			fmt.Printf("  Processed data items: %d\n", len(result.ProcessedData))
			fmt.Printf("  Upload performed: %t\n", result.UploadPerformed)

			// Print data differences if debug mode is enabled
			pipelines.PrintDataDifferences(result.OriginalData, result.ProcessedData)
		}
	} else {
		log.Printf("✅ Attachment reprocessing completed for %s - NO CHANGES detected, S3 not updated", result.GovID)
	}

	return nil
}