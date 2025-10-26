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

var (
	intermediateSource string
)

// pipelineCmd represents the pipeline command
var pipelineCmd = &cobra.Command{
	Use:   "pipeline [flags] <gov_id>",
	Short: "Run the full data pipeline for a government ID",
	Long: `Execute the complete data pipeline for a given government ID including
scraping, processing, and uploading. The pipeline can use different intermediate
sources to optimize processing.`,
	Example: `  dokito-cli pipeline 00-F-0229
  dokito-cli pipeline --intermediate-source=html 00-F-0229
  dokito-cli pipeline --intermediate-source=raw_json 00-F-0229
  dokito-cli pipeline --no-debug 00-F-0229`,
	Args: cobra.ExactArgs(1),
	RunE: runPipeline,
}

func init() {
	rootCmd.AddCommand(pipelineCmd)

	// Pipeline-specific flags
	pipelineCmd.Flags().StringVar(&intermediateSource, "intermediate-source", "none",
		"Source for intermediate data (none, html, raw_json, processed_json)")
}

func runPipeline(cmd *cobra.Command, args []string) error {
	govID := strings.TrimSpace(args[0])

	// Parse intermediate source
	source, err := parseIntermediateSource(intermediateSource)
	if err != nil {
		return fmt.Errorf("invalid intermediate source: %w", err)
	}

	// Get debug mode from global flag
	debugMode := GetDebugMode()

	// Create pipeline config
	config := pipelines.NYPUCPipelineConfig{
		DebugMode:          debugMode,
		IntermediateSource: source,
	}

	// Show configuration for transparency
	fmt.Printf("🔧 Pipeline Configuration:\n")
	fmt.Printf("  Gov ID: %s\n", govID)
	fmt.Printf("  Intermediate Source: %s\n", config.IntermediateSource)
	fmt.Printf("  Debug Mode: %t\n", config.DebugMode)
	fmt.Println("")

	// Create execution context with config from CLI args
	var execConfig *core.ExecutionConfig
	if config.DebugMode {
		execConfig = core.NewExecutionConfigWithDebug()
	} else {
		execConfig = core.NewExecutionConfig()
	}
	ctx := core.WithExecutionConfig(context.Background(), execConfig)

	// Execute the shared NY PUC pipeline
	result, err := pipelines.ExecuteNYPUCBasicPipelineWithConfig(ctx, govID, config)
	if err != nil {
		return fmt.Errorf("pipeline failed: %w", err)
	}

	log.Printf("🎉 Full pipeline completed successfully for %s using %s source. Scraped %d items, processed %d items.",
		result.GovID, config.IntermediateSource, result.ScrapeCount, result.ProcessCount)

	return nil
}

// parseIntermediateSource converts a string to IntermediateSource enum
func parseIntermediateSource(source string) (pipelines.IntermediateSource, error) {
	switch strings.ToLower(source) {
	case "none", "":
		return pipelines.IntermediateSourceNone, nil
	case "html":
		return pipelines.IntermediateSourceHTML, nil
	case "raw_json", "raw-json":
		return pipelines.IntermediateSourceRawJSON, nil
	case "processed_json", "processed-json":
		return pipelines.IntermediateSourceProcessedJSON, nil
	default:
		return "", fmt.Errorf("invalid intermediate source: %s. Valid options: none, html, raw_json, processed_json", source)
	}
}