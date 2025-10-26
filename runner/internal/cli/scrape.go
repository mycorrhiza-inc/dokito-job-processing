package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/spf13/cobra"
	"runner/internal/core"
)

// scrapeCmd represents the scrape command
var scrapeCmd = &cobra.Command{
	Use:   "scrape <gov_id>",
	Short: "Run scraper only and print results",
	Long: `Execute only the scraping phase of the pipeline for a given government ID.
This command will scrape the data and output the raw results as JSON without
performing any processing or uploading.`,
	Example: `  dokito-cli scrape 00-F-0229
  dokito-cli scrape --no-debug 00-F-0229`,
	Args: cobra.ExactArgs(1),
	RunE: runScrape,
}

func init() {
	rootCmd.AddCommand(scrapeCmd)
}

func runScrape(cmd *cobra.Command, args []string) error {
	govID := strings.TrimSpace(args[0])
	log.Printf("🔍 Running scraper only for govID: %s", govID)

	// Get debug mode from global flag
	debugMode := GetDebugMode()

	// Create execution context with debug mode
	var ctx context.Context
	if debugMode {
		ctx = core.WithExecutionConfig(context.Background(), core.NewExecutionConfigWithDebug())
	} else {
		ctx = core.WithExecutionConfig(context.Background(), core.NewExecutionConfig())
	}

	// Initialize mapping and determine scraper type
	mapping := core.GetDefaultGovIDMapping()
	scraperType := mapping.GetScraperForGovID(govID)

	log.Printf("📋 Using scraper type: %s", scraperType)

	// Execute scraper
	results, err := core.ExecuteScraperWithALLMode(ctx, govID, scraperType)
	if err != nil {
		return fmt.Errorf("scraper execution failed: %w", err)
	}

	log.Printf("✅ Scraper completed. Found %d results", len(results))

	// Pretty print results
	output, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal results: %w", err)
	}

	fmt.Println(string(output))
	return nil
}