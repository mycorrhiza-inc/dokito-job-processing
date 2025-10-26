package cli

import (
	"os"

	"github.com/spf13/cobra"
)

var (
	// Global flags
	debugMode bool
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "dokito-cli",
	Short: "Dokito CLI - Debug tool for the job processing pipeline",
	Long: `Dokito CLI is a command-line tool for testing and debugging the data pipeline
that processes government documents. It provides commands for scraping, processing,
uploading data, and managing background job queues.

The CLI is designed primarily for development, testing, and debugging workflows.`,
	Version: "1.0.0",
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// Global flags
	rootCmd.PersistentFlags().BoolVar(&debugMode, "debug", true, "Enable debug mode with verbose output")

	// Add version template
	rootCmd.SetVersionTemplate(`{{printf "%s version %s\n" .Name .Version}}`)
}

// GetDebugMode returns the current debug mode setting
func GetDebugMode() bool {
	return debugMode
}