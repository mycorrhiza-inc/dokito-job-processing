package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"runner/internal/core"
)

// envCmd represents the env command
var envCmd = &cobra.Command{
	Use:   "env",
	Short: "Show environment configuration",
	Long: `Display the current environment configuration including paths to
scraper binaries, dokito binaries, and relevant environment variables.
This is useful for debugging configuration issues.`,
	Example: `  dokito-cli env`,
	Args:    cobra.NoArgs,
	RunE:    runEnv,
}

func init() {
	rootCmd.AddCommand(envCmd)
}

func runEnv(cmd *cobra.Command, args []string) error {
	fmt.Println("🔧 Environment Configuration:")
	fmt.Println("")

	scraperPaths := core.GetScraperPaths()
	dokitoPaths := core.GetDokitoPaths()

	fmt.Println("Scraper Binaries:")
	fmt.Printf("  NYPUC:    %s\n", getStatus(scraperPaths.NYPUCPath))
	fmt.Printf("  COPUC:    %s\n", getStatus(scraperPaths.COPUCPath))
	fmt.Printf("  UtahCoal: %s\n", getStatus(scraperPaths.UtahCoalPath))

	fmt.Println("")
	fmt.Println("Dokito Binaries:")
	fmt.Printf("  Process:  %s\n", getStatus(dokitoPaths.ProcessDocketsPath))
	fmt.Printf("  Upload:   %s\n", getStatus(dokitoPaths.UploadDocketsPath))
	fmt.Printf("  Download: %s\n", getStatus(dokitoPaths.DownloadAttachmentsPath))
	fmt.Printf("  Database: %s\n", getStatus(dokitoPaths.DatabaseUtilsPath))

	fmt.Println("")
	fmt.Println("Environment Variables:")
	fmt.Printf("  OPENSCRAPER_PATH_NYPUC: %s\n", os.Getenv("OPENSCRAPER_PATH_NYPUC"))
	fmt.Printf("  OPENSCRAPER_PATH_COPUC: %s\n", os.Getenv("OPENSCRAPER_PATH_COPUC"))
	fmt.Printf("  OPENSCRAPER_PATH_UTAHCOAL: %s\n", os.Getenv("OPENSCRAPER_PATH_UTAHCOAL"))
	fmt.Printf("  DOKITO_PROCESS_DOCKETS_BINARY_PATH: %s\n", os.Getenv("DOKITO_PROCESS_DOCKETS_BINARY_PATH"))
	fmt.Printf("  DOKITO_UPLOAD_DOCKETS_BINARY_PATH: %s\n", os.Getenv("DOKITO_UPLOAD_DOCKETS_BINARY_PATH"))
	fmt.Printf("  DOKITO_DOWNLOAD_ATTACHMENTS_BINARY_PATH: %s\n", os.Getenv("DOKITO_DOWNLOAD_ATTACHMENTS_BINARY_PATH"))
	fmt.Printf("  DOKITO_DATABASE_UTILS_BINARY_PATH: %s\n", os.Getenv("DOKITO_DATABASE_UTILS_BINARY_PATH"))

	fmt.Println("")
	fmt.Println("Worker Configuration:")
	workerConcurrency := os.Getenv("DOKITO_WORKER_CONCURRENCY")
	if workerConcurrency == "" {
		workerConcurrency = "5 (default)"
	}
	fmt.Printf("  DOKITO_WORKER_CONCURRENCY: %s\n", workerConcurrency)

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://127.0.0.1:6379 (default)"
	}
	fmt.Printf("  REDIS_URL: %s\n", redisURL)

	return nil
}

func getStatus(path string) string {
	if path == "" {
		return "❌ not configured"
	}

	if _, err := os.Stat(path); err != nil {
		return fmt.Sprintf("❌ configured but missing (%s)", path)
	}

	return fmt.Sprintf("✅ %s", path)
}